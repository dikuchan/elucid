use std::collections::HashSet;
use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use serde::{Deserialize, Serialize};

pub const TIMESTAMP_COLUMN_NAME: &str = "@timestamp";

pub const REST_COLUMN_NAME: &str = "@rest";

pub const TIME_SOURCE_KEY: &str = "time_source";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ColumnType {
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "int64")]
    Int64,
    #[serde(rename = "int32")]
    Int32,
    #[serde(rename = "uint64")]
    Uint64,
    #[serde(rename = "uint32")]
    Uint32,
    #[serde(rename = "float64")]
    Float64,
    #[serde(rename = "float32")]
    Float32,
    #[serde(rename = "bool")]
    Bool,
    #[serde(rename = "timestamp")]
    Timestamp,
}

impl ColumnType {
    pub fn to_arrow(self) -> DataType {
        match self {
            Self::Utf8 => DataType::Utf8,
            Self::Int64 => DataType::Int64,
            Self::Int32 => DataType::Int32,
            Self::Uint64 => DataType::UInt64,
            Self::Uint32 => DataType::UInt32,
            Self::Float64 => DataType::Float64,
            Self::Float32 => DataType::Float32,
            Self::Bool => DataType::Boolean,
            Self::Timestamp => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableName(String);

impl TableName {
    pub fn new(name: impl AsRef<str>) -> Result<Self, SchemaError> {
        let name = name.as_ref();
        if name.is_empty() {
            return Err(SchemaError::InvalidTableName(
                "table name must not be empty".to_owned(),
            ));
        }
        if name.contains('\0') {
            return Err(SchemaError::InvalidTableName(
                "table name must not contain null bytes".to_owned(),
            ));
        }
        if name == "." {
            return Err(SchemaError::InvalidTableName(
                "table name must not be bare '.'".to_owned(),
            ));
        }
        if name.starts_with('/') || name.starts_with('\\') {
            return Err(SchemaError::InvalidTableName(
                "table name must not be an absolute path".to_owned(),
            ));
        }
        if name.contains('/') || name.contains('\\') || name.contains("..") {
            return Err(SchemaError::InvalidTableName(
                "table name must not contain '/', '\\', or '..'".to_owned(),
            ));
        }
        Ok(Self(name.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for TableName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ColumnDescriptor {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: ColumnType,
    /// Whether this column is the time column.
    #[serde(default)]
    pub time: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SchemaConfig {
    pub table: String,
    pub columns: Vec<ColumnDescriptor>,
}

/// Errors that can occur during schema operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SchemaError {
    /// YAML parsing failed.
    #[error("YAML parse error")]
    YamlParse(#[source] serde_yaml::Error),
    /// Schema validation failed.
    #[error("Schema validation failed:\n{0}")]
    Validation(ValidationErrors),
    /// Table already exists.
    #[error("Table '{table}' already exists")]
    TableExists { table: String },
    /// Invalid table name.
    #[error("invalid table name: {0}")]
    InvalidTableName(String),
    /// I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Schema compilation error.
    #[error("Schema compilation failed: {0}")]
    Compile(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ValidationErrors {
    pub errors: Vec<String>,
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for error in &self.errors {
            writeln!(f, "  - {error}")?;
        }
        Ok(())
    }
}

impl SchemaConfig {
    pub fn from_yaml(text: &str) -> Result<Self, SchemaError> {
        let config: SchemaConfig = serde_yaml::from_str(text).map_err(SchemaError::YamlParse)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), SchemaError> {
        let mut errors = Vec::new();

        // Validate table name for path safety.
        if let Err(e) = TableName::new(&self.table) {
            errors.push(e.to_string());
        }

        // Exactly one time column.
        let time_columns: Vec<&str> = self
            .columns
            .iter()
            .filter(|c| c.time)
            .map(|c| c.name.as_str())
            .collect();
        match time_columns.len() {
            0 => errors.push("no column marked with 'time: true'".to_owned()),
            1 => {}
            n => errors.push(format!(
                "expected exactly one 'time: true' column, found {n}: {}",
                time_columns.join(", ")
            )),
        }

        // No @-prefixed user columns.
        for col in &self.columns {
            if col.name.starts_with('@') {
                errors.push(format!(
                    "column '{}' starts with '@', which is reserved for system fields",
                    col.name
                ));
            }
        }

        // No duplicate names.
        let mut seen = HashSet::new();
        for col in &self.columns {
            if !seen.insert(&col.name) {
                errors.push(format!("duplicate column name '{}'", col.name));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(SchemaError::Validation(ValidationErrors { errors }))
        }
    }

    pub fn compile(&self) -> Schema {
        let mut time_source = None;
        let mut fields: Vec<Arc<Field>> = Vec::with_capacity(self.columns.len() + 1);

        for col in &self.columns {
            let field = if col.time {
                time_source = Some(col.name.clone());
                Field::new(TIMESTAMP_COLUMN_NAME, col.ty.to_arrow(), false)
            } else {
                Field::new(&col.name, col.ty.to_arrow(), true)
            };
            fields.push(Arc::new(field));
        }

        // System @rest column.
        fields.push(Arc::new(Field::new(REST_COLUMN_NAME, DataType::Utf8, true)));

        let mut metadata = std::collections::HashMap::new();
        if let Some(source) = time_source {
            metadata.insert(TIME_SOURCE_KEY.to_owned(), source);
        }

        Schema::new_with_metadata(fields, metadata)
    }

    pub fn table_name(&self) -> Result<TableName, SchemaError> {
        TableName::new(&self.table)
    }

    fn table_dir(data_root: &Path, table: &TableName) -> PathBuf {
        data_root.join(table.as_str())
    }

    /// Register this schema: validate and persist to disk.
    ///
    /// Creates `<data_root>/<table>/` and writes `_schema.yaml`.
    /// The Arrow schema is re-compiled on load via [`SchemaConfig::load`].
    ///
    /// Returns `Err(SchemaError::TableExists)` if the table directory already
    /// exists.
    pub fn register(&self, data_root: &Path) -> Result<(), SchemaError> {
        self.validate()?;
        let table_name = self.table_name()?;

        std::fs::create_dir_all(data_root)?;

        let dir = Self::table_dir(data_root, &table_name);

        std::fs::create_dir(&dir).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                SchemaError::TableExists {
                    table: self.table.clone(),
                }
            } else {
                SchemaError::Io(e)
            }
        })?;

        let config_yaml = serde_yaml::to_string(self)
            .map_err(|e| SchemaError::Compile(format!("failed to serialize config: {e}")))?;

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join("_schema.yaml"))?;
        file.write_all(config_yaml.as_bytes())?;

        Ok(())
    }

    /// Load a previously registered schema from disk.
    ///
    /// Reads `<data_root>/<table>/_schema.yaml` and returns the parsed
    /// [`SchemaConfig`].
    pub fn load(data_root: &Path, table: &TableName) -> Result<Self, SchemaError> {
        let config_path = Self::table_dir(data_root, table).join("_schema.yaml");
        let yaml = std::fs::read_to_string(&config_path)?;
        let config: SchemaConfig = serde_yaml::from_str(&yaml).map_err(SchemaError::YamlParse)?;
        Ok(config)
    }

    /// Load and compile the Arrow schema from disk.
    pub fn load_arrow(data_root: &Path, table: &TableName) -> Result<Schema, SchemaError> {
        let config = Self::load(data_root, table)?;
        Ok(config.compile())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn web_access_yaml() -> &'static str {
        r#"
table: web_access
columns:
  - name: _ts
    type: timestamp
    time: true
  - name: host
    type: utf8
  - name: method
    type: utf8
  - name: status
    type: int64
  - name: bytes
    type: int64
"#
    }

    #[test]
    fn parse_valid_schema() {
        let config = SchemaConfig::from_yaml(web_access_yaml()).expect("should parse");
        assert_eq!(config.table, "web_access");
        assert_eq!(config.columns.len(), 5);
        assert_eq!(config.columns[0].name, "_ts");
        assert!(config.columns[0].time);
        assert_eq!(config.columns[0].ty, ColumnType::Timestamp);
    }

    #[test]
    fn round_trip() {
        let config = SchemaConfig::from_yaml(web_access_yaml()).expect("should parse");
        let yaml = serde_yaml::to_string(&config).expect("should serialize");
        let reparsed = SchemaConfig::from_yaml(&yaml).expect("should reparse");
        assert_eq!(config, reparsed);
    }

    #[test]
    fn validation_valid_schema_passes() {
        let config = SchemaConfig::from_yaml(web_access_yaml()).expect("should parse and validate");
        assert_eq!(config.table, "web_access");
    }

    #[test]
    fn validation_no_time_column() {
        let yaml = r#"
table: test
columns:
  - name: host
    type: utf8
"#;
        let err = SchemaConfig::from_yaml(yaml).expect_err("should fail");
        match err {
            SchemaError::Validation(ve) => {
                assert!(
                    ve.errors
                        .iter()
                        .any(|e| e.contains("no column marked with 'time: true'"))
                );
            }
            _ => panic!("expected Validation error, got {err:?}"),
        }
    }

    #[test]
    fn validation_multiple_time_columns() {
        let yaml = r#"
table: test
columns:
  - name: ts1
    type: timestamp
    time: true
  - name: ts2
    type: timestamp
    time: true
"#;
        let err = SchemaConfig::from_yaml(yaml).expect_err("should fail");
        match err {
            SchemaError::Validation(ve) => {
                assert!(
                    ve.errors
                        .iter()
                        .any(|e| e.contains("expected exactly one 'time: true' column, found 2"))
                );
            }
            _ => panic!("expected Validation error, got {err:?}"),
        }
    }

    #[test]
    fn validation_at_prefixed_column() {
        let yaml = r#"
table: test
columns:
  - name: "@timestamp"
    type: timestamp
    time: true
"#;
        let err = SchemaConfig::from_yaml(yaml).expect_err("should fail");
        match err {
            SchemaError::Validation(ve) => {
                assert!(
                    ve.errors
                        .iter()
                        .any(|e| e.contains("reserved for system fields"))
                );
            }
            _ => panic!("expected Validation error, got {err:?}"),
        }
    }

    #[test]
    fn invalid_type_rejected_by_deserialization() {
        let yaml = r#"
table: test
columns:
  - name: ts
    type: timestamp
    time: true
  - name: data
    type: binary
"#;
        let err = SchemaConfig::from_yaml(yaml).expect_err("should fail");
        assert!(matches!(err, SchemaError::YamlParse(_)));
    }

    #[test]
    fn validation_duplicate_names() {
        let yaml = r#"
table: test
columns:
  - name: ts
    type: timestamp
    time: true
  - name: host
    type: utf8
  - name: host
    type: int64
"#;
        let err = SchemaConfig::from_yaml(yaml).expect_err("should fail");
        match err {
            SchemaError::Validation(ve) => {
                assert!(
                    ve.errors
                        .iter()
                        .any(|e| e.contains("duplicate column name 'host'"))
                );
            }
            _ => panic!("expected Validation error, got {err:?}"),
        }
    }

    #[test]
    fn validation_multiple_errors_collected() {
        let yaml = r#"
table: test
columns:
  - name: host
    type: utf8
  - name: "@bad"
    type: utf8
"#;
        let err = SchemaConfig::from_yaml(yaml).expect_err("should fail");
        match err {
            SchemaError::Validation(ve) => {
                assert!(
                    ve.errors
                        .iter()
                        .any(|e| e.contains("no column marked with 'time: true'"))
                );
                assert!(
                    ve.errors
                        .iter()
                        .any(|e| e.contains("reserved for system fields"))
                );
                assert!(
                    ve.errors.len() >= 2,
                    "expected at least 2 errors, got {}",
                    ve.errors.len()
                );
            }
            _ => panic!("expected Validation error, got {err:?}"),
        }
    }

    #[test]
    fn compile_web_access_schema() {
        let config = SchemaConfig::from_yaml(web_access_yaml()).expect("should parse");
        let schema = config.compile();

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            &["@timestamp", "host", "method", "status", "bytes", "@rest"]
        );

        // @timestamp is non-nullable Timestamp(Millisecond, UTC).
        let ts_field = schema.field_with_name("@timestamp").expect("should exist");
        assert!(!ts_field.is_nullable());
        assert_eq!(
            ts_field.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );

        // User columns are nullable.
        let host_field = schema.field_with_name("host").expect("should exist");
        assert!(host_field.is_nullable());
        assert_eq!(host_field.data_type(), &DataType::Utf8);

        let status_field = schema.field_with_name("status").expect("should exist");
        assert!(status_field.is_nullable());
        assert_eq!(status_field.data_type(), &DataType::Int64);

        // @rest is nullable utf8.
        let rest_field = schema.field_with_name("@rest").expect("should exist");
        assert!(rest_field.is_nullable());
        assert_eq!(rest_field.data_type(), &DataType::Utf8);

        // Metadata stores original time column name.
        assert_eq!(
            schema.metadata().get(TIME_SOURCE_KEY),
            Some(&"_ts".to_owned())
        );
    }

    #[test]
    fn compile_all_column_types() {
        let yaml = r#"
table: all_types
columns:
  - name: ts
    type: timestamp
    time: true
  - name: s
    type: utf8
  - name: i64
    type: int64
  - name: i32
    type: int32
  - name: u64
    type: uint64
  - name: u32
    type: uint32
  - name: f64
    type: float64
  - name: f32
    type: float32
  - name: b
    type: bool
"#;
        let config = SchemaConfig::from_yaml(yaml).expect("should parse");
        let schema = config.compile();

        let expected = &[
            (
                "@timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            ("s", DataType::Utf8, true),
            ("i64", DataType::Int64, true),
            ("i32", DataType::Int32, true),
            ("u64", DataType::UInt64, true),
            ("u32", DataType::UInt32, true),
            ("f64", DataType::Float64, true),
            ("f32", DataType::Float32, true),
            ("b", DataType::Boolean, true),
            ("@rest", DataType::Utf8, true),
        ];

        for (i, (name, dtype, nullable)) in expected.iter().enumerate() {
            let field = schema.field(i);
            assert_eq!(field.name(), *name, "field {i}: name mismatch");
            assert_eq!(field.data_type(), dtype, "field {i}: type mismatch");
            assert_eq!(
                field.is_nullable(),
                *nullable,
                "field {i}: nullability mismatch"
            );
        }
    }

    #[test]
    fn register_creates_directory_and_files() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let config = SchemaConfig::from_yaml(web_access_yaml()).expect("should parse");

        config.register(tmp.path()).expect("should register");

        let table_dir = tmp.path().join("web_access");
        assert!(table_dir.is_dir());
        assert!(table_dir.join("_schema.yaml").is_file());
    }

    #[test]
    fn register_rejects_duplicate_table() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let config = SchemaConfig::from_yaml(web_access_yaml()).expect("should parse");

        config.register(tmp.path()).expect("first register");
        let err = config
            .register(tmp.path())
            .expect_err("should fail on duplicate");
        assert!(matches!(err, SchemaError::TableExists { .. }));
    }

    #[test]
    fn load_round_trip() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let config = SchemaConfig::from_yaml(web_access_yaml()).expect("should parse");

        config.register(tmp.path()).expect("should register");
        let table = TableName::new("web_access").expect("valid name");
        let loaded = SchemaConfig::load(tmp.path(), &table).expect("should load");
        assert_eq!(config, loaded);
    }

    #[test]
    fn load_arrow_round_trip() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let config = SchemaConfig::from_yaml(web_access_yaml()).expect("should parse");
        let expected = config.compile();

        config.register(tmp.path()).expect("should register");
        let table = TableName::new("web_access").expect("valid name");
        let loaded = SchemaConfig::load_arrow(tmp.path(), &table).expect("should load");

        assert_eq!(expected, loaded);
    }

    #[test]
    fn load_missing_table_fails() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let table = TableName::new("nonexistent").expect("valid name");
        let err = SchemaConfig::load(tmp.path(), &table).expect_err("should fail");
        assert!(matches!(err, SchemaError::Io(_)));
    }

    #[test]
    fn yaml_parse_error() {
        let err = SchemaConfig::from_yaml("not: valid: yaml: {{{}").expect_err("should fail");
        assert!(matches!(err, SchemaError::YamlParse(_)));
    }

    #[test]
    fn table_name_rejects_empty() {
        assert!(TableName::new("").is_err());
    }

    #[test]
    fn table_name_rejects_slash() {
        assert!(TableName::new("foo/bar").is_err());
    }

    #[test]
    fn table_name_rejects_backslash() {
        assert!(TableName::new("foo\\bar").is_err());
    }

    #[test]
    fn table_name_rejects_dot_dot() {
        assert!(TableName::new("..").is_err());
    }

    #[test]
    fn table_name_rejects_dot_dot_within() {
        assert!(TableName::new("foo..bar").is_err());
    }

    #[test]
    fn table_name_rejects_absolute_unix() {
        assert!(TableName::new("/etc/passwd").is_err());
    }

    #[test]
    fn table_name_rejects_absolute_windows() {
        assert!(TableName::new("\\\\server\\share").is_err());
    }

    #[test]
    fn table_name_accepts_valid() {
        assert!(TableName::new("web_access").is_ok());
        assert!(TableName::new("my-table_v2").is_ok());
        assert!(TableName::new("table.name").is_ok());
    }

    #[test]
    fn table_name_rejects_traversal_attempt() {
        assert!(TableName::new("../secret").is_err());
    }

    #[test]
    fn table_name_rejects_null_bytes() {
        assert!(TableName::new("foo\0bar").is_err());
    }

    #[test]
    fn table_name_rejects_bare_dot() {
        assert!(TableName::new(".").is_err());
    }

    #[test]
    fn table_name_as_str_and_display() {
        let name = TableName::new("web_access").expect("valid");
        assert_eq!(name.as_str(), "web_access");
        assert_eq!(name.to_string(), "web_access");
        assert_eq!(format!("{name}"), "web_access");
    }

    #[test]
    fn table_name_as_ref_str() {
        let name = TableName::new("events").expect("valid");
        let s: &str = name.as_ref();
        assert_eq!(s, "events");
    }

    #[test]
    fn table_name_clone_eq_hash() {
        use std::collections::HashSet;
        let a = TableName::new("test").expect("valid");
        let b = a.clone();
        assert_eq!(a, b);
        let mut set = HashSet::new();
        set.insert(a.clone());
        assert!(set.contains(&b));
    }

    #[test]
    fn validation_rejects_invalid_table_name() {
        let yaml = r#"
table: "../evil"
columns:
  - name: ts
    type: timestamp
    time: true
"#;
        let err = SchemaConfig::from_yaml(yaml).expect_err("should fail");
        match err {
            SchemaError::Validation(ve) => {
                assert!(
                    ve.errors.iter().any(|e| e.contains("invalid table name")),
                    "expected table name validation error, got: {ve:?}"
                );
            }
            _ => panic!("expected Validation error, got {err:?}"),
        }
    }
}
