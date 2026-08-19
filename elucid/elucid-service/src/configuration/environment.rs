use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt::{Debug, Formatter};

use toml_edit::{DocumentMut, Item, Table, Value};

use super::error::{ConfigurationError, EnvironmentOverrideInvalidReason};

pub(super) const DIRECT_POSTGRESQL_URL: &str = "ELUCID_METASTORE__POSTGRESQL_URL";
pub(super) const DIRECT_OBJECT_STORE_ACCESS_KEY_ID: &str = "ELUCID_OBJECT_STORE__ACCESS_KEY_ID";
pub(super) const DIRECT_OBJECT_STORE_SECRET_ACCESS_KEY: &str =
    "ELUCID_OBJECT_STORE__SECRET_ACCESS_KEY";
pub(super) const DIRECT_OBJECT_STORE_SESSION_TOKEN: &str = "ELUCID_OBJECT_STORE__SESSION_TOKEN";

#[derive(Clone)]
#[non_exhaustive]
pub struct Environment {
    values: BTreeMap<String, EnvironmentValue>,
}

impl Environment {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            values: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut environment = Self::new();
        for (name, value) in pairs {
            environment.set(name, value);
        }
        environment
    }

    #[must_use]
    pub fn from_current_process() -> Self {
        let values = std::env::vars_os()
            .filter_map(|(name, value)| {
                name.into_string()
                    .ok()
                    .map(|name| (name, EnvironmentValue::from_os_string(value)))
            })
            .collect();
        Self { values }
    }

    pub fn set(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.values
            .insert(name.into(), EnvironmentValue::Unicode(value.into()));
    }

    pub fn remove(&mut self, name: &str) {
        self.values.remove(name);
    }

    pub(super) fn apply_configuration_overrides(
        &self,
        document: &mut DocumentMut,
    ) -> Result<(), ConfigurationError> {
        for (name, value) in &self.values {
            if is_direct_secret_override(name) {
                continue;
            }
            let Some((section, field)) = configuration_override_path(name)? else {
                continue;
            };
            let EnvironmentValue::Unicode(value) = value else {
                return Err(ConfigurationError::EnvironmentOverrideInvalid {
                    name: name.clone(),
                    reason: EnvironmentOverrideInvalidReason::ValueNotUnicode,
                });
            };

            let section_item = document
                .as_table_mut()
                .entry(&section)
                .or_insert_with(|| Item::Table(Table::new()));
            let section_table = section_item.as_table_mut().ok_or_else(|| {
                ConfigurationError::EnvironmentOverrideInvalid {
                    name: name.clone(),
                    reason: EnvironmentOverrideInvalidReason::SectionIsNotTable,
                }
            })?;
            section_table.insert(&field, Item::Value(parse_override_value(&field, value)));
        }
        Ok(())
    }

    pub(super) fn value(&self, name: &str) -> EnvironmentLookup<'_> {
        match self.values.get(name) {
            Some(EnvironmentValue::Unicode(value)) => EnvironmentLookup::Unicode(value),
            Some(EnvironmentValue::NotUnicode) => EnvironmentLookup::NotUnicode,
            None => EnvironmentLookup::Missing,
        }
    }
}

impl Default for Environment {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for Environment {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let names = self.values.keys().collect::<Vec<_>>();
        formatter
            .debug_struct("Environment")
            .field("names", &names)
            .field("values", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone)]
enum EnvironmentValue {
    Unicode(String),
    NotUnicode,
}

impl EnvironmentValue {
    fn from_os_string(value: OsString) -> Self {
        value.into_string().map_or(Self::NotUnicode, Self::Unicode)
    }
}

pub(super) enum EnvironmentLookup<'a> {
    Missing,
    NotUnicode,
    Unicode(&'a str),
}

fn configuration_override_path(name: &str) -> Result<Option<(String, String)>, ConfigurationError> {
    let Some(suffix) = name.strip_prefix("ELUCID_") else {
        return Ok(None);
    };
    if !suffix.contains("__") {
        return Ok(None);
    }

    let mut components = suffix.split("__");
    let section = components.next();
    let field = components.next();
    if components.next().is_some()
        || !section.is_some_and(is_environment_path_component)
        || !field.is_some_and(is_environment_path_component)
    {
        return Err(ConfigurationError::EnvironmentOverrideInvalid {
            name: name.to_owned(),
            reason: EnvironmentOverrideInvalidReason::InvalidPath,
        });
    }

    Ok(Some((
        section.unwrap_or_default().to_ascii_lowercase(),
        field.unwrap_or_default().to_ascii_lowercase(),
    )))
}

fn is_environment_path_component(component: &str) -> bool {
    !component.is_empty()
        && component
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn is_direct_secret_override(name: &str) -> bool {
    matches!(
        name,
        DIRECT_POSTGRESQL_URL
            | DIRECT_OBJECT_STORE_ACCESS_KEY_ID
            | DIRECT_OBJECT_STORE_SECRET_ACCESS_KEY
            | DIRECT_OBJECT_STORE_SESSION_TOKEN
    )
}

fn parse_override_value(field: &str, value: &str) -> Value {
    if is_unsigned_integer_field(field) {
        return value
            .parse::<i64>()
            .map_or_else(|_| Value::from(value), Value::from);
    }
    Value::from(value)
}

fn is_unsigned_integer_field(field: &str) -> bool {
    [
        "_bytes",
        "_connections",
        "_queries",
        "_requests",
        "_rows",
        "_seconds",
    ]
    .iter()
    .any(|suffix| field.ends_with(suffix))
}
