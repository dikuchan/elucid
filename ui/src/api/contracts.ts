import { z } from 'zod';

const sourceIdSchema = z.uuid().brand<'SourceId'>();
const schemaIdSchema = z.uuid().brand<'SchemaId'>();
const queryIdSchema = z.uuid().brand<'QueryId'>();
const boundedStringSchema = z.string().max(4096);
const identifierSchema = z.string().min(1).max(255);
const safeUnsignedIntegerSchema = z
  .number()
  .int()
  .min(0)
  .max(Number.MAX_SAFE_INTEGER);
const positiveSafeIntegerSchema = safeUnsignedIntegerSchema.min(1);
const utcDateTimeSchema = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u);
const logicalTypeSchema = z.enum([
  'bool',
  'int32',
  'int64',
  'uint32',
  'uint64',
  'float32',
  'float64',
  'utf8',
  'datetime',
  'eid',
  'json',
]);
const nullabilitySchema = z.enum(['NON_NULL', 'NULLABLE']);

type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { readonly [key: string]: JsonValue };

const jsonValueSchema: z.ZodType<JsonValue> = z.lazy(() =>
  z.union([
    z.null(),
    z.boolean(),
    z.number(),
    z.string(),
    z.array(jsonValueSchema),
    z.record(z.string(), jsonValueSchema),
  ]),
);
const jsonObjectSchema = z.record(z.string(), jsonValueSchema);

const sourceSummaryWireSchema = z
  .object({
    source_id: sourceIdSchema,
    name: identifierSchema,
    display_name: boundedStringSchema,
    active_schema_version: positiveSafeIntegerSchema,
  })
  .strict();

const sourceListWireSchema = z
  .object({
    completion: z.enum(['COMPLETE', 'TRUNCATED']),
    limit: positiveSafeIntegerSchema,
    sources: z.array(sourceSummaryWireSchema),
  })
  .strict();

const fieldWireSchema = z
  .object({
    field_id: z.uuid(),
    name: boundedStringSchema,
    logical_type: logicalTypeSchema,
    nullability: nullabilitySchema,
    role: z.enum([
      'EVENT_TIME',
      'INGESTION_TIME',
      'EVENT_ID',
      'DATA',
      'REMAINDER',
    ]),
    description: boundedStringSchema.nullable(),
    historical_remainder_pointer: boundedStringSchema.nullable(),
  })
  .strict();

const schemaSummaryWireSchema = z
  .object({
    schema_id: schemaIdSchema,
    version: positiveSafeIntegerSchema,
  })
  .strict();

const schemaDetailWireSchema = z
  .object({
    schema_id: schemaIdSchema,
    version: positiveSafeIntegerSchema,
    fields: z.array(fieldWireSchema),
  })
  .strict();

const ingestionProfileWireSchema = z
  .object({
    profile_revision_id: z.uuid(),
    revision: positiveSafeIntegerSchema,
    target_schema_id: schemaIdSchema,
    maximum_record_bytes: positiveSafeIntegerSchema,
    event_time_json_pointer: boundedStringSchema,
    event_time_format: z.enum(['RFC3339', 'UNIX_MILLISECONDS']),
  })
  .strict();

const inputWireSchema = z
  .object({
    input_id: z.uuid(),
    name: identifierSchema,
    active_profile: ingestionProfileWireSchema,
  })
  .strict();

const sourceDetailWireSchema = z
  .object({
    source_id: sourceIdSchema,
    name: identifierSchema,
    display_name: boundedStringSchema,
    active_schema: schemaDetailWireSchema,
    schema_versions: z.array(schemaSummaryWireSchema),
    inputs: z.array(inputWireSchema),
  })
  .strict();

const queryTimeRangeWireSchema = z
  .object({
    start_inclusive: utcDateTimeSchema,
    end_exclusive: utcDateTimeSchema,
  })
  .strict();

const queryColumnWireSchema = z
  .object({
    name: boundedStringSchema,
    logical_type: logicalTypeSchema,
    nullability: nullabilitySchema,
  })
  .strict();

const querySpanWireSchema = z
  .object({
    start_byte: safeUnsignedIntegerSchema,
    end_byte: safeUnsignedIntegerSchema,
  })
  .strict()
  .refine((span) => span.start_byte <= span.end_byte, {
    message: 'start_byte must not exceed end_byte',
  });

const queryPositionWireSchema = z
  .object({
    line: positiveSafeIntegerSchema,
    column: positiveSafeIntegerSchema,
  })
  .strict();

const querySourceRangeWireSchema = z
  .object({
    start: queryPositionWireSchema,
    end: queryPositionWireSchema,
  })
  .strict();

const queryDiagnosticWireSchema = z
  .object({
    code: boundedStringSchema,
    severity: z.enum(['ERROR', 'WARNING']),
    message: boundedStringSchema,
    span: querySpanWireSchema.nullable(),
    source_range: querySourceRangeWireSchema.nullable(),
  })
  .strict();

const queryStatisticsWireSchema = z
  .object({
    selected_segments: safeUnsignedIntegerSchema,
    selected_parquet_bytes: safeUnsignedIntegerSchema,
    output_rows: safeUnsignedIntegerSchema,
    output_bytes: safeUnsignedIntegerSchema,
    elapsed_milliseconds: safeUnsignedIntegerSchema,
  })
  .strict();

const queryExecutionWireSchema = z
  .object({
    query_id: queryIdSchema,
    source_id: sourceIdSchema,
    active_schema_id: schemaIdSchema,
    active_schema_version: positiveSafeIntegerSchema,
    time_range: queryTimeRangeWireSchema,
    columns: z.array(queryColumnWireSchema).min(1),
    rows: z.array(z.array(jsonValueSchema)),
    completion: z.enum(['COMPLETE', 'TRUNCATED']),
    truncation_reason: z.enum(['OUTPUT_ROWS', 'OUTPUT_BYTES']).nullable(),
    diagnostics: z.array(queryDiagnosticWireSchema),
    statistics: queryStatisticsWireSchema,
  })
  .strict()
  .superRefine((response, context) => {
    const reasonMatchesCompletion =
      (response.completion === 'COMPLETE' &&
        response.truncation_reason === null) ||
      (response.completion === 'TRUNCATED' &&
        response.truncation_reason !== null);
    if (!reasonMatchesCompletion) {
      context.addIssue({
        code: 'custom',
        path: ['truncation_reason'],
        message: 'truncation_reason does not match completion',
      });
    }

    if (response.statistics.output_rows !== response.rows.length) {
      context.addIssue({
        code: 'custom',
        path: ['statistics', 'output_rows'],
        message: 'output_rows does not match the decoded row count',
      });
    }

    response.rows.forEach((row, rowIndex) => {
      if (row.length !== response.columns.length) {
        context.addIssue({
          code: 'custom',
          path: ['rows', rowIndex],
          message: 'row length does not match columns',
        });
        return;
      }
      row.forEach((value, columnIndex) => {
        const column = response.columns[columnIndex];
        if (column !== undefined && !cellMatchesColumn(value, column)) {
          context.addIssue({
            code: 'custom',
            path: ['rows', rowIndex, columnIndex],
            message: `cell does not match ${column.logical_type} ${column.nullability}`,
          });
        }
      });
    });
  });

const errorEnvelopeWireSchema = z
  .object({
    error: z
      .object({
        code: boundedStringSchema,
        message: boundedStringSchema,
        details: jsonObjectSchema,
      })
      .strict(),
  })
  .strict();

const queryErrorDetailsWireSchema = z
  .object({ diagnostics: z.array(queryDiagnosticWireSchema) })
  .strict();

export type SourceId = z.infer<typeof sourceIdSchema>;
export type LogicalType = z.infer<typeof logicalTypeSchema>;
export type Nullability = z.infer<typeof nullabilitySchema>;
export type JsonCell = JsonValue;

export interface SourceSummary {
  readonly sourceId: SourceId;
  readonly name: string;
  readonly displayName: string;
  readonly activeSchemaVersion: number;
}

export interface SourceList {
  readonly completion: 'COMPLETE' | 'TRUNCATED';
  readonly limit: number;
  readonly sources: readonly SourceSummary[];
}

export interface FieldSummary {
  readonly fieldId: string;
  readonly name: string;
  readonly logicalType: LogicalType;
  readonly nullability: Nullability;
  readonly role:
    'EVENT_TIME' | 'INGESTION_TIME' | 'EVENT_ID' | 'DATA' | 'REMAINDER';
  readonly description: string | null;
  readonly historicalRemainderPointer: string | null;
}

export interface SchemaSummary {
  readonly schemaId: string;
  readonly version: number;
}

export interface SourceDetail {
  readonly sourceId: SourceId;
  readonly name: string;
  readonly displayName: string;
  readonly activeSchema: {
    readonly schemaId: string;
    readonly version: number;
    readonly fields: readonly FieldSummary[];
  };
  readonly schemaVersions: readonly SchemaSummary[];
  readonly inputs: readonly {
    readonly inputId: string;
    readonly name: string;
    readonly activeProfile: {
      readonly profileRevisionId: string;
      readonly revision: number;
      readonly targetSchemaId: string;
      readonly maximumRecordBytes: number;
      readonly eventTimeJsonPointer: string;
      readonly eventTimeFormat: 'RFC3339' | 'UNIX_MILLISECONDS';
    };
  }[];
}

export interface QueryDiagnostic {
  readonly code: string;
  readonly severity: 'ERROR' | 'WARNING';
  readonly message: string;
  readonly span: {
    readonly startByte: number;
    readonly endByte: number;
  } | null;
  readonly sourceRange: {
    readonly start: { readonly line: number; readonly column: number };
    readonly end: { readonly line: number; readonly column: number };
  } | null;
}

export interface QueryColumn {
  readonly name: string;
  readonly logicalType: LogicalType;
  readonly nullability: Nullability;
}

export interface QueryExecution {
  readonly queryId: string;
  readonly sourceId: SourceId;
  readonly activeSchemaId: string;
  readonly activeSchemaVersion: number;
  readonly timeRange: {
    readonly startInclusive: string;
    readonly endExclusive: string;
  };
  readonly columns: readonly QueryColumn[];
  readonly rows: readonly (readonly JsonCell[])[];
  readonly completion: 'COMPLETE' | 'TRUNCATED';
  readonly truncationReason: 'OUTPUT_ROWS' | 'OUTPUT_BYTES' | null;
  readonly diagnostics: readonly QueryDiagnostic[];
  readonly statistics: {
    readonly selectedSegments: number;
    readonly selectedParquetBytes: number;
    readonly outputRows: number;
    readonly outputBytes: number;
    readonly elapsedMilliseconds: number;
  };
}

export interface ApiErrorEnvelope {
  readonly code: string;
  readonly message: string;
  readonly details: Readonly<Record<string, JsonValue>>;
  readonly diagnostics: readonly QueryDiagnostic[];
}

export class ApiContractError extends Error {
  constructor(label: string, error: z.ZodError) {
    const issues = error.issues
      .map((issue) => {
        const path = issue.path.length === 0 ? '<root>' : issue.path.join('.');
        return `${path}: ${issue.message}`;
      })
      .join('; ');
    super(`Invalid ${label}: ${issues}`);
    this.name = 'ApiContractError';
  }
}

export function decodeSourceList(input: unknown): SourceList {
  const response = decode(sourceListWireSchema, input, 'source list response');
  return {
    completion: response.completion,
    limit: response.limit,
    sources: response.sources.map((source) => ({
      sourceId: source.source_id,
      name: source.name,
      displayName: source.display_name,
      activeSchemaVersion: source.active_schema_version,
    })),
  };
}

export function decodeSourceDetail(input: unknown): SourceDetail {
  const source = decode(
    sourceDetailWireSchema,
    input,
    'source detail response',
  );
  return {
    sourceId: source.source_id,
    name: source.name,
    displayName: source.display_name,
    activeSchema: {
      schemaId: source.active_schema.schema_id,
      version: source.active_schema.version,
      fields: source.active_schema.fields.map((field) => ({
        fieldId: field.field_id,
        name: field.name,
        logicalType: field.logical_type,
        nullability: field.nullability,
        role: field.role,
        description: field.description,
        historicalRemainderPointer: field.historical_remainder_pointer,
      })),
    },
    schemaVersions: source.schema_versions.map((schema) => ({
      schemaId: schema.schema_id,
      version: schema.version,
    })),
    inputs: source.inputs.map((inputSummary) => ({
      inputId: inputSummary.input_id,
      name: inputSummary.name,
      activeProfile: {
        profileRevisionId: inputSummary.active_profile.profile_revision_id,
        revision: inputSummary.active_profile.revision,
        targetSchemaId: inputSummary.active_profile.target_schema_id,
        maximumRecordBytes: inputSummary.active_profile.maximum_record_bytes,
        eventTimeJsonPointer:
          inputSummary.active_profile.event_time_json_pointer,
        eventTimeFormat: inputSummary.active_profile.event_time_format,
      },
    })),
  };
}

export function decodeQueryExecution(input: unknown): QueryExecution {
  const response = decode(
    queryExecutionWireSchema,
    input,
    'query execution response',
  );
  return {
    queryId: response.query_id,
    sourceId: response.source_id,
    activeSchemaId: response.active_schema_id,
    activeSchemaVersion: response.active_schema_version,
    timeRange: {
      startInclusive: response.time_range.start_inclusive,
      endExclusive: response.time_range.end_exclusive,
    },
    columns: response.columns.map((column) => ({
      name: column.name,
      logicalType: column.logical_type,
      nullability: column.nullability,
    })),
    rows: response.rows,
    completion: response.completion,
    truncationReason: response.truncation_reason,
    diagnostics: response.diagnostics.map(mapDiagnostic),
    statistics: {
      selectedSegments: response.statistics.selected_segments,
      selectedParquetBytes: response.statistics.selected_parquet_bytes,
      outputRows: response.statistics.output_rows,
      outputBytes: response.statistics.output_bytes,
      elapsedMilliseconds: response.statistics.elapsed_milliseconds,
    },
  };
}

export function decodeErrorEnvelope(input: unknown): ApiErrorEnvelope {
  const response = decode(errorEnvelopeWireSchema, input, 'error response');
  const hasDiagnostics = Object.hasOwn(response.error.details, 'diagnostics');
  const diagnostics = hasDiagnostics
    ? decode(
        queryErrorDetailsWireSchema,
        response.error.details,
        'query error details',
      ).diagnostics.map(mapDiagnostic)
    : [];
  return {
    code: response.error.code,
    message: response.error.message,
    details: response.error.details,
    diagnostics,
  };
}

function decode<Schema extends z.ZodType>(
  schema: Schema,
  input: unknown,
  label: string,
): z.output<Schema> {
  const result = schema.safeParse(input);
  if (!result.success) {
    throw new ApiContractError(label, result.error);
  }
  return result.data;
}

function mapDiagnostic(
  diagnostic: z.infer<typeof queryDiagnosticWireSchema>,
): QueryDiagnostic {
  return {
    code: diagnostic.code,
    severity: diagnostic.severity,
    message: diagnostic.message,
    span:
      diagnostic.span === null
        ? null
        : {
            startByte: diagnostic.span.start_byte,
            endByte: diagnostic.span.end_byte,
          },
    sourceRange:
      diagnostic.source_range === null
        ? null
        : {
            start: diagnostic.source_range.start,
            end: diagnostic.source_range.end,
          },
  };
}

function cellMatchesColumn(
  value: JsonValue,
  column: z.infer<typeof queryColumnWireSchema>,
): boolean {
  if (value === null) {
    return column.nullability === 'NULLABLE';
  }
  switch (column.logical_type) {
    case 'bool':
      return typeof value === 'boolean';
    case 'int32':
      return (
        typeof value === 'number' &&
        Number.isInteger(value) &&
        value >= -2_147_483_648 &&
        value <= 2_147_483_647
      );
    case 'int64':
      return (
        typeof value === 'string' &&
        integerStringInRange(
          value,
          -9_223_372_036_854_775_808n,
          9_223_372_036_854_775_807n,
        )
      );
    case 'uint32':
      return (
        typeof value === 'number' &&
        Number.isInteger(value) &&
        value >= 0 &&
        value <= 4_294_967_295
      );
    case 'uint64':
      return (
        typeof value === 'string' &&
        integerStringInRange(value, 0n, 18_446_744_073_709_551_615n)
      );
    case 'float32':
    case 'float64':
      return typeof value === 'number' && Number.isFinite(value);
    case 'utf8':
      return typeof value === 'string';
    case 'datetime':
      return (
        typeof value === 'string' && utcDateTimeSchema.safeParse(value).success
      );
    case 'eid':
      return typeof value === 'string' && /^[0-9a-f]{32}$/u.test(value);
    case 'json':
      return true;
  }
}

function integerStringInRange(
  value: string,
  minimum: bigint,
  maximum: bigint,
): boolean {
  if (!/^(?:0|-?[1-9]\d*)$/u.test(value)) {
    return false;
  }
  const parsed = BigInt(value);
  return parsed >= minimum && parsed <= maximum;
}
