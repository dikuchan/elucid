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

const componentStatusSchema = z.enum(['UP', 'DEGRADED', 'DOWN']);
const listCompletionSchema = z.enum(['COMPLETE', 'TRUNCATED']);

const componentHealthWireSchema = z
  .object({
    postgresql: componentStatusSchema,
    object_store: componentStatusSchema,
    spool: componentStatusSchema,
    ingestion_worker: componentStatusSchema,
    query: componentStatusSchema,
    maintenance: componentStatusSchema,
  })
  .strict();

const effectiveLimitsWireSchema = z
  .object({
    maximum_http_batch_bytes: positiveSafeIntegerSchema,
    maximum_http_batch_records: positiveSafeIntegerSchema,
    maximum_batch_event_days: positiveSafeIntegerSchema,
    maximum_concurrent_ingestion_requests: positiveSafeIntegerSchema,
    maximum_concurrent_queries: positiveSafeIntegerSchema,
    query_timeout_seconds: positiveSafeIntegerSchema,
    maximum_query_scan_bytes: positiveSafeIntegerSchema,
    query_memory_bytes: positiveSafeIntegerSchema,
    maximum_query_result_rows: positiveSafeIntegerSchema,
    maximum_query_result_bytes: positiveSafeIntegerSchema,
    spool_capacity_bytes: positiveSafeIntegerSchema,
    scratch_capacity_bytes: positiveSafeIntegerSchema,
  })
  .strict();

const operationalStatusWireSchema = z
  .object({
    phase: z.enum(['STARTING', 'READY', 'DEGRADED', 'DRAINING']),
    admission: z.enum(['OPEN', 'CLOSED']),
    components: componentHealthWireSchema,
    limits: effectiveLimitsWireSchema,
    spool: z
      .object({
        capacity_bytes: positiveSafeIntegerSchema,
        used_bytes: safeUnsignedIntegerSchema,
        pending_batches: safeUnsignedIntegerSchema,
        oldest_queued_age_seconds: safeUnsignedIntegerSchema.nullable(),
      })
      .strict(),
    publication: z
      .object({
        status: componentStatusSchema,
        pending_batches: safeUnsignedIntegerSchema,
        prepared_segments: safeUnsignedIntegerSchema,
        planned_objects: safeUnsignedIntegerSchema,
        uploaded_objects: safeUnsignedIntegerSchema,
      })
      .strict(),
    maintenance: z
      .object({
        ownership: z.enum(['STARTING', 'DISABLED', 'OWNED', 'STANDBY']),
        recent_compactions: z.tuple([]),
      })
      .strict(),
  })
  .strict()
  .superRefine((response, context) => {
    if (response.spool.used_bytes > response.spool.capacity_bytes) {
      context.addIssue({
        code: 'custom',
        path: ['spool', 'used_bytes'],
        message: 'used_bytes must not exceed capacity_bytes',
      });
    }
    if (
      response.spool.capacity_bytes !== response.limits.spool_capacity_bytes
    ) {
      context.addIssue({
        code: 'custom',
        path: ['spool', 'capacity_bytes'],
        message: 'capacity_bytes does not match the effective spool limit',
      });
    }
  });

const eventDaySchema = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}$/u)
  .refine(isUtcCalendarDay, { message: 'event day is not a calendar date' });

const segmentWireSchema = z
  .object({
    segment_id: z.uuid(),
    source_id: sourceIdSchema,
    schema_id: schemaIdSchema,
    schema_version: positiveSafeIntegerSchema,
    state: z.enum(['PREPARED', 'ACTIVE', 'SUPERSEDED', 'EXPIRED', 'ABANDONED']),
    origin: z.enum(['INGESTION', 'COMPACTION']),
    event_day: eventDaySchema,
    row_count: positiveSafeIntegerSchema,
    uncompressed_bytes: positiveSafeIntegerSchema,
    parquet_bytes: positiveSafeIntegerSchema,
    minimum_event_time: utcDateTimeSchema,
    maximum_event_time: utcDateTimeSchema,
    minimum_ingestion_time: utcDateTimeSchema,
    maximum_ingestion_time: utcDateTimeSchema,
    published_at: utcDateTimeSchema.nullable(),
    retired_at: utcDateTimeSchema.nullable(),
  })
  .strict()
  .superRefine((segment, context) => {
    if (
      Date.parse(segment.minimum_event_time) >
      Date.parse(segment.maximum_event_time)
    ) {
      context.addIssue({
        code: 'custom',
        path: ['minimum_event_time'],
        message: 'minimum_event_time must not exceed maximum_event_time',
      });
    }
    if (
      Date.parse(segment.minimum_ingestion_time) >
      Date.parse(segment.maximum_ingestion_time)
    ) {
      context.addIssue({
        code: 'custom',
        path: ['minimum_ingestion_time'],
        message:
          'minimum_ingestion_time must not exceed maximum_ingestion_time',
      });
    }
  });

const segmentListWireSchema = z
  .object({
    completion: listCompletionSchema,
    limit: positiveSafeIntegerSchema,
    segments: z.array(segmentWireSchema),
  })
  .strict()
  .superRefine((response, context) => {
    validateBoundedList(
      response.completion,
      response.limit,
      response.segments.length,
      'segments',
      context,
    );
  });

const deadLetterSummaryWireSchema = z
  .object({
    object_id: z.uuid(),
    source_id: sourceIdSchema,
    input_id: z.uuid(),
    batch_id: z.uuid(),
    byte_size: positiveSafeIntegerSchema,
    published_at: utcDateTimeSchema,
    retention_deadline: utcDateTimeSchema,
  })
  .strict()
  .refine(
    (summary) =>
      Date.parse(summary.published_at) <=
      Date.parse(summary.retention_deadline),
    {
      path: ['retention_deadline'],
      message: 'retention_deadline must not precede published_at',
    },
  );

const deadLetterListWireSchema = z
  .object({
    completion: listCompletionSchema,
    limit: positiveSafeIntegerSchema,
    dead_letters: z.array(deadLetterSummaryWireSchema),
  })
  .strict()
  .superRefine((response, context) => {
    validateBoundedList(
      response.completion,
      response.limit,
      response.dead_letters.length,
      'dead_letters',
      context,
    );
  });

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

export type ComponentStatus = z.infer<typeof componentStatusSchema>;

export interface OperationalStatus {
  readonly phase: 'STARTING' | 'READY' | 'DEGRADED' | 'DRAINING';
  readonly admission: 'OPEN' | 'CLOSED';
  readonly components: {
    readonly postgresql: ComponentStatus;
    readonly objectStore: ComponentStatus;
    readonly spool: ComponentStatus;
    readonly ingestionWorker: ComponentStatus;
    readonly query: ComponentStatus;
    readonly maintenance: ComponentStatus;
  };
  readonly limits: {
    readonly maximumHttpBatchBytes: number;
    readonly maximumHttpBatchRecords: number;
    readonly maximumBatchEventDays: number;
    readonly maximumConcurrentIngestionRequests: number;
    readonly maximumConcurrentQueries: number;
    readonly queryTimeoutSeconds: number;
    readonly maximumQueryScanBytes: number;
    readonly queryMemoryBytes: number;
    readonly maximumQueryResultRows: number;
    readonly maximumQueryResultBytes: number;
    readonly spoolCapacityBytes: number;
    readonly scratchCapacityBytes: number;
  };
  readonly spool: {
    readonly capacityBytes: number;
    readonly usedBytes: number;
    readonly pendingBatches: number;
    readonly oldestQueuedAgeSeconds: number | null;
  };
  readonly publication: {
    readonly status: ComponentStatus;
    readonly pendingBatches: number;
    readonly preparedSegments: number;
    readonly plannedObjects: number;
    readonly uploadedObjects: number;
  };
  readonly maintenance: {
    readonly ownership: 'STARTING' | 'DISABLED' | 'OWNED' | 'STANDBY';
    readonly recentCompactions: readonly [];
  };
}

export interface SegmentSummary {
  readonly segmentId: string;
  readonly sourceId: SourceId;
  readonly schemaId: string;
  readonly schemaVersion: number;
  readonly state:
    'PREPARED' | 'ACTIVE' | 'SUPERSEDED' | 'EXPIRED' | 'ABANDONED';
  readonly origin: 'INGESTION' | 'COMPACTION';
  readonly eventDay: string;
  readonly rowCount: number;
  readonly uncompressedBytes: number;
  readonly parquetBytes: number;
  readonly minimumEventTime: string;
  readonly maximumEventTime: string;
  readonly minimumIngestionTime: string;
  readonly maximumIngestionTime: string;
  readonly publishedAt: string | null;
  readonly retiredAt: string | null;
}

export interface SegmentList {
  readonly completion: 'COMPLETE' | 'TRUNCATED';
  readonly limit: number;
  readonly segments: readonly SegmentSummary[];
}

export interface DeadLetterSummary {
  readonly objectId: string;
  readonly sourceId: SourceId;
  readonly inputId: string;
  readonly batchId: string;
  readonly byteSize: number;
  readonly publishedAt: string;
  readonly retentionDeadline: string;
}

export interface DeadLetterList {
  readonly completion: 'COMPLETE' | 'TRUNCATED';
  readonly limit: number;
  readonly deadLetters: readonly DeadLetterSummary[];
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

export function decodeOperationalStatus(input: unknown): OperationalStatus {
  const response = decode(
    operationalStatusWireSchema,
    input,
    'operational status response',
  );
  return {
    phase: response.phase,
    admission: response.admission,
    components: {
      postgresql: response.components.postgresql,
      objectStore: response.components.object_store,
      spool: response.components.spool,
      ingestionWorker: response.components.ingestion_worker,
      query: response.components.query,
      maintenance: response.components.maintenance,
    },
    limits: {
      maximumHttpBatchBytes: response.limits.maximum_http_batch_bytes,
      maximumHttpBatchRecords: response.limits.maximum_http_batch_records,
      maximumBatchEventDays: response.limits.maximum_batch_event_days,
      maximumConcurrentIngestionRequests:
        response.limits.maximum_concurrent_ingestion_requests,
      maximumConcurrentQueries: response.limits.maximum_concurrent_queries,
      queryTimeoutSeconds: response.limits.query_timeout_seconds,
      maximumQueryScanBytes: response.limits.maximum_query_scan_bytes,
      queryMemoryBytes: response.limits.query_memory_bytes,
      maximumQueryResultRows: response.limits.maximum_query_result_rows,
      maximumQueryResultBytes: response.limits.maximum_query_result_bytes,
      spoolCapacityBytes: response.limits.spool_capacity_bytes,
      scratchCapacityBytes: response.limits.scratch_capacity_bytes,
    },
    spool: {
      capacityBytes: response.spool.capacity_bytes,
      usedBytes: response.spool.used_bytes,
      pendingBatches: response.spool.pending_batches,
      oldestQueuedAgeSeconds: response.spool.oldest_queued_age_seconds,
    },
    publication: {
      status: response.publication.status,
      pendingBatches: response.publication.pending_batches,
      preparedSegments: response.publication.prepared_segments,
      plannedObjects: response.publication.planned_objects,
      uploadedObjects: response.publication.uploaded_objects,
    },
    maintenance: {
      ownership: response.maintenance.ownership,
      recentCompactions: response.maintenance.recent_compactions,
    },
  };
}

export function decodeSegmentList(input: unknown): SegmentList {
  const response = decode(
    segmentListWireSchema,
    input,
    'segment list response',
  );
  return {
    completion: response.completion,
    limit: response.limit,
    segments: response.segments.map((segment) => ({
      segmentId: segment.segment_id,
      sourceId: segment.source_id,
      schemaId: segment.schema_id,
      schemaVersion: segment.schema_version,
      state: segment.state,
      origin: segment.origin,
      eventDay: segment.event_day,
      rowCount: segment.row_count,
      uncompressedBytes: segment.uncompressed_bytes,
      parquetBytes: segment.parquet_bytes,
      minimumEventTime: segment.minimum_event_time,
      maximumEventTime: segment.maximum_event_time,
      minimumIngestionTime: segment.minimum_ingestion_time,
      maximumIngestionTime: segment.maximum_ingestion_time,
      publishedAt: segment.published_at,
      retiredAt: segment.retired_at,
    })),
  };
}

export function decodeDeadLetterList(input: unknown): DeadLetterList {
  const response = decode(
    deadLetterListWireSchema,
    input,
    'dead-letter list response',
  );
  return {
    completion: response.completion,
    limit: response.limit,
    deadLetters: response.dead_letters.map((summary) => ({
      objectId: summary.object_id,
      sourceId: summary.source_id,
      inputId: summary.input_id,
      batchId: summary.batch_id,
      byteSize: summary.byte_size,
      publishedAt: summary.published_at,
      retentionDeadline: summary.retention_deadline,
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

function isUtcCalendarDay(value: string): boolean {
  const milliseconds = Date.parse(`${value}T00:00:00.000Z`);
  return (
    Number.isFinite(milliseconds) &&
    new Date(milliseconds).toISOString().slice(0, 10) === value
  );
}

function validateBoundedList(
  completion: 'COMPLETE' | 'TRUNCATED',
  limit: number,
  itemCount: number,
  itemPath: string,
  context: z.RefinementCtx,
): void {
  if (itemCount > limit) {
    context.addIssue({
      code: 'custom',
      path: [itemPath],
      message: 'item count exceeds the reported limit',
    });
  }
  if (completion === 'TRUNCATED' && itemCount !== limit) {
    context.addIssue({
      code: 'custom',
      path: ['completion'],
      message: 'a truncated list must contain exactly the reported limit',
    });
  }
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
