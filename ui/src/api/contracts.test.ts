import { describe, expect, test } from 'vitest';

import {
  decodeErrorEnvelope,
  decodeQueryExecution,
  decodeQueryExecutionList,
  decodeSourceDetail,
} from './contracts';

const queryResponse = {
  query_id: '019d1234-5678-7abc-8123-456789abcdef',
  source_id: '019d1234-5678-7abc-8123-456789abcdea',
  active_schema_id: '019d1234-5678-7abc-8123-456789abcdeb',
  active_schema_version: 2,
  time_range: {
    start_inclusive: '2026-08-20T00:00:00.000Z',
    end_exclusive: '2026-08-21T00:00:00.000Z',
  },
  columns: [
    {
      name: 'sequence',
      logical_type: 'int64',
      nullability: 'NON_NULL',
    },
    {
      name: 'context',
      logical_type: 'json',
      nullability: 'NULLABLE',
    },
  ],
  rows: [['9223372036854775807', { region: 'eu' }]],
  completion: 'TRUNCATED',
  truncation_reason: 'OUTPUT_ROWS',
  diagnostics: [],
  statistics: {
    selected_segments: 2,
    selected_parquet_bytes: 4096,
    output_rows: 1,
    output_bytes: 64,
    elapsed_milliseconds: 7,
  },
} as const;

describe('API response decoding', () => {
  test('keeps encoded 64-bit values and validates row cells against typed columns', () => {
    const decoded = decodeQueryExecution(queryResponse);

    expect(decoded.rows[0]?.[0]).toBe('9223372036854775807');
    expect(() =>
      decodeQueryExecution({
        ...queryResponse,
        rows: [[Number('9223372036854775807'), { region: 'eu' }]],
      }),
    ).toThrow(/int64/iu);
    expect(() =>
      decodeQueryExecution({ ...queryResponse, rows: [['1']] }),
    ).toThrow(/columns/iu);
  });

  test('rejects an impossible completion and truncation combination', () => {
    expect(() =>
      decodeQueryExecution({
        ...queryResponse,
        completion: 'COMPLETE',
      }),
    ).toThrow(/truncation_reason/iu);
  });

  test('retains query diagnostics and rejects unknown source fields', () => {
    const failure = decodeErrorEnvelope({
      error: {
        code: 'QUERY_SEMANTIC_ERROR',
        message: 'Query semantic analysis failed',
        details: {
          diagnostics: [
            {
              code: 'QUERY_FIELD_NOT_FOUND',
              severity: 'ERROR',
              message: 'Field `statuz` was not found',
              span: { start_byte: 27, end_byte: 33 },
              source_range: {
                start: { line: 1, column: 28 },
                end: { line: 1, column: 34 },
              },
            },
          ],
        },
      },
    });

    expect(failure.diagnostics[0]?.span).toEqual({
      startByte: 27,
      endByte: 33,
    });
    expect(() =>
      decodeSourceDetail({
        source_id: '019d1234-5678-7abc-8123-456789abcdea',
        name: 'demo_logs',
        display_name: 'Demo logs',
        active_schema: {
          schema_id: '019d1234-5678-7abc-8123-456789abcdeb',
          version: 1,
          fields: [],
        },
        schema_versions: [],
        inputs: [],
        invented_statistics: { rows: 42 },
      }),
    ).toThrow(/unrecognized/iu);
  });

  test('decodes a bounded query execution list without losing 64-bit row limits', () => {
    const decoded = decodeQueryExecutionList({
      completion: 'COMPLETE',
      limit: 50,
      query_executions: [
        {
          query_id: '019d1234-5678-7abc-8123-456789abcdef',
          query: 'source demo_logs | take 100',
          time_range: {
            start_inclusive: '2026-08-20T00:00:00.123Z',
            end_exclusive: '2026-08-21T00:00:00.456Z',
          },
          output_rows: '18446744073709551615',
          submitted_at: '2026-08-23T12:34:56.789Z',
        },
      ],
    });

    expect(decoded.queryExecutions[0]?.outputRows).toBe('18446744073709551615');
    expect(() =>
      decodeQueryExecutionList({
        completion: 'TRUNCATED',
        limit: 2,
        query_executions: [],
      }),
    ).toThrow(/truncated/iu);
  });
});
