import { describe, expect, test } from 'vitest';

import {
  decodeDeadLetterList,
  decodeOperationalStatus,
  decodeSegmentList,
} from './contracts';

const sourceId = '019d1234-5678-7abc-8123-456789abcdea';

const statusResponse = {
  phase: 'DEGRADED',
  admission: 'CLOSED',
  components: {
    postgresql: 'DEGRADED',
    object_store: 'DOWN',
    spool: 'UP',
    ingestion_worker: 'UP',
    query: 'UP',
    maintenance: 'UP',
  },
  limits: {
    maximum_http_batch_bytes: 1_048_576,
    maximum_http_batch_records: 100_000,
    maximum_batch_event_days: 32,
    maximum_concurrent_ingestion_requests: 8,
    maximum_concurrent_queries: 4,
    query_timeout_seconds: 30,
    maximum_query_scan_bytes: 536_870_912,
    query_memory_bytes: 268_435_456,
    maximum_query_result_rows: 10_000,
    maximum_query_result_bytes: 16_777_216,
    spool_capacity_bytes: 1_073_741_824,
    scratch_capacity_bytes: 2_147_483_648,
  },
  spool: {
    capacity_bytes: 1_073_741_824,
    used_bytes: 4_096,
    pending_batches: 2,
    oldest_queued_age_seconds: 12,
  },
  publication: {
    status: 'DOWN',
    pending_batches: 2,
    prepared_segments: 1,
    planned_objects: 1,
    uploaded_objects: 0,
  },
  maintenance: {
    ownership: 'STANDBY',
    recent_compactions: [],
  },
} as const;

const segmentListResponse = {
  completion: 'COMPLETE',
  limit: 100,
  segments: [
    {
      segment_id: '019d1234-5678-7abc-8123-456789abcdef',
      source_id: sourceId,
      schema_id: '019d1234-5678-7abc-8123-456789abcdeb',
      schema_version: 2,
      state: 'ACTIVE',
      origin: 'INGESTION',
      event_day: '2026-08-20',
      row_count: 42,
      uncompressed_bytes: 8_192,
      parquet_bytes: 2_048,
      minimum_event_time: '2026-08-20T00:00:00.000Z',
      maximum_event_time: '2026-08-20T23:59:59.999Z',
      minimum_ingestion_time: '2026-08-20T00:00:01.000Z',
      maximum_ingestion_time: '2026-08-21T00:00:01.000Z',
      published_at: '2026-08-21T00:00:02.000Z',
      retired_at: null,
    },
  ],
} as const;

const deadLetterListResponse = {
  completion: 'COMPLETE',
  limit: 100,
  dead_letters: [
    {
      object_id: '019d1234-5678-7abc-8123-456789abcd00',
      source_id: sourceId,
      input_id: '019d1234-5678-7abc-8123-456789abcd01',
      batch_id: '019d1234-5678-7abc-8123-456789abcd02',
      byte_size: 512,
      published_at: '2026-08-20T12:00:00.000Z',
      retention_deadline: '2026-09-19T12:00:00.000Z',
    },
  ],
} as const;

describe('operational API response decoding', () => {
  test('decodes health and bounded ingestion state without leaking wire names', () => {
    const decoded = decodeOperationalStatus(statusResponse);

    expect(decoded.components.objectStore).toBe('DOWN');
    expect(decoded.spool).toEqual({
      capacityBytes: 1_073_741_824,
      usedBytes: 4_096,
      pendingBatches: 2,
      oldestQueuedAgeSeconds: 12,
    });
    expect(decoded.publication.preparedSegments).toBe(1);
    expect(decoded.maintenance.ownership).toBe('STANDBY');

    expect(() =>
      decodeOperationalStatus({
        ...statusResponse,
        spool: {
          ...statusResponse.spool,
          used_bytes: statusResponse.spool.capacity_bytes + 1,
        },
      }),
    ).toThrow(/used_bytes/iu);
  });

  test('decodes exact segment identities and rejects inverted time bounds', () => {
    const decoded = decodeSegmentList(segmentListResponse);

    expect(decoded.segments[0]).toMatchObject({
      sourceId,
      schemaVersion: 2,
      state: 'ACTIVE',
      origin: 'INGESTION',
      eventDay: '2026-08-20',
      rowCount: 42,
      parquetBytes: 2_048,
    });

    expect(() =>
      decodeSegmentList({
        ...segmentListResponse,
        segments: [
          {
            ...segmentListResponse.segments[0],
            minimum_event_time: '2026-08-21T00:00:00.000Z',
          },
        ],
      }),
    ).toThrow(/minimum_event_time/iu);
  });

  test('decodes dead-letter summaries and rejects an inverted retention interval', () => {
    const decoded = decodeDeadLetterList(deadLetterListResponse);

    expect(decoded.deadLetters[0]).toMatchObject({
      sourceId,
      byteSize: 512,
      publishedAt: '2026-08-20T12:00:00.000Z',
    });

    expect(() =>
      decodeDeadLetterList({
        ...deadLetterListResponse,
        dead_letters: [
          {
            ...deadLetterListResponse.dead_letters[0],
            retention_deadline: '2026-08-19T12:00:00.000Z',
          },
        ],
      }),
    ).toThrow(/retention_deadline/iu);
  });
});
