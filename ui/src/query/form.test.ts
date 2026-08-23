import { describe, expect, test } from 'vitest';

import { buildQueryRequest, queryFormFromExecution } from './form';

describe('query request form', () => {
  test('treats datetime-local text as an explicit UTC range', () => {
    expect(
      buildQueryRequest({
        query: 'source demo_logs | take 100',
        startUtc: '2026-08-20T12:34',
        endUtc: '2026-08-21T01:02:03',
        outputRows: '1000',
      }),
    ).toEqual({
      kind: 'valid',
      request: {
        query: 'source demo_logs | take 100',
        timeRange: {
          startInclusive: '2026-08-20T12:34:00.000Z',
          endExclusive: '2026-08-21T01:02:03.000Z',
        },
        outputRows: 1000,
      },
    });
  });

  test('preserves query text exactly because backend spans refer to its UTF-8 bytes', () => {
    const query = '  source demo_logs | take 100\n';

    const result = buildQueryRequest({
      query,
      startUtc: '2026-08-20T00:00',
      endUtc: '2026-08-21T00:00',
      outputRows: '100',
    });

    expect(result.kind).toBe('valid');
    if (result.kind === 'valid') {
      expect(result.request.query).toBe(query);
    }
  });

  test('rejects an empty query, reversed range, and unsafe row count at the client boundary', () => {
    expect(
      buildQueryRequest({
        query: '   ',
        startUtc: '2026-08-21T00:00',
        endUtc: '2026-08-20T00:00',
        outputRows: '9007199254740992',
      }),
    ).toEqual({
      kind: 'invalid',
      problems: [
        'Enter a query.',
        'The UTC end must be later than the start.',
        'Output rows must be a positive safe integer.',
      ],
    });
  });

  test('restores an execution request exactly enough to run it again', () => {
    const form = queryFormFromExecution({
      queryId: '019d1234-5678-7abc-8123-456789abcdef',
      query: 'source demo_logs\n| take 100',
      timeRange: {
        startInclusive: '2026-08-20T12:34:56.123Z',
        endExclusive: '2026-08-21T01:02:03.456Z',
      },
      outputRows: '100',
      submittedAt: '2026-08-23T12:34:56.789Z',
    });

    expect(form).toEqual({
      query: 'source demo_logs\n| take 100',
      startUtc: '2026-08-20T12:34:56.123',
      endUtc: '2026-08-21T01:02:03.456',
      outputRows: '100',
    });
    expect(buildQueryRequest(form)).toEqual({
      kind: 'valid',
      request: {
        query: 'source demo_logs\n| take 100',
        timeRange: {
          startInclusive: '2026-08-20T12:34:56.123Z',
          endExclusive: '2026-08-21T01:02:03.456Z',
        },
        outputRows: 100,
      },
    });
  });
});
