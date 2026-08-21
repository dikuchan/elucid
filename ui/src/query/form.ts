import type { QueryExecutionRequest } from '../api/client';

export interface QueryFormValues {
  readonly query: string;
  readonly startUtc: string;
  readonly endUtc: string;
  readonly outputRows: string;
}

type QueryFormResult =
  | {
      readonly kind: 'valid';
      readonly request: QueryExecutionRequest;
    }
  | {
      readonly kind: 'invalid';
      readonly problems: readonly string[];
    };

export function buildQueryRequest(values: QueryFormValues): QueryFormResult {
  const problems: string[] = [];
  if (values.query.trim().length === 0) {
    problems.push('Enter a query.');
  }

  const startInclusive = parseUtcInput(values.startUtc);
  const endExclusive = parseUtcInput(values.endUtc);
  if (startInclusive === null || endExclusive === null) {
    problems.push('Enter a valid UTC range.');
  } else if (Date.parse(startInclusive) >= Date.parse(endExclusive)) {
    problems.push('The UTC end must be later than the start.');
  }

  const outputRows = parseOutputRows(values.outputRows);
  if (outputRows === null) {
    problems.push('Output rows must be a positive safe integer.');
  }

  if (
    problems.length > 0 ||
    startInclusive === null ||
    endExclusive === null ||
    outputRows === null
  ) {
    return { kind: 'invalid', problems };
  }
  return {
    kind: 'valid',
    request: {
      query: values.query,
      timeRange: { startInclusive, endExclusive },
      outputRows,
    },
  };
}

export function initialQueryForm(now: Date = new Date()): QueryFormValues {
  const endMilliseconds = Math.floor(now.getTime() / 60_000) * 60_000;
  const startMilliseconds = endMilliseconds - 24 * 60 * 60 * 1000;
  return {
    query: '',
    startUtc: toUtcInput(new Date(startMilliseconds)),
    endUtc: toUtcInput(new Date(endMilliseconds)),
    outputRows: '1000',
  };
}

export function defaultQueryForSource(sourceName: string): string {
  return `source ${sourceName}\n| sort by -@event_time\n| take 100`;
}

function parseUtcInput(value: string): string | null {
  const match = /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/u.exec(
    value,
  );
  if (match === null) {
    return null;
  }
  const date = match[1];
  const hours = match[2];
  const minutes = match[3];
  const seconds = match[4] ?? '00';
  if (date === undefined || hours === undefined || minutes === undefined) {
    return null;
  }
  const canonical = `${date}T${hours}:${minutes}:${seconds}.000Z`;
  const milliseconds = Date.parse(canonical);
  if (
    !Number.isFinite(milliseconds) ||
    new Date(milliseconds).toISOString() !== canonical
  ) {
    return null;
  }
  return canonical;
}

function parseOutputRows(value: string): number | null {
  if (!/^[1-9]\d*$/u.test(value)) {
    return null;
  }
  const rows = Number(value);
  return Number.isSafeInteger(rows) ? rows : null;
}

function toUtcInput(value: Date): string {
  return value.toISOString().slice(0, 16);
}
