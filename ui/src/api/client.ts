import {
  ApiContractError,
  decodeErrorEnvelope,
  decodeQueryExecution,
  decodeSourceDetail,
  decodeSourceList,
} from './contracts';
import type {
  QueryDiagnostic,
  QueryExecution,
  SourceDetail,
  SourceId,
  SourceList,
} from './contracts';

export interface QueryExecutionRequest {
  readonly query: string;
  readonly timeRange: {
    readonly startInclusive: string;
    readonly endExclusive: string;
  };
  readonly outputRows: number;
}

type ApiFailure =
  | { readonly kind: 'aborted' }
  | { readonly kind: 'network'; readonly reason: string }
  | {
      readonly kind: 'http';
      readonly status: number;
      readonly code: string;
      readonly message: string;
      readonly diagnostics: readonly QueryDiagnostic[];
      readonly requestId: string | null;
      readonly retryAfterMilliseconds: number | null;
    }
  | {
      readonly kind: 'invalid-response';
      readonly status: number;
      readonly requestId: string | null;
      readonly reason: string;
    };

export class ApiClientError extends Error {
  readonly failure: ApiFailure;

  constructor(failure: ApiFailure) {
    super(failureMessage(failure));
    this.name = 'ApiClientError';
    this.failure = failure;
  }
}

export async function listSources(signal?: AbortSignal): Promise<SourceList> {
  return requestJson('/api/v1/sources', getRequest(signal), decodeSourceList);
}

export async function getSource(
  sourceId: SourceId,
  signal?: AbortSignal,
): Promise<SourceDetail> {
  return requestJson(
    `/api/v1/sources/${encodeURIComponent(sourceId)}`,
    getRequest(signal),
    decodeSourceDetail,
  );
}

function getRequest(signal: AbortSignal | undefined): RequestInit {
  return signal === undefined ? { method: 'GET' } : { method: 'GET', signal };
}

export async function executeQuery(
  request: QueryExecutionRequest,
  signal: AbortSignal,
): Promise<QueryExecution> {
  return requestJson(
    '/api/v1/query-executions',
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: request.query,
        time_range: {
          start_inclusive: request.timeRange.startInclusive,
          end_exclusive: request.timeRange.endExclusive,
        },
        output_rows: request.outputRows,
      }),
      signal,
    },
    decodeQueryExecution,
  );
}

export function shouldRetryRead(failureCount: number, error: Error): boolean {
  if (failureCount >= 2 || !(error instanceof ApiClientError)) {
    return false;
  }
  switch (error.failure.kind) {
    case 'network':
      return true;
    case 'http':
      return (
        error.failure.status === 408 ||
        error.failure.status === 425 ||
        error.failure.status === 429 ||
        error.failure.status >= 500
      );
    case 'aborted':
    case 'invalid-response':
      return false;
  }
}

export function readRetryDelay(attemptIndex: number, error: Error): number {
  if (
    error instanceof ApiClientError &&
    error.failure.kind === 'http' &&
    error.failure.retryAfterMilliseconds !== null
  ) {
    return error.failure.retryAfterMilliseconds;
  }
  return Math.min(250 * 2 ** attemptIndex, 2000);
}

async function requestJson<Result>(
  path: string,
  init: RequestInit,
  decode: (input: unknown) => Result,
): Promise<Result> {
  let response: Response;
  try {
    response = await fetch(path, init);
  } catch (error: unknown) {
    if (init.signal?.aborted === true || isAbortError(error)) {
      throw new ApiClientError({ kind: 'aborted' });
    }
    throw new ApiClientError({
      kind: 'network',
      reason: error instanceof Error ? error.message : 'Unknown network error',
    });
  }

  const requestId = response.headers.get('X-Request-Id');
  const payload = await parseJson(response, requestId);
  if (!response.ok) {
    let envelope;
    try {
      envelope = decodeErrorEnvelope(payload);
    } catch (error: unknown) {
      throw invalidResponseError(response.status, requestId, error);
    }
    throw new ApiClientError({
      kind: 'http',
      status: response.status,
      code: envelope.code,
      message: envelope.message,
      diagnostics: envelope.diagnostics,
      requestId,
      retryAfterMilliseconds: retryAfterMilliseconds(response.headers),
    });
  }

  try {
    return decode(payload);
  } catch (error: unknown) {
    throw invalidResponseError(response.status, requestId, error);
  }
}

async function parseJson(
  response: Response,
  requestId: string | null,
): Promise<unknown> {
  const text = await response.text();
  try {
    return JSON.parse(text) as unknown;
  } catch (error: unknown) {
    throw invalidResponseError(response.status, requestId, error);
  }
}

function invalidResponseError(
  status: number,
  requestId: string | null,
  error: unknown,
): ApiClientError {
  return new ApiClientError({
    kind: 'invalid-response',
    status,
    requestId,
    reason:
      error instanceof ApiContractError || error instanceof SyntaxError
        ? error.message
        : 'Response could not be decoded',
  });
}

function retryAfterMilliseconds(headers: Headers): number | null {
  const raw = headers.get('Retry-After');
  if (raw === null || !/^\d+$/u.test(raw)) {
    return null;
  }
  const seconds = Number(raw);
  if (!Number.isSafeInteger(seconds) || seconds < 0 || seconds > 30) {
    return null;
  }
  return seconds * 1000;
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError';
}

function failureMessage(failure: ApiFailure): string {
  switch (failure.kind) {
    case 'aborted':
      return 'Query was cancelled';
    case 'network':
      return `Network request failed: ${failure.reason}`;
    case 'http':
      return `${failure.code}: ${failure.message}`;
    case 'invalid-response':
      return `Invalid API response: ${failure.reason}`;
  }
}
