import { lazy, Suspense, useEffect, useRef, useState } from 'react';
import { skipToken, useMutation, useQuery } from '@tanstack/react-query';
import {
  Alert,
  AppShell,
  Badge,
  Box,
  Button,
  Group,
  Paper,
  SegmentedControl,
  Skeleton,
  Stack,
  Text,
  TextInput,
  Title,
} from '@mantine/core';

import {
  ApiClientError,
  executeQuery,
  getOperationalStatus,
  getSource,
  listDeadLetters,
  listSegments,
  listSources,
  readRetryDelay,
  shouldRetryRead,
} from './api/client';
import type { QueryExecutionRequest } from './api/client';
import type {
  OperationalStatus,
  QueryDiagnostic,
  QueryExecution,
  SourceDetail,
  SourceId,
  SourceList,
  SourceSummary,
} from './api/contracts';
import { OperationsWorkspace } from './components/OperationsWorkspace';
import type {
  OperationalStatusState,
  SourceOperationalState,
} from './components/OperationsWorkspace';
import { QueryResults } from './components/QueryResults';
import type { QueryResultState } from './components/QueryResults';
import { SchemaRail } from './components/SchemaRail';
import type { SchemaRailState } from './components/SchemaRail';
import { SourceRail } from './components/SourceRail';
import type { SourceRailState } from './components/SourceRail';
import {
  buildQueryRequest,
  defaultQueryForSource,
  initialQueryForm,
} from './query/form';
import classes from './App.module.css';

const QueryEditor = lazy(async () => {
  const module = await import('./components/QueryEditor');
  return { default: module.QueryEditor };
});

interface RunningQuery {
  readonly request: QueryExecutionRequest;
  readonly controller: AbortController;
}

type RunDisposition = 'ordinary' | 'cancellation-requested' | 'cancelled';
type WorkspaceMode = 'query' | 'operations';

const OPERATIONAL_STATUS_REFRESH_MILLISECONDS = 2_500;
const OPERATIONAL_LIST_REFRESH_MILLISECONDS = 5_000;

export function App() {
  const [workspaceMode, setWorkspaceMode] = useState<WorkspaceMode>('query');
  const sourcesQuery = useQuery({
    queryKey: ['sources'],
    queryFn: ({ signal }) => listSources(signal),
    retry: shouldRetryRead,
    retryDelay: readRetryDelay,
    staleTime: 10_000,
  });
  const operationalStatusQuery = useQuery({
    queryKey: ['operational-status'],
    queryFn: ({ signal }) => getOperationalStatus(signal),
    retry: shouldRetryRead,
    retryDelay: readRetryDelay,
    staleTime: 1_000,
    refetchInterval: OPERATIONAL_STATUS_REFRESH_MILLISECONDS,
    refetchIntervalInBackground: false,
  });
  const [requestedSourceId, setRequestedSourceId] = useState<SourceId | null>(
    null,
  );
  const [form, setForm] = useState(initialQueryForm);
  const [formProblems, setFormProblems] = useState<readonly string[]>([]);
  const [runDisposition, setRunDisposition] =
    useState<RunDisposition>('ordinary');
  const abortControllerRef = useRef<AbortController | null>(null);
  const previousSourceIdRef = useRef<SourceId | null>(null);

  const sources = sourcesQuery.data?.sources ?? [];
  const selectedSource = selectSource(sources, requestedSourceId);
  const sourceDetailQuery = useQuery({
    queryKey: ['source', selectedSource?.sourceId ?? null],
    queryFn:
      selectedSource === null
        ? skipToken
        : ({ signal }) => getSource(selectedSource.sourceId, signal),
    retry: shouldRetryRead,
    retryDelay: readRetryDelay,
    staleTime: 10_000,
  });
  const sourceOperationsEnabled =
    workspaceMode === 'operations' && selectedSource !== null;
  const segmentQuery = useQuery({
    queryKey: ['segments', selectedSource?.sourceId ?? null],
    queryFn:
      workspaceMode !== 'operations' || selectedSource === null
        ? skipToken
        : ({ signal }) => listSegments(selectedSource.sourceId, signal),
    retry: shouldRetryRead,
    retryDelay: readRetryDelay,
    staleTime: 2_000,
    refetchInterval: sourceOperationsEnabled
      ? OPERATIONAL_LIST_REFRESH_MILLISECONDS
      : false,
    refetchIntervalInBackground: false,
  });
  const deadLetterQuery = useQuery({
    queryKey: ['dead-letters', selectedSource?.sourceId ?? null],
    queryFn:
      workspaceMode !== 'operations' || selectedSource === null
        ? skipToken
        : ({ signal }) => listDeadLetters(selectedSource.sourceId, signal),
    retry: shouldRetryRead,
    retryDelay: readRetryDelay,
    staleTime: 2_000,
    refetchInterval: sourceOperationsEnabled
      ? OPERATIONAL_LIST_REFRESH_MILLISECONDS
      : false,
    refetchIntervalInBackground: false,
  });

  const queryMutation = useMutation({
    mutationFn: ({ request, controller }: RunningQuery) =>
      executeQuery(request, controller.signal),
    retry: false,
    onMutate: () => {
      setRunDisposition('ordinary');
    },
    onSuccess: () => {
      setRunDisposition('ordinary');
    },
    onError: (error) => {
      setRunDisposition(isCancellation(error) ? 'cancelled' : 'ordinary');
    },
    onSettled: (_data, _error, variables) => {
      if (abortControllerRef.current === variables.controller) {
        abortControllerRef.current = null;
      }
    },
  });
  const resetQuery = queryMutation.reset;

  useEffect(() => {
    if (
      selectedSource === null ||
      previousSourceIdRef.current === selectedSource.sourceId
    ) {
      return;
    }
    previousSourceIdRef.current = selectedSource.sourceId;
    abortControllerRef.current?.abort();
    resetQuery();
    setRunDisposition('ordinary');
    setFormProblems([]);
    setForm((current) => ({
      ...current,
      query: defaultQueryForSource(selectedSource.name),
    }));
  }, [resetQuery, selectedSource]);

  const sourceRailState = deriveSourceRailState(
    sourcesQuery.status,
    sourcesQuery.data,
    sourcesQuery.error,
    selectedSource,
  );
  const schemaRailState = deriveSchemaRailState(
    selectedSource,
    sourceDetailQuery.status,
    sourceDetailQuery.data,
    sourceDetailQuery.error,
  );
  const resultState = deriveResultState(
    queryMutation.status,
    queryMutation.data,
    queryMutation.error,
    runDisposition,
  );
  const diagnostics = currentDiagnostics(
    form.query,
    queryMutation.variables,
    queryMutation.data?.diagnostics ?? [],
    queryMutation.error,
  );
  const operationalStatusState = deriveOperationalStatusState(
    operationalStatusQuery.status,
    operationalStatusQuery.data,
    operationalStatusQuery.error,
  );
  const segmentState = deriveSourceOperationalState(
    selectedSource,
    segmentQuery.status,
    segmentQuery.data,
    segmentQuery.error,
  );
  const deadLetterState = deriveSourceOperationalState(
    selectedSource,
    deadLetterQuery.status,
    deadLetterQuery.data,
    deadLetterQuery.error,
  );

  const runQuery = () => {
    if (queryMutation.isPending || selectedSource === null) {
      return;
    }
    const outcome = buildQueryRequest(form);
    if (outcome.kind === 'invalid') {
      setFormProblems(outcome.problems);
      return;
    }
    const controller = new AbortController();
    abortControllerRef.current = controller;
    setFormProblems([]);
    queryMutation.mutate({ request: outcome.request, controller });
  };

  const cancelQuery = () => {
    if (abortControllerRef.current !== null) {
      setRunDisposition('cancellation-requested');
      abortControllerRef.current.abort();
    }
  };

  const chooseSource = (sourceId: SourceId) => {
    if (sourceId === selectedSource?.sourceId) {
      return;
    }
    previousSourceIdRef.current = sourceId;
    abortControllerRef.current?.abort();
    resetQuery();
    setRunDisposition('ordinary');
    setFormProblems([]);
    setRequestedSourceId(sourceId);
    const source = sources.find((candidate) => candidate.sourceId === sourceId);
    if (source !== undefined) {
      setForm((current) => ({
        ...current,
        query: defaultQueryForSource(source.name),
      }));
    }
  };

  const refreshOperations = () => {
    void operationalStatusQuery.refetch();
    if (selectedSource !== null) {
      void segmentQuery.refetch();
      void deadLetterQuery.refetch();
    }
  };

  return (
    <AppShell
      header={{ height: 58 }}
      navbar={{ width: 248, breakpoint: 'md', collapsed: { mobile: true } }}
      aside={{ width: 320, breakpoint: 'lg', collapsed: { mobile: true } }}
      padding={0}
      className={classes.shell}
    >
      <AppShell.Header className={classes.header}>
        <Group h="100%" px="md" justify="space-between" wrap="nowrap">
          <Group gap="sm" wrap="nowrap">
            <Box className={classes.brandMark} aria-hidden="true">
              E
            </Box>
            <Box>
              <Title order={1} size="h4" className={classes.brandTitle}>
                Elucid
              </Title>
              <Text size="xs" c="dimmed">
                Event data workspace
              </Text>
            </Box>
          </Group>
          <Group gap="xs" wrap="nowrap">
            <SegmentedControl
              size="xs"
              value={workspaceMode}
              data={[
                { label: 'Query', value: 'query' },
                { label: 'Operations', value: 'operations' },
              ]}
              onChange={(value) => {
                if (isWorkspaceMode(value)) {
                  setWorkspaceMode(value);
                }
              }}
              aria-label="Workspace view"
            />
            <ServiceBadge state={operationalStatusState} />
            <Badge
              variant="dot"
              color="cyan"
              size="sm"
              className={classes.sameOriginBadge}
            >
              same origin
            </Badge>
          </Group>
        </Group>
      </AppShell.Header>

      <AppShell.Navbar className={classes.rail}>
        <SourceRail
          state={sourceRailState}
          onSelect={chooseSource}
          onRetry={() => {
            void sourcesQuery.refetch();
          }}
        />
      </AppShell.Navbar>

      <AppShell.Main className={classes.main}>
        {workspaceMode === 'query' ? (
          <Box className={classes.workspace}>
            <Paper
              withBorder
              radius="md"
              p="md"
              className={classes.contextPanel}
            >
              <Group
                justify="space-between"
                align="flex-end"
                wrap="wrap"
                gap="md"
              >
                <Box>
                  <Text
                    size="xs"
                    fw={700}
                    tt="uppercase"
                    c="dimmed"
                    className={classes.eyebrow}
                  >
                    Selected source
                  </Text>
                  <Group gap="xs" mt={4}>
                    <Text fw={650}>{selectedSource?.name ?? 'None'}</Text>
                    {selectedSource === null ? null : (
                      <Badge size="xs" variant="light" color="cyan">
                        active schema v{selectedSource.activeSchemaVersion}
                      </Badge>
                    )}
                  </Group>
                </Box>
                <Group gap="xs" align="flex-end" wrap="wrap">
                  <TextInput
                    label="From · UTC"
                    type="datetime-local"
                    step={1}
                    value={form.startUtc}
                    onChange={(event) => {
                      const value = event.currentTarget.value;
                      setForm((current) => ({
                        ...current,
                        startUtc: value,
                      }));
                      setFormProblems([]);
                    }}
                    disabled={queryMutation.isPending}
                    size="xs"
                    className={classes.timeInput}
                  />
                  <TextInput
                    label="To · UTC"
                    type="datetime-local"
                    step={1}
                    value={form.endUtc}
                    onChange={(event) => {
                      const value = event.currentTarget.value;
                      setForm((current) => ({
                        ...current,
                        endUtc: value,
                      }));
                      setFormProblems([]);
                    }}
                    disabled={queryMutation.isPending}
                    size="xs"
                    className={classes.timeInput}
                  />
                  <TextInput
                    label="Output rows"
                    type="number"
                    min={1}
                    step={1}
                    value={form.outputRows}
                    onChange={(event) => {
                      const value = event.currentTarget.value;
                      setForm((current) => ({
                        ...current,
                        outputRows: value,
                      }));
                      setFormProblems([]);
                    }}
                    disabled={queryMutation.isPending}
                    size="xs"
                    className={classes.rowsInput}
                  />
                  <Button
                    size="xs"
                    variant="default"
                    disabled={!queryMutation.isPending}
                    onClick={cancelQuery}
                  >
                    Cancel
                  </Button>
                  <Button
                    size="xs"
                    disabled={
                      selectedSource === null || queryMutation.isPending
                    }
                    onClick={runQuery}
                  >
                    Run query
                  </Button>
                </Group>
              </Group>
            </Paper>

            {formProblems.length === 0 ? null : (
              <Alert color="red" variant="light" title="Cannot run query">
                <Stack gap={2}>
                  {formProblems.map((problem) => (
                    <Text key={problem} size="xs">
                      {problem}
                    </Text>
                  ))}
                </Stack>
              </Alert>
            )}

            <Paper withBorder radius="md" className={classes.editorPanel}>
              <Group
                justify="space-between"
                px="md"
                py="sm"
                className={classes.panelHeader}
              >
                <Group gap="xs">
                  <Text size="sm" fw={650}>
                    Query
                  </Text>
                  <Badge size="xs" variant="outline" color="gray">
                    Elucid QL
                  </Badge>
                </Group>
                <Text size="xs" c="dimmed">
                  Ctrl/⌘ ↵ to run
                </Text>
              </Group>
              <Suspense fallback={<Skeleton height={152} radius={0} />}>
                <QueryEditor
                  value={form.query}
                  diagnostics={diagnostics}
                  disabled={queryMutation.isPending}
                  onChange={(query) => {
                    setForm((current) => ({ ...current, query }));
                    setFormProblems([]);
                  }}
                  onRun={runQuery}
                />
              </Suspense>
              <QueryDiagnostics diagnostics={diagnostics} />
            </Paper>

            <QueryResults state={resultState} />
          </Box>
        ) : (
          <OperationsWorkspace
            source={selectedSource}
            statusState={operationalStatusState}
            segmentState={segmentState}
            deadLetterState={deadLetterState}
            onRefresh={refreshOperations}
          />
        )}
      </AppShell.Main>

      <AppShell.Aside className={classes.rail}>
        <SchemaRail
          state={schemaRailState}
          onRetry={() => {
            void sourceDetailQuery.refetch();
          }}
        />
      </AppShell.Aside>
    </AppShell>
  );
}

function QueryDiagnostics({
  diagnostics,
}: Readonly<{ diagnostics: readonly QueryDiagnostic[] }>) {
  if (diagnostics.length === 0) {
    return null;
  }
  return (
    <Stack gap={0} className={classes.diagnostics} aria-live="polite">
      {diagnostics.map((diagnostic, index) => (
        <Group
          key={`${diagnostic.code}-${String(index)}`}
          px="md"
          py="sm"
          gap="sm"
          align="flex-start"
          wrap="nowrap"
          className={classes.diagnostic}
        >
          <Badge
            color={diagnostic.severity === 'ERROR' ? 'red' : 'yellow'}
            variant="light"
            size="xs"
          >
            {diagnostic.severity === 'ERROR' ? 'Error' : 'Warning'}
          </Badge>
          <Box>
            <Group gap="xs">
              <Text size="xs" fw={650} className={classes.identity}>
                {diagnostic.code}
              </Text>
              {diagnostic.sourceRange === null ? null : (
                <Text size="xs" c="dimmed">
                  line {diagnostic.sourceRange.start.line}, columns{' '}
                  {diagnostic.sourceRange.start.column}–
                  {diagnostic.sourceRange.end.column}
                </Text>
              )}
            </Group>
            <Text size="xs" mt={3}>
              {diagnostic.message}
            </Text>
          </Box>
        </Group>
      ))}
    </Stack>
  );
}

function ServiceBadge({ state }: Readonly<{ state: OperationalStatusState }>) {
  switch (state.kind) {
    case 'loading':
      return (
        <Badge variant="light" color="gray" size="sm">
          Connecting
        </Badge>
      );
    case 'error':
      return (
        <Badge variant="light" color="red" size="sm">
          API unavailable
        </Badge>
      );
    case 'ready':
      return (
        <Badge
          variant="light"
          color={servicePhaseColor(state.status.phase)}
          size="sm"
        >
          {state.status.phase}
        </Badge>
      );
  }
}

function selectSource(
  sources: readonly SourceSummary[],
  requestedSourceId: SourceId | null,
): SourceSummary | null {
  return (
    sources.find((source) => source.sourceId === requestedSourceId) ??
    sources[0] ??
    null
  );
}

function deriveSourceRailState(
  status: 'pending' | 'error' | 'success',
  data: SourceList | undefined,
  error: Error | null,
  selectedSource: SourceSummary | null,
): SourceRailState {
  if (status === 'pending') {
    return { kind: 'loading' };
  }
  if (status === 'error' || data === undefined) {
    return { kind: 'error', message: readableError(error) };
  }
  if (data.sources.length === 0 || selectedSource === null) {
    return { kind: 'empty' };
  }
  return {
    kind: 'ready',
    list: data,
    selectedSourceId: selectedSource.sourceId,
  };
}

function deriveSchemaRailState(
  selectedSource: SourceSummary | null,
  status: 'pending' | 'error' | 'success',
  data: SourceDetail | undefined,
  error: Error | null,
): SchemaRailState {
  if (selectedSource === null) {
    return { kind: 'none' };
  }
  if (status === 'pending') {
    return { kind: 'loading', sourceName: selectedSource.name };
  }
  if (status === 'error' || data === undefined) {
    return {
      kind: 'error',
      sourceName: selectedSource.name,
      message: readableError(error),
    };
  }
  return { kind: 'ready', source: data };
}

function deriveOperationalStatusState(
  status: 'pending' | 'error' | 'success',
  data: OperationalStatus | undefined,
  error: Error | null,
): OperationalStatusState {
  if (status === 'pending') {
    return { kind: 'loading' };
  }
  if (status === 'error' || data === undefined) {
    return { kind: 'error', message: readableError(error) };
  }
  return { kind: 'ready', status: data };
}

function deriveSourceOperationalState<Value>(
  selectedSource: SourceSummary | null,
  status: 'pending' | 'error' | 'success',
  data: Value | undefined,
  error: Error | null,
): SourceOperationalState<Value> {
  if (selectedSource === null) {
    return { kind: 'no-source' };
  }
  if (status === 'pending') {
    return { kind: 'loading' };
  }
  if (status === 'error' || data === undefined) {
    return { kind: 'error', message: readableError(error) };
  }
  return { kind: 'ready', value: data };
}

function deriveResultState(
  status: 'idle' | 'pending' | 'error' | 'success',
  data: QueryExecution | undefined,
  error: Error | null,
  disposition: RunDisposition,
): QueryResultState {
  if (status === 'pending') {
    return {
      kind: disposition === 'cancellation-requested' ? 'cancelling' : 'running',
    };
  }
  if (status === 'success' && data !== undefined) {
    return { kind: 'success', execution: data };
  }
  if (status === 'error' && error !== null) {
    return isCancellation(error) || disposition === 'cancelled'
      ? { kind: 'cancelled' }
      : { kind: 'error', error };
  }
  return { kind: 'idle' };
}

function currentDiagnostics(
  currentQuery: string,
  runningQuery: RunningQuery | undefined,
  successDiagnostics: readonly QueryDiagnostic[],
  error: Error | null,
): readonly QueryDiagnostic[] {
  if (runningQuery?.request.query !== currentQuery) {
    return [];
  }
  if (error instanceof ApiClientError && error.failure.kind === 'http') {
    return error.failure.diagnostics;
  }
  return successDiagnostics;
}

function isCancellation(error: Error): boolean {
  return error instanceof ApiClientError && error.failure.kind === 'aborted';
}

function isWorkspaceMode(value: string): value is WorkspaceMode {
  return value === 'query' || value === 'operations';
}

function servicePhaseColor(phase: OperationalStatus['phase']): string {
  switch (phase) {
    case 'READY':
      return 'teal';
    case 'DEGRADED':
      return 'orange';
    case 'STARTING':
      return 'gray';
    case 'DRAINING':
      return 'orange';
  }
}

function readableError(error: Error | null): string {
  if (error === null) {
    return 'The API did not return usable data.';
  }
  if (!(error instanceof ApiClientError)) {
    return error.message;
  }
  switch (error.failure.kind) {
    case 'aborted':
      return 'The request was cancelled.';
    case 'network':
      return 'The Elucid server could not be reached.';
    case 'http':
      return `${error.failure.code}: ${error.failure.message}`;
    case 'invalid-response':
      return 'The server returned data that does not match the UI contract.';
  }
}
