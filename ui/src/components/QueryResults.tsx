import type { ReactNode } from 'react';
import {
  Badge,
  Box,
  Center,
  Code,
  Group,
  Loader,
  Paper,
  Stack,
  Table,
  Text,
} from '@mantine/core';

import { ApiClientError } from '../api/client';
import type {
  JsonCell,
  LogicalType,
  QueryColumn,
  QueryExecution,
} from '../api/contracts';
import classes from '../App.module.css';

export type QueryResultState =
  | { readonly kind: 'idle' }
  | { readonly kind: 'running' }
  | { readonly kind: 'cancelling' }
  | { readonly kind: 'cancelled' }
  | { readonly kind: 'error'; readonly error: Error }
  | { readonly kind: 'success'; readonly execution: QueryExecution };

export function QueryResults({ state }: Readonly<{ state: QueryResultState }>) {
  return (
    <Paper withBorder radius="md" className={classes.resultsPanel}>
      <Group
        justify="space-between"
        px="md"
        py="sm"
        className={classes.panelHeader}
      >
        <Group gap="xs">
          <Text size="sm" fw={650}>
            Results
          </Text>
          <ResultBadge state={state} />
        </Group>
        <Text size="xs" c="dimmed">
          {resultCount(state)}
        </Text>
      </Group>
      <ResultBody state={state} />
      {state.kind === 'success' ? (
        <ResultStatistics execution={state.execution} />
      ) : null}
    </Paper>
  );
}

function ResultBadge({ state }: Readonly<{ state: QueryResultState }>) {
  switch (state.kind) {
    case 'idle':
      return (
        <Badge size="xs" variant="outline" color="gray">
          Idle
        </Badge>
      );
    case 'running':
      return (
        <Badge size="xs" variant="light" color="cyan">
          Running
        </Badge>
      );
    case 'cancelling':
      return (
        <Badge size="xs" variant="light" color="yellow">
          Cancelling
        </Badge>
      );
    case 'cancelled':
      return (
        <Badge size="xs" variant="outline" color="gray">
          Cancelled
        </Badge>
      );
    case 'error':
      return (
        <Badge size="xs" variant="light" color="red">
          Error
        </Badge>
      );
    case 'success':
      return (
        <Badge
          size="xs"
          variant="light"
          color={state.execution.completion === 'COMPLETE' ? 'teal' : 'yellow'}
        >
          {state.execution.completion === 'COMPLETE' ? 'Complete' : 'Truncated'}
        </Badge>
      );
  }
}

function ResultBody({ state }: Readonly<{ state: QueryResultState }>) {
  switch (state.kind) {
    case 'idle':
      return (
        <EmptyResult
          title="No query result"
          message="Run the query to read an exact segment snapshot."
        />
      );
    case 'running':
      return <PendingResult message="Planning and scanning exact objects…" />;
    case 'cancelling':
      return <PendingResult message="Waiting for query cancellation…" />;
    case 'cancelled':
      return (
        <EmptyResult
          title="Query cancelled"
          message="The browser disconnected the in-process execution."
        />
      );
    case 'error':
      return <QueryFailure error={state.error} />;
    case 'success':
      if (state.execution.rows.length === 0) {
        return (
          <EmptyResult
            title="No matching rows"
            message="The query completed successfully for this UTC range."
          />
        );
      }
      return <ResultTable execution={state.execution} />;
  }
}

function PendingResult({ message }: Readonly<{ message: string }>) {
  return (
    <Center className={classes.resultState}>
      <Stack gap="sm" align="center">
        <Loader size="sm" color="cyan" />
        <Text size="sm" c="dimmed">
          {message}
        </Text>
      </Stack>
    </Center>
  );
}

function EmptyResult({
  title,
  message,
}: Readonly<{ title: string; message: string }>) {
  return (
    <Center className={classes.resultState}>
      <Stack gap={4} align="center">
        <Text size="sm" fw={650}>
          {title}
        </Text>
        <Text size="xs" c="dimmed" ta="center">
          {message}
        </Text>
      </Stack>
    </Center>
  );
}

function QueryFailure({ error }: Readonly<{ error: Error }>) {
  const details = describeFailure(error);
  return (
    <Center className={classes.resultState}>
      <Stack gap="xs" maw={520} align="center">
        <Code color="red">{details.code}</Code>
        <Text size="sm" fw={650} ta="center">
          Query failed
        </Text>
        <Text size="xs" c="dimmed" ta="center">
          {details.message}
        </Text>
        {details.requestId === null ? null : (
          <Text size="xs" c="dimmed" className={classes.identity}>
            Request {details.requestId}
          </Text>
        )}
      </Stack>
    </Center>
  );
}

function ResultTable({ execution }: Readonly<{ execution: QueryExecution }>) {
  return (
    <Table.ScrollContainer
      minWidth={Math.max(620, execution.columns.length * 180)}
      className={classes.tableScroll}
    >
      <Table
        striped
        highlightOnHover
        withColumnBorders
        verticalSpacing="xs"
        horizontalSpacing="sm"
      >
        <Table.Thead>
          <Table.Tr>
            {execution.columns.map((column, index) => (
              <Table.Th key={`${column.name}-${String(index)}`}>
                <ColumnHeader column={column} />
              </Table.Th>
            ))}
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {execution.rows.map((row, rowIndex) => (
            <Table.Tr key={`${execution.queryId}-${String(rowIndex)}`}>
              {row.map((cell, columnIndex) => {
                const column = execution.columns[columnIndex];
                if (column === undefined) {
                  return null;
                }
                return (
                  <Table.Td
                    key={`${column.name}-${String(columnIndex)}`}
                    className={cellClass(column.logicalType)}
                  >
                    {formatCell(cell, column.logicalType)}
                  </Table.Td>
                );
              })}
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Table.ScrollContainer>
  );
}

function ColumnHeader({ column }: Readonly<{ column: QueryColumn }>) {
  return (
    <Group gap={6} wrap="nowrap">
      <Text size="xs" fw={650}>
        {column.name}
      </Text>
      <Code fz={9}>
        {column.logicalType}
        {column.nullability === 'NULLABLE' ? '?' : ''}
      </Code>
    </Group>
  );
}

function ResultStatistics({
  execution,
}: Readonly<{ execution: QueryExecution }>) {
  return (
    <Box className={classes.resultsFooter}>
      {execution.completion === 'TRUNCATED' ? (
        <Text size="xs" c="yellow.4" px="md" pt="sm">
          Result stopped at the configured{' '}
          {execution.truncationReason === 'OUTPUT_ROWS'
            ? 'row limit'
            : 'byte limit'}
          .
        </Text>
      ) : null}
      <Group justify="space-between" px="md" py="sm" wrap="wrap">
        <Group gap="lg">
          <Statistic
            label="Segments"
            value={formatInteger(execution.statistics.selectedSegments)}
          />
          <Statistic
            label="Selected bytes"
            value={formatBytes(execution.statistics.selectedParquetBytes)}
          />
          <Statistic
            label="Output bytes"
            value={formatBytes(execution.statistics.outputBytes)}
          />
          <Statistic
            label="Elapsed"
            value={`${formatInteger(execution.statistics.elapsedMilliseconds)} ms`}
          />
        </Group>
        <Text
          size="xs"
          c="dimmed"
          title={execution.queryId}
          className={classes.identity}
        >
          Exact snapshot · schema v{execution.activeSchemaVersion} · query{' '}
          {execution.queryId.slice(0, 8)}
        </Text>
      </Group>
    </Box>
  );
}

function Statistic({
  label,
  value,
}: Readonly<{ label: string; value: string }>) {
  return (
    <Group gap={5} wrap="nowrap">
      <Text size="xs" c="dimmed">
        {label}
      </Text>
      <Text size="xs" fw={650}>
        {value}
      </Text>
    </Group>
  );
}

function describeFailure(error: Error): {
  readonly code: string;
  readonly message: string;
  readonly requestId: string | null;
} {
  if (!(error instanceof ApiClientError)) {
    return {
      code: 'UNEXPECTED_ERROR',
      message: error.message,
      requestId: null,
    };
  }
  switch (error.failure.kind) {
    case 'aborted':
      return {
        code: 'QUERY_CANCELLED',
        message: 'Query was cancelled.',
        requestId: null,
      };
    case 'network':
      return {
        code: 'NETWORK_ERROR',
        message: error.failure.reason,
        requestId: null,
      };
    case 'http':
      return {
        code: error.failure.code,
        message: error.failure.message,
        requestId: error.failure.requestId,
      };
    case 'invalid-response':
      return {
        code: 'INVALID_API_RESPONSE',
        message: error.failure.reason,
        requestId: error.failure.requestId,
      };
  }
}

function resultCount(state: QueryResultState): string {
  return state.kind === 'success'
    ? `${formatInteger(state.execution.statistics.outputRows)} rows`
    : '—';
}

function formatCell(value: JsonCell, logicalType: LogicalType): ReactNode {
  if (value === null) {
    return (
      <Text component="span" size="xs" c="dimmed" fs="italic">
        NULL
      </Text>
    );
  }
  if (logicalType === 'json') {
    return JSON.stringify(value);
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }
  return String(value);
}

function cellClass(logicalType: LogicalType): string {
  switch (logicalType) {
    case 'int32':
    case 'int64':
    case 'uint32':
    case 'uint64':
    case 'float32':
    case 'float64':
      return classes.numericCell;
    case 'datetime':
    case 'eid':
    case 'json':
      return classes.monospaceCell;
    case 'bool':
    case 'utf8':
      return classes.resultCell;
  }
}

function formatInteger(value: number): string {
  return new Intl.NumberFormat('en-US').format(value);
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${formatInteger(bytes)} B`;
  }
  const units = ['KiB', 'MiB', 'GiB', 'TiB'] as const;
  let value = bytes / 1024;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  const unit = units[unitIndex] ?? units[0];
  return `${value.toFixed(value >= 10 ? 1 : 2)} ${unit}`;
}
