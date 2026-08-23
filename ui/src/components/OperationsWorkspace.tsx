import type { ReactNode } from 'react';
import {
  Alert,
  Badge,
  Box,
  Button,
  Center,
  Group,
  Paper,
  Progress,
  SimpleGrid,
  Skeleton,
  Stack,
  Table,
  Text,
} from '@mantine/core';

import type {
  ComponentStatus,
  DeadLetterList,
  OperationalStatus,
  SegmentList,
  SegmentSummary,
  SourceSummary,
} from '../api/contracts';
import classes from '../App.module.css';

export type OperationalStatusState =
  | { readonly kind: 'loading' }
  | { readonly kind: 'error'; readonly message: string }
  | { readonly kind: 'ready'; readonly status: OperationalStatus };

export type SourceOperationalState<Value> =
  | { readonly kind: 'no-source' }
  | { readonly kind: 'loading' }
  | { readonly kind: 'error'; readonly message: string }
  | { readonly kind: 'ready'; readonly value: Value };

interface OperationsWorkspaceProps {
  readonly source: SourceSummary | null;
  readonly statusState: OperationalStatusState;
  readonly segmentState: SourceOperationalState<SegmentList>;
  readonly deadLetterState: SourceOperationalState<DeadLetterList>;
  readonly onRefresh: () => void;
}

export function OperationsWorkspace({
  source,
  statusState,
  segmentState,
  deadLetterState,
  onRefresh,
}: OperationsWorkspaceProps) {
  return (
    <Box className={classes.workspace}>
      <Paper withBorder radius="md" p="md" className={classes.contextPanel}>
        <Group justify="space-between" align="center" wrap="wrap" gap="md">
          <Box>
            <Text
              size="xs"
              fw={700}
              tt="uppercase"
              c="dimmed"
              className={classes.eyebrow}
            >
              Operations
            </Text>
            <Group gap="xs" mt={4}>
              <Text fw={650}>
                {source?.displayName ?? 'No source selected'}
              </Text>
            </Group>
          </Box>
          <Button size="xs" variant="default" onClick={onRefresh}>
            Refresh
          </Button>
        </Group>
      </Paper>

      <OperationalOverview state={statusState} />
      <SegmentPanel state={segmentState} />
      <DeadLetterPanel state={deadLetterState} />
    </Box>
  );
}

function OperationalOverview({
  state,
}: Readonly<{ state: OperationalStatusState }>) {
  switch (state.kind) {
    case 'loading':
      return (
        <SimpleGrid cols={{ base: 1, sm: 2, xl: 4 }} spacing="sm">
          {Array.from({ length: 4 }, (_, index) => (
            <Skeleton key={index} height={132} radius="md" />
          ))}
        </SimpleGrid>
      );
    case 'error':
      return (
        <Paper withBorder radius="md" p="md" className={classes.statusPanel}>
          <Alert color="red" variant="light" title="Status unavailable">
            {state.message}
          </Alert>
        </Paper>
      );
    case 'ready':
      return <LoadedOperationalOverview status={state.status} />;
  }
}

function LoadedOperationalOverview({
  status,
}: Readonly<{ status: OperationalStatus }>) {
  const spoolPercent = Math.min(
    100,
    (status.spool.usedBytes / status.spool.capacityBytes) * 100,
  );
  return (
    <>
      <SimpleGrid cols={{ base: 1, sm: 2, xl: 4 }} spacing="sm">
        <OperationalCard
          label="Admission"
          value={status.admission}
          badge={status.phase}
          badgeColor={status.admission === 'OPEN' ? 'teal' : 'red'}
          detail={`HTTP batches · up to ${formatBytes(status.limits.maximumHttpBatchBytes)}`}
        />
        <Paper withBorder radius="md" p="md" className={classes.operationCard}>
          <Text size="xs" c="dimmed" fw={650}>
            Durable spool
          </Text>
          <Group justify="space-between" align="baseline" mt={6}>
            <Text size="xl" fw={700}>
              {formatBytes(status.spool.usedBytes)}
            </Text>
            <Text size="xs" c="dimmed">
              of {formatBytes(status.spool.capacityBytes)}
            </Text>
          </Group>
          <Progress
            value={spoolPercent}
            color={spoolColor(spoolPercent)}
            mt="sm"
          />
          <Text size="xs" c="dimmed" mt="xs">
            {formatInteger(status.spool.pendingBatches)} queued · oldest{' '}
            {formatAge(status.spool.oldestQueuedAgeSeconds)}
          </Text>
        </Paper>
        <OperationalCard
          label="Publication"
          value={`${formatInteger(status.publication.pendingBatches)} batches`}
          badge={status.publication.status}
          badgeColor={componentColor(status.publication.status)}
          detail={`${formatInteger(status.publication.preparedSegments)} prepared · ${formatInteger(status.publication.plannedObjects)} planned · ${formatInteger(status.publication.uploadedObjects)} uploaded`}
        />
        <OperationalCard
          label="Maintenance"
          value={status.maintenance.ownership}
          badge={status.components.maintenance}
          badgeColor={componentColor(status.components.maintenance)}
          detail={`${String(status.maintenance.recentCompactions.length)} recent compactions`}
        />
      </SimpleGrid>

      <Paper withBorder radius="md" className={classes.statusPanel}>
        <PanelHeader title="Component health" badge={status.phase} />
        <SimpleGrid
          cols={{ base: 2, sm: 3, lg: 6 }}
          spacing={0}
          className={classes.componentGrid}
        >
          <ComponentHealth
            label="PostgreSQL"
            status={status.components.postgresql}
          />
          <ComponentHealth
            label="Object store"
            status={status.components.objectStore}
          />
          <ComponentHealth label="Spool" status={status.components.spool} />
          <ComponentHealth
            label="Ingestion"
            status={status.components.ingestionWorker}
          />
          <ComponentHealth label="Query" status={status.components.query} />
          <ComponentHealth
            label="Maintenance"
            status={status.components.maintenance}
          />
        </SimpleGrid>
        <SimpleGrid
          cols={{ base: 2, sm: 4, lg: 6 }}
          spacing={0}
          className={classes.limitGrid}
        >
          <Limit
            label="Batch records"
            value={formatInteger(status.limits.maximumHttpBatchRecords)}
          />
          <Limit
            label="Event days"
            value={formatInteger(status.limits.maximumBatchEventDays)}
          />
          <Limit
            label="Ingest requests"
            value={formatInteger(
              status.limits.maximumConcurrentIngestionRequests,
            )}
          />
          <Limit
            label="Queries"
            value={formatInteger(status.limits.maximumConcurrentQueries)}
          />
          <Limit
            label="Query scan"
            value={formatBytes(status.limits.maximumQueryScanBytes)}
          />
          <Limit
            label="Scratch"
            value={formatBytes(status.limits.scratchCapacityBytes)}
          />
        </SimpleGrid>
      </Paper>
    </>
  );
}

function OperationalCard({
  label,
  value,
  badge,
  badgeColor,
  detail,
}: Readonly<{
  label: string;
  value: string;
  badge: string;
  badgeColor: string;
  detail: string;
}>) {
  return (
    <Paper withBorder radius="md" p="md" className={classes.operationCard}>
      <Group justify="space-between" wrap="nowrap">
        <Text size="xs" c="dimmed" fw={650}>
          {label}
        </Text>
        <Badge size="xs" variant="light" color={badgeColor}>
          {badge}
        </Badge>
      </Group>
      <Text size="xl" fw={700} mt={6}>
        {value}
      </Text>
      <Text size="xs" c="dimmed" mt="sm">
        {detail}
      </Text>
    </Paper>
  );
}

function ComponentHealth({
  label,
  status,
}: Readonly<{ label: string; status: ComponentStatus }>) {
  return (
    <Box p="md" className={classes.componentHealth}>
      <Text size="xs" c="dimmed">
        {label}
      </Text>
      <Group gap={6} mt={4} wrap="nowrap">
        <Box
          className={classes.healthDot}
          data-status={status.toLocaleLowerCase()}
          aria-hidden="true"
        />
        <Text size="xs" fw={700}>
          {status}
        </Text>
      </Group>
    </Box>
  );
}

function Limit({ label, value }: Readonly<{ label: string; value: string }>) {
  return (
    <Box px="md" py="sm" className={classes.limit}>
      <Text size="xs" c="dimmed">
        {label}
      </Text>
      <Text size="xs" fw={650} mt={2}>
        {value}
      </Text>
    </Box>
  );
}

function SegmentPanel({
  state,
}: Readonly<{ state: SourceOperationalState<SegmentList> }>) {
  return (
    <Paper withBorder radius="md" className={classes.operationalListPanel}>
      <PanelHeader
        title="Segments"
        badge={
          state.kind === 'ready'
            ? listCount(state.value.segments.length, state.value)
            : '—'
        }
      />
      <OperationalListState
        state={state}
        emptyMessage="No segments for this source."
        itemCount={(list) => list.segments.length}
      >
        {(list) => <SegmentTable list={list} />}
      </OperationalListState>
    </Paper>
  );
}

function SegmentTable({ list }: Readonly<{ list: SegmentList }>) {
  return (
    <Table.ScrollContainer minWidth={980}>
      <Table
        striped
        highlightOnHover
        verticalSpacing="xs"
        horizontalSpacing="md"
      >
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Segment</Table.Th>
            <Table.Th>Lifecycle</Table.Th>
            <Table.Th>UTC day</Table.Th>
            <Table.Th>Schema</Table.Th>
            <Table.Th ta="right">Rows</Table.Th>
            <Table.Th ta="right">Parquet</Table.Th>
            <Table.Th>Event range</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {list.segments.map((segment) => (
            <Table.Tr key={segment.segmentId}>
              <Table.Td>
                <Identity value={segment.segmentId} />
              </Table.Td>
              <Table.Td>
                <Group gap={6} wrap="nowrap">
                  <Badge
                    size="xs"
                    variant="light"
                    color={segmentColor(segment.state)}
                  >
                    {segment.state}
                  </Badge>
                  <Text size="xs" c="dimmed">
                    {segment.origin}
                  </Text>
                </Group>
              </Table.Td>
              <Table.Td>
                <Text size="xs" className={classes.identity}>
                  {segment.eventDay}
                </Text>
              </Table.Td>
              <Table.Td>
                <Text size="xs">v{segment.schemaVersion}</Text>
              </Table.Td>
              <Table.Td ta="right">
                <Text size="xs" className={classes.identity}>
                  {formatInteger(segment.rowCount)}
                </Text>
              </Table.Td>
              <Table.Td ta="right">
                <Text size="xs" className={classes.identity}>
                  {formatBytes(segment.parquetBytes)}
                </Text>
              </Table.Td>
              <Table.Td>
                <EventRange segment={segment} />
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Table.ScrollContainer>
  );
}

function EventRange({ segment }: Readonly<{ segment: SegmentSummary }>) {
  return (
    <Stack gap={1}>
      <Text size="xs" className={classes.identity}>
        {formatUtcTimestamp(segment.minimumEventTime)}
      </Text>
      <Text size="xs" c="dimmed" className={classes.identity}>
        {formatUtcTimestamp(segment.maximumEventTime)}
      </Text>
    </Stack>
  );
}

function DeadLetterPanel({
  state,
}: Readonly<{ state: SourceOperationalState<DeadLetterList> }>) {
  return (
    <Paper withBorder radius="md" className={classes.operationalListPanel}>
      <PanelHeader
        title="Dead-letter objects"
        badge={
          state.kind === 'ready'
            ? listCount(state.value.deadLetters.length, state.value)
            : '—'
        }
      />
      <OperationalListState
        state={state}
        emptyMessage="No published dead letters for this source."
        itemCount={(list) => list.deadLetters.length}
      >
        {(list) => <DeadLetterTable list={list} />}
      </OperationalListState>
    </Paper>
  );
}

function DeadLetterTable({ list }: Readonly<{ list: DeadLetterList }>) {
  return (
    <Table.ScrollContainer minWidth={780}>
      <Table
        striped
        highlightOnHover
        verticalSpacing="xs"
        horizontalSpacing="md"
      >
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Object</Table.Th>
            <Table.Th>Batch</Table.Th>
            <Table.Th>Input</Table.Th>
            <Table.Th ta="right">Bytes</Table.Th>
            <Table.Th>Published</Table.Th>
            <Table.Th>Retained until</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {list.deadLetters.map((summary) => (
            <Table.Tr key={summary.objectId}>
              <Table.Td>
                <Identity value={summary.objectId} />
              </Table.Td>
              <Table.Td>
                <Identity value={summary.batchId} />
              </Table.Td>
              <Table.Td>
                <Identity value={summary.inputId} />
              </Table.Td>
              <Table.Td ta="right">
                <Text size="xs" className={classes.identity}>
                  {formatBytes(summary.byteSize)}
                </Text>
              </Table.Td>
              <Table.Td>
                <Text size="xs" className={classes.identity}>
                  {formatUtcTimestamp(summary.publishedAt)}
                </Text>
              </Table.Td>
              <Table.Td>
                <Text size="xs" className={classes.identity}>
                  {formatUtcTimestamp(summary.retentionDeadline)}
                </Text>
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Table.ScrollContainer>
  );
}

function OperationalListState<
  Value extends { readonly completion: 'COMPLETE' | 'TRUNCATED' },
>({
  state,
  emptyMessage,
  itemCount,
  children,
}: Readonly<{
  state: SourceOperationalState<Value>;
  emptyMessage: string;
  itemCount: (value: Value) => number;
  children: (value: Value) => ReactNode;
}>) {
  switch (state.kind) {
    case 'no-source':
      return (
        <EmptyOperationalState message="Select a source to view its segments and dead letters." />
      );
    case 'loading':
      return (
        <Stack gap="xs" p="md">
          {Array.from({ length: 3 }, (_, index) => (
            <Skeleton key={index} height={38} />
          ))}
        </Stack>
      );
    case 'error':
      return (
        <Center className={classes.operationalState}>
          <Stack gap={4} align="center">
            <Text size="sm" fw={650}>
              Data unavailable
            </Text>
            <Text size="xs" c="dimmed" ta="center">
              {state.message}
            </Text>
          </Stack>
        </Center>
      );
    case 'ready': {
      return itemCount(state.value) === 0 ? (
        <EmptyOperationalState message={emptyMessage} />
      ) : (
        children(state.value)
      );
    }
  }
}

function EmptyOperationalState({ message }: Readonly<{ message: string }>) {
  return (
    <Center className={classes.operationalState}>
      <Text size="xs" c="dimmed" ta="center">
        {message}
      </Text>
    </Center>
  );
}

function PanelHeader({
  title,
  badge,
}: Readonly<{ title: string; badge: string }>) {
  return (
    <Group
      justify="space-between"
      px="md"
      py="sm"
      className={classes.panelHeader}
    >
      <Text size="sm" fw={650}>
        {title}
      </Text>
      <Badge size="xs" variant="outline" color="gray">
        {badge}
      </Badge>
    </Group>
  );
}

function Identity({ value }: Readonly<{ value: string }>) {
  return (
    <Text size="xs" title={value} className={classes.identity}>
      {value.slice(0, 8)}
    </Text>
  );
}

function listCount(
  itemCount: number,
  list: Readonly<{ completion: 'COMPLETE' | 'TRUNCATED'; limit: number }>,
): string {
  return list.completion === 'TRUNCATED'
    ? `${String(itemCount)} / ${String(list.limit)}+`
    : formatInteger(itemCount);
}

function componentColor(status: ComponentStatus): string {
  switch (status) {
    case 'UP':
      return 'teal';
    case 'DEGRADED':
      return 'orange';
    case 'DOWN':
      return 'red';
  }
}

function segmentColor(state: SegmentSummary['state']): string {
  switch (state) {
    case 'ACTIVE':
      return 'teal';
    case 'PREPARED':
      return 'cyan';
    case 'SUPERSEDED':
      return 'blue';
    case 'EXPIRED':
      return 'orange';
    case 'ABANDONED':
      return 'red';
  }
}

function spoolColor(percent: number): string {
  if (percent >= 90) {
    return 'red';
  }
  if (percent >= 70) {
    return 'orange';
  }
  return 'cyan';
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

function formatAge(seconds: number | null): string {
  if (seconds === null) {
    return 'none';
  }
  if (seconds < 60) {
    return `${formatInteger(seconds)}s`;
  }
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${formatInteger(minutes)}m`;
  }
  const hours = Math.floor(minutes / 60);
  return `${formatInteger(hours)}h`;
}

function formatUtcTimestamp(value: string): string {
  return `${value.slice(0, 10)} ${value.slice(11, 19)} UTC`;
}
