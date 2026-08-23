import {
  AppShell,
  Badge,
  Box,
  Button,
  Divider,
  Group,
  ScrollArea,
  Skeleton,
  Stack,
  Text,
  UnstyledButton,
} from '@mantine/core';

import type {
  QueryExecutionList,
  QueryExecutionSummary,
} from '../api/contracts';
import classes from '../App.module.css';

export type QueryHistoryRailState =
  | { readonly kind: 'loading' }
  | { readonly kind: 'error'; readonly message: string }
  | { readonly kind: 'empty' }
  | { readonly kind: 'ready'; readonly list: QueryExecutionList };

interface QueryHistoryRailProps {
  readonly state: QueryHistoryRailState;
  readonly disabled: boolean;
  readonly onSelect: (execution: QueryExecutionSummary) => void;
  readonly onRetry: () => void;
}

export function QueryHistoryRail({
  state,
  disabled,
  onSelect,
  onRetry,
}: QueryHistoryRailProps) {
  return (
    <>
      <AppShell.Section p="md">
        <Group justify="space-between">
          <Text
            size="xs"
            fw={700}
            tt="uppercase"
            c="dimmed"
            className={classes.eyebrow}
          >
            Query history
          </Text>
          <HistoryCount state={state} />
        </Group>
      </AppShell.Section>
      <Divider />
      <AppShell.Section grow component={ScrollArea}>
        <HistoryBody
          state={state}
          disabled={disabled}
          onSelect={onSelect}
          onRetry={onRetry}
        />
      </AppShell.Section>
    </>
  );
}

function HistoryCount({ state }: Readonly<{ state: QueryHistoryRailState }>) {
  if (state.kind !== 'ready') {
    return null;
  }
  return (
    <Badge variant="outline" color="gray" size="xs">
      {state.list.completion === 'TRUNCATED'
        ? `${String(state.list.queryExecutions.length)}+`
        : state.list.queryExecutions.length}
    </Badge>
  );
}

function HistoryBody({
  state,
  disabled,
  onSelect,
  onRetry,
}: Readonly<QueryHistoryRailProps>) {
  switch (state.kind) {
    case 'loading':
      return (
        <Stack gap="xs" p="sm">
          {Array.from({ length: 5 }, (_, index) => (
            <Skeleton key={index} height={78} radius="sm" />
          ))}
        </Stack>
      );
    case 'error':
      return (
        <Stack gap="sm" p="md" align="flex-start">
          <Text size="sm" fw={650}>
            History unavailable
          </Text>
          <Text size="xs" c="dimmed">
            {state.message}
          </Text>
          <Button size="xs" variant="default" onClick={onRetry}>
            Retry
          </Button>
        </Stack>
      );
    case 'empty':
      return (
        <Box p="md">
          <Text size="sm" fw={650}>
            No queries yet
          </Text>
        </Box>
      );
    case 'ready':
      return (
        <Stack gap={4} p="sm">
          {state.list.queryExecutions.map((execution) => (
            <UnstyledButton
              key={execution.queryId}
              className={classes.historyItem}
              p="sm"
              disabled={disabled}
              onClick={() => {
                onSelect(execution);
              }}
              aria-label={`Restore query submitted ${execution.submittedAt}`}
            >
              <Text
                size="xs"
                fw={600}
                lineClamp={2}
                className={classes.historyQuery}
                title={execution.query}
              >
                {execution.query.length === 0
                  ? '(empty query)'
                  : execution.query}
              </Text>
              <Group justify="space-between" gap="xs" mt={7} wrap="nowrap">
                <Text size="xs" c="dimmed">
                  {formatSubmittedAt(execution.submittedAt)}
                </Text>
                <Text size="xs" c="dimmed">
                  {execution.outputRows} rows
                </Text>
              </Group>
            </UnstyledButton>
          ))}
        </Stack>
      );
  }
}

function formatSubmittedAt(timestamp: string): string {
  return `${timestamp.slice(0, 10)} ${timestamp.slice(11, 19)} UTC`;
}
