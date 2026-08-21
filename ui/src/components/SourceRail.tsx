import { useMemo, useState } from 'react';
import {
  AppShell,
  Badge,
  Box,
  Button,
  Group,
  ScrollArea,
  Skeleton,
  Stack,
  Text,
  TextInput,
  UnstyledButton,
} from '@mantine/core';

import type { SourceId, SourceList } from '../api/contracts';
import classes from '../App.module.css';

export type SourceRailState =
  | { readonly kind: 'loading' }
  | { readonly kind: 'error'; readonly message: string }
  | { readonly kind: 'empty' }
  | {
      readonly kind: 'ready';
      readonly list: SourceList;
      readonly selectedSourceId: SourceId;
    };

interface SourceRailProps {
  readonly state: SourceRailState;
  readonly onSelect: (sourceId: SourceId) => void;
  readonly onRetry: () => void;
}

export function SourceRail({ state, onSelect, onRetry }: SourceRailProps) {
  const [filter, setFilter] = useState('');
  const visibleSources = useMemo(() => {
    if (state.kind !== 'ready') {
      return [];
    }
    const normalizedFilter = filter.trim().toLocaleLowerCase();
    if (normalizedFilter.length === 0) {
      return state.list.sources;
    }
    return state.list.sources.filter(
      (source) =>
        source.name.toLocaleLowerCase().includes(normalizedFilter) ||
        source.displayName.toLocaleLowerCase().includes(normalizedFilter),
    );
  }, [filter, state]);

  return (
    <>
      <AppShell.Section p="md" pb="sm">
        <Group justify="space-between">
          <Text
            size="xs"
            fw={700}
            tt="uppercase"
            c="dimmed"
            className={classes.eyebrow}
          >
            Sources
          </Text>
          <SourceCount state={state} />
        </Group>
        <TextInput
          mt="sm"
          size="xs"
          placeholder="Filter sources"
          value={filter}
          onChange={(event) => {
            setFilter(event.currentTarget.value);
          }}
          disabled={state.kind !== 'ready'}
          aria-label="Filter sources"
        />
      </AppShell.Section>
      <AppShell.Section grow component={ScrollArea} px="sm">
        <Box py="xs">
          <SourceListBody
            state={state}
            visibleSources={visibleSources}
            onSelect={onSelect}
            onRetry={onRetry}
          />
        </Box>
      </AppShell.Section>
      <AppShell.Section p="md" className={classes.railFooter}>
        <Text size="xs" c="dimmed">
          {sourceFooter(state)}
        </Text>
      </AppShell.Section>
    </>
  );
}

function SourceCount({ state }: Readonly<{ state: SourceRailState }>) {
  if (state.kind !== 'ready') {
    return null;
  }
  return (
    <Badge variant="outline" color="gray" size="xs">
      {state.list.completion === 'TRUNCATED'
        ? `${String(state.list.sources.length)} / ${String(state.list.limit)}+`
        : state.list.sources.length}
    </Badge>
  );
}

function SourceListBody({
  state,
  visibleSources,
  onSelect,
  onRetry,
}: Readonly<{
  state: SourceRailState;
  visibleSources: readonly SourceList['sources'][number][];
  onSelect: (sourceId: SourceId) => void;
  onRetry: () => void;
}>) {
  switch (state.kind) {
    case 'loading':
      return (
        <Stack gap="xs">
          <Skeleton height={58} radius="sm" />
          <Skeleton height={58} radius="sm" />
          <Skeleton height={58} radius="sm" />
        </Stack>
      );
    case 'error':
      return (
        <Stack gap="sm" p="sm" align="flex-start">
          <Text size="sm" fw={650}>
            Sources unavailable
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
        <Box p="sm">
          <Text size="sm" fw={650}>
            No sources
          </Text>
          <Text size="xs" c="dimmed" mt={4}>
            Apply a catalog before running queries.
          </Text>
        </Box>
      );
    case 'ready':
      if (visibleSources.length === 0) {
        return (
          <Text size="xs" c="dimmed" p="sm">
            No source matches this filter.
          </Text>
        );
      }
      return (
        <Stack gap={4}>
          {visibleSources.map((source) => {
            const selected = source.sourceId === state.selectedSourceId;
            return (
              <UnstyledButton
                key={source.sourceId}
                className={selected ? classes.sourceActive : classes.sourceIdle}
                p="sm"
                onClick={() => {
                  onSelect(source.sourceId);
                }}
                aria-current={selected ? 'true' : undefined}
              >
                <Group justify="space-between" align="flex-start" wrap="nowrap">
                  <Box className={classes.truncate}>
                    <Text size="sm" fw={selected ? 650 : 600} truncate>
                      {source.displayName}
                    </Text>
                    <Text size="xs" c="dimmed" truncate>
                      {source.name}
                    </Text>
                  </Box>
                  <Badge
                    size="xs"
                    variant={selected ? 'light' : 'outline'}
                    color={selected ? 'cyan' : 'gray'}
                  >
                    v{source.activeSchemaVersion}
                  </Badge>
                </Group>
              </UnstyledButton>
            );
          })}
        </Stack>
      );
  }
}

function sourceFooter(state: SourceRailState): string {
  switch (state.kind) {
    case 'loading':
      return 'Loading the bounded catalog…';
    case 'error':
      return 'Catalog read failed';
    case 'empty':
      return 'Catalog is empty';
    case 'ready':
      return state.list.completion === 'TRUNCATED'
        ? `Showing the first ${String(state.list.limit)} sources`
        : 'Bounded catalog view';
  }
}
