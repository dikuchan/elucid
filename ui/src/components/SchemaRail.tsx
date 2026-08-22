import {
  AppShell,
  Badge,
  Box,
  Button,
  Code,
  Divider,
  Group,
  ScrollArea,
  Skeleton,
  Stack,
  Text,
} from '@mantine/core';

import type { SourceDetail } from '../api/contracts';
import classes from '../App.module.css';

export type SchemaRailState =
  | { readonly kind: 'none' }
  | { readonly kind: 'loading'; readonly sourceName: string }
  | {
      readonly kind: 'error';
      readonly sourceName: string;
      readonly message: string;
    }
  | { readonly kind: 'ready'; readonly source: SourceDetail };

interface SchemaRailProps {
  readonly state: SchemaRailState;
  readonly onRetry: () => void;
}

export function SchemaRail({ state, onRetry }: SchemaRailProps) {
  return (
    <>
      <AppShell.Section p="md">
        <Text
          size="xs"
          fw={700}
          tt="uppercase"
          c="dimmed"
          className={classes.eyebrow}
        >
          Active schema
        </Text>
        <SchemaHeading state={state} />
      </AppShell.Section>
      <Divider />
      <AppShell.Section grow component={ScrollArea}>
        <SchemaBody state={state} onRetry={onRetry} />
      </AppShell.Section>
    </>
  );
}

function SchemaHeading({ state }: Readonly<{ state: SchemaRailState }>) {
  switch (state.kind) {
    case 'none':
      return (
        <Text size="xs" c="dimmed" mt="xs">
          Select a source to inspect its schema.
        </Text>
      );
    case 'loading':
      return (
        <Stack gap={6} mt="xs">
          <Text size="sm" fw={650}>
            {state.sourceName}
          </Text>
          <Skeleton height={16} width="55%" />
        </Stack>
      );
    case 'error':
      return (
        <Text size="sm" fw={650} mt="xs">
          {state.sourceName}
        </Text>
      );
    case 'ready':
      return (
        <Group justify="space-between" mt="xs" wrap="nowrap">
          <Box className={classes.truncate}>
            <Text size="sm" fw={650} truncate>
              {state.source.name}
            </Text>
            <Text size="xs" c="dimmed">
              {state.source.activeSchema.fields.length} fields
            </Text>
          </Box>
          <Badge size="sm" variant="light" color="cyan">
            v{state.source.activeSchema.version} active
          </Badge>
        </Group>
      );
  }
}

function SchemaBody({
  state,
  onRetry,
}: Readonly<{ state: SchemaRailState; onRetry: () => void }>) {
  switch (state.kind) {
    case 'none':
      return null;
    case 'loading':
      return (
        <Stack gap="sm" p="md">
          {Array.from({ length: 6 }, (_, index) => (
            <Skeleton key={index} height={38} />
          ))}
        </Stack>
      );
    case 'error':
      return (
        <Stack gap="sm" p="md" align="flex-start">
          <Text size="sm" fw={650}>
            Schema unavailable
          </Text>
          <Text size="xs" c="dimmed">
            {state.message}
          </Text>
          <Button size="xs" variant="default" onClick={onRetry}>
            Retry
          </Button>
        </Stack>
      );
    case 'ready':
      return <LoadedSchema source={state.source} />;
  }
}

function LoadedSchema({ source }: Readonly<{ source: SourceDetail }>) {
  const history = [...source.schemaVersions].sort(
    (left, right) => right.version - left.version,
  );
  return (
    <>
      <Stack gap={0}>
        {source.activeSchema.fields.map((field) => (
          <Box key={field.fieldId} px="md" py="sm" className={classes.fieldRow}>
            <Group justify="space-between" wrap="nowrap" align="flex-start">
              <Box className={classes.truncate}>
                <Text size="xs" fw={600} truncate className={classes.fieldName}>
                  {field.name}
                </Text>
                <Text size="xs" c="dimmed">
                  {field.nullability} · {field.role}
                </Text>
              </Box>
              <Code fz={10}>{field.logicalType}</Code>
            </Group>
            {field.description === null ? null : (
              <Text size="xs" c="dimmed" mt={5}>
                {field.description}
              </Text>
            )}
            {field.historicalRemainderPointer === null ? null : (
              <Text size="xs" c="cyan.8" mt={5}>
                Historical: {field.historicalRemainderPointer}
              </Text>
            )}
          </Box>
        ))}
      </Stack>
      <Divider />
      <Box p="md">
        <Text
          size="xs"
          fw={700}
          tt="uppercase"
          c="dimmed"
          className={classes.eyebrow}
        >
          Schema history
        </Text>
        <Stack gap="xs" mt="sm">
          {history.map((schema) => {
            const active = schema.schemaId === source.activeSchema.schemaId;
            return (
              <Box key={schema.schemaId} className={classes.schemaVersion}>
                <Group justify="space-between" wrap="nowrap">
                  <Box className={classes.truncate}>
                    <Text size="xs" fw={650}>
                      Version {schema.version}
                    </Text>
                    <Text
                      size="xs"
                      c="dimmed"
                      truncate
                      title={schema.schemaId}
                      className={classes.identity}
                    >
                      {schema.schemaId}
                    </Text>
                  </Box>
                  <Badge
                    size="xs"
                    variant={active ? 'light' : 'outline'}
                    color={active ? 'cyan' : 'gray'}
                  >
                    {active ? 'Active' : 'Historical'}
                  </Badge>
                </Group>
              </Box>
            );
          })}
        </Stack>
      </Box>
    </>
  );
}
