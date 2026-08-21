import {
  AppShell,
  Badge,
  Box,
  Button,
  Code,
  Divider,
  Group,
  Paper,
  ScrollArea,
  Stack,
  Table,
  Text,
  TextInput,
  Textarea,
  Title,
} from '@mantine/core';

import classes from './App.module.css';

const queryText = `source demo_logs
| filter status >= 400
| project @event_time, service, status, message
| sort by -@event_time
| take 100`;

const resultRows = [
  ['2026-08-20T14:42:18.302Z', 'gateway', '503', 'upstream request timed out'],
  ['2026-08-20T14:40:07.114Z', 'identity', '401', 'token signature rejected'],
  ['2026-08-20T14:37:52.901Z', 'gateway', '429', 'request rate exceeded'],
] as const;

const fields = [
  ['@event_time', 'datetime', 'NON_NULL'],
  ['@ingestion_time', 'datetime', 'NON_NULL'],
  ['@event_id', 'eid', 'NON_NULL'],
  ['message', 'utf8', 'NON_NULL'],
  ['service', 'utf8', 'NULLABLE'],
  ['status', 'int32', 'NULLABLE'],
  ['region', 'utf8', 'NULLABLE'],
  ['@rest', 'json', 'NON_NULL'],
] as const;

export function App() {
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
                Event query workspace
              </Text>
            </Box>
          </Group>
          <Group gap="xs" wrap="nowrap">
            <Badge variant="light" color="gray" size="sm">
              Static layout
            </Badge>
            <Badge variant="dot" color="cyan" size="sm">
              local
            </Badge>
          </Group>
        </Group>
      </AppShell.Header>

      <AppShell.Navbar className={classes.rail}>
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
            <Badge variant="outline" color="gray" size="xs">
              2 / 100
            </Badge>
          </Group>
          <TextInput
            mt="sm"
            size="xs"
            placeholder="Filter sources"
            disabled
            aria-label="Filter sources"
          />
        </AppShell.Section>
        <AppShell.Section grow component={ScrollArea} px="sm">
          <Stack gap={4} py="xs">
            <Paper className={classes.sourceActive} p="sm" radius="sm">
              <Group justify="space-between" align="flex-start" wrap="nowrap">
                <Box className={classes.truncate}>
                  <Text size="sm" fw={650} truncate>
                    Demo logs
                  </Text>
                  <Text size="xs" c="dimmed" truncate>
                    demo_logs
                  </Text>
                </Box>
                <Badge size="xs" variant="light" color="cyan">
                  v2
                </Badge>
              </Group>
            </Paper>
            <Paper className={classes.sourceIdle} p="sm" radius="sm">
              <Group justify="space-between" align="flex-start" wrap="nowrap">
                <Box className={classes.truncate}>
                  <Text size="sm" fw={600} truncate>
                    Authentication events
                  </Text>
                  <Text size="xs" c="dimmed" truncate>
                    auth_events
                  </Text>
                </Box>
                <Badge size="xs" variant="outline" color="gray">
                  v1
                </Badge>
              </Group>
            </Paper>
          </Stack>
        </AppShell.Section>
        <AppShell.Section p="md" className={classes.railFooter}>
          <Text size="xs" c="dimmed">
            Preview data only
          </Text>
        </AppShell.Section>
      </AppShell.Navbar>

      <AppShell.Main className={classes.main}>
        <Box className={classes.workspace}>
          <Paper withBorder radius="md" p="md" className={classes.contextPanel}>
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
                  Querying
                </Text>
                <Group gap="xs" mt={4}>
                  <Text fw={650}>demo_logs</Text>
                  <Badge size="xs" variant="light" color="cyan">
                    active schema v2
                  </Badge>
                </Group>
              </Box>
              <Group gap="xs" align="flex-end" wrap="wrap">
                <TextInput
                  label="From · UTC"
                  value="2026-08-20 00:00:00"
                  readOnly
                  size="xs"
                  className={classes.timeInput}
                />
                <TextInput
                  label="To · UTC"
                  value="2026-08-21 00:00:00"
                  readOnly
                  size="xs"
                  className={classes.timeInput}
                />
                <TextInput
                  label="Output rows"
                  value="1000"
                  readOnly
                  size="xs"
                  className={classes.rowsInput}
                />
                <Button size="xs" variant="default" disabled>
                  Cancel
                </Button>
                <Button size="xs" disabled>
                  Run query
                </Button>
              </Group>
            </Group>
          </Paper>

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
                Ctrl ↵ to run
              </Text>
            </Group>
            <Textarea
              value={queryText}
              readOnly
              aria-label="Query editor preview"
              autosize
              minRows={5}
              maxRows={5}
              variant="unstyled"
              classNames={{ input: classes.queryInput }}
            />
            <Divider />
            <Group
              px="md"
              py="sm"
              gap="sm"
              align="flex-start"
              wrap="nowrap"
              className={classes.diagnostic}
            >
              <Badge color="red" variant="light" size="xs">
                Error
              </Badge>
              <Box>
                <Group gap="xs">
                  <Code fz={10}>QUERY_FIELD_NOT_FOUND</Code>
                  <Text size="xs" c="dimmed">
                    line 2, columns 10–16
                  </Text>
                </Group>
                <Text size="xs" mt={3}>
                  Static source-span placement using an existing diagnostic
                  code.
                </Text>
              </Box>
            </Group>
          </Paper>

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
                <Badge size="xs" variant="light" color="teal">
                  Complete
                </Badge>
              </Group>
              <Text size="xs" c="dimmed">
                3 rows
              </Text>
            </Group>
            <Table.ScrollContainer
              minWidth={820}
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
                    <Table.Th>
                      <ColumnHeader name="@event_time" type="datetime" />
                    </Table.Th>
                    <Table.Th>
                      <ColumnHeader name="service" type="utf8?" />
                    </Table.Th>
                    <Table.Th>
                      <ColumnHeader name="status" type="int32?" />
                    </Table.Th>
                    <Table.Th>
                      <ColumnHeader name="message" type="utf8" />
                    </Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {resultRows.map((row) => (
                    <Table.Tr key={row[0]}>
                      <Table.Td className={classes.monospaceCell}>
                        {row[0]}
                      </Table.Td>
                      <Table.Td>{row[1]}</Table.Td>
                      <Table.Td className={classes.numericCell}>
                        {row[2]}
                      </Table.Td>
                      <Table.Td>{row[3]}</Table.Td>
                    </Table.Tr>
                  ))}
                </Table.Tbody>
              </Table>
            </Table.ScrollContainer>
            <Group
              justify="space-between"
              px="md"
              py="sm"
              className={classes.resultsFooter}
              wrap="wrap"
            >
              <Group gap="lg">
                <Statistic label="Segments" value="4" />
                <Statistic label="Selected bytes" value="1.8 MB" />
                <Statistic label="Output bytes" value="492 B" />
                <Statistic label="Elapsed" value="42 ms" />
              </Group>
              <Text size="xs" c="dimmed">
                Exact snapshot · schema v2
              </Text>
            </Group>
          </Paper>
        </Box>
      </AppShell.Main>

      <AppShell.Aside className={classes.rail}>
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
          <Group justify="space-between" mt="xs">
            <Box>
              <Text size="sm" fw={650}>
                demo_logs
              </Text>
              <Text size="xs" c="dimmed">
                8 fields
              </Text>
            </Box>
            <Badge size="sm" variant="light" color="cyan">
              v2 active
            </Badge>
          </Group>
        </AppShell.Section>
        <Divider />
        <AppShell.Section grow component={ScrollArea}>
          <Stack gap={0}>
            {fields.map(([name, type, nullability]) => (
              <Group
                key={name}
                justify="space-between"
                px="md"
                py="sm"
                wrap="nowrap"
                className={classes.fieldRow}
              >
                <Box className={classes.truncate}>
                  <Text
                    size="xs"
                    fw={600}
                    truncate
                    className={classes.fieldName}
                  >
                    {name}
                  </Text>
                  <Text size="xs" c="dimmed">
                    {nullability}
                  </Text>
                </Box>
                <Code fz={10}>{type}</Code>
              </Group>
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
              <Paper withBorder p="sm" radius="sm">
                <Group justify="space-between">
                  <Box>
                    <Text size="xs" fw={650}>
                      Version 2
                    </Text>
                    <Text size="xs" c="dimmed">
                      8 fields · region promoted
                    </Text>
                  </Box>
                  <Badge size="xs" variant="light" color="cyan">
                    Active
                  </Badge>
                </Group>
              </Paper>
              <Paper withBorder p="sm" radius="sm">
                <Group justify="space-between">
                  <Box>
                    <Text size="xs" fw={650}>
                      Version 1
                    </Text>
                    <Text size="xs" c="dimmed">
                      7 fields
                    </Text>
                  </Box>
                  <Badge size="xs" variant="outline" color="gray">
                    Historical
                  </Badge>
                </Group>
              </Paper>
            </Stack>
          </Box>
        </AppShell.Section>
      </AppShell.Aside>
    </AppShell>
  );
}

function ColumnHeader({
  name,
  type,
}: Readonly<{ name: string; type: string }>) {
  return (
    <Group gap={6} wrap="nowrap">
      <Text size="xs" fw={650}>
        {name}
      </Text>
      <Code fz={9}>{type}</Code>
    </Group>
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
