# Elucid UI

The Elucid UI is a same-origin browser application for querying events and inspecting bounded operational state. Its production assets are built ahead of time and embedded into the Elucid server; the release does not require a Node.js runtime or a separate frontend service.

## Current slice

The current implementation is a bounded query and operations workspace. It reads the source list and active schema from the same-origin API, runs and cancels synchronous queries, validates every response before it enters application state, maps Rust UTF-8 diagnostic spans into the editor, and renders typed rows and execution statistics. The operations view shows component health, effective limits, durable spool usage, publication backlog, maintenance ownership, and bounded segment and dead-letter lists for the selected source. The production build generates an ignored asset directory that `elucid-service` embeds; the release binary needs neither Node.js nor a separate frontend process.

## Stack

- React and Vite provide the client-only SPA and production asset build.
- Mantine Core and Mantine Hooks provide the design system, accessibility behavior, theme tokens, layout primitives, controls, and dense data presentation.
- TanStack Query owns remote server state, bounded read retries, request races, invalidation, GET cancellation, fixed operational polling, and the non-retrying query mutation.
- TanStack Table will own the result-grid model without changing the server-defined row order or query semantics.
- TanStack Virtual will be added only if configured result limits or measurements justify virtualization.
- TanStack Router will be added when the application has a second addressable screen or useful URL state. The local Query/Operations switch does not create a second URL or require a router.
- TanStack Start is excluded because the Rust process already owns the HTTP server, API, and embedded asset delivery; a second server runtime, SSR, and server functions do not serve this application.
- CodeMirror 6 and a generated incremental Lezer parser provide the query editor, EQL syntax highlighting, keyboard execution, and source-span decorations.
- Zod validates unknown JSON once at the HTTP boundary before values enter application code.
- Strict TypeScript, type-aware ESLint, Prettier, and focused Vitest contract tests form the frontend quality boundary.

Dependencies are installed only in the change that first uses them. TanStack Table, TanStack Virtual, TanStack Router, and browser-component test tooling remain deferred until their owning behavior justifies them.

The browser grammar exists only to classify tokens for responsive highlighting while a query is being edited. The Rust lexer and parser remain authoritative for language acceptance, and backend diagnostics remain an independent source-span layer rather than being inferred from Lezer recovery nodes.

## State and network boundaries

- TanStack Query manages source, schema, status, segment, and dead-letter reads. Status refreshes every 2.5 seconds; source-specific operational lists refresh every 5 seconds only while the Operations view is active. Read retries are limited to network, overload, timeout, and server failures; permanent HTTP and invalid-response failures are not retried.
- Synchronous query execution uses a mutation with automatic retries disabled. Retrying a POST can duplicate expensive work even though Elucid stores no durable query execution.
- Query cancellation aborts the underlying fetch so the Rust service observes the disconnect and cancels in-process execution.
- Editor text, explicit UTC range, selected source, and presentation preferences are local UI state. They do not belong in the remote-state cache.
- Every successful and error response enters through one runtime-decoding module. TypeScript declarations alone are not accepted as proof that Rust JSON matches the browser contract.

## Layout

- The header identifies the application, switches between Query and Operations locally, and shows the service phase.
- The left rail owns bounded source selection.
- The center workspace owns either the explicit UTC range, output-row limit, query editor, source-span diagnostics, run and cancel controls, result table, completion state, and execution statistics, or the bounded operational status, segment, and dead-letter views.
- The right rail owns the active schema, field details, and immutable schema history for the selected source.
- Query and Operations reuse the same selected source, source rail, schema rail, and application shell.

The application uses system fonts, local bundled styles, and one forced light color scheme. It does not follow the system theme or expose a theme switch. Production code must not load scripts, fonts, styles, source maps, or other assets from external origins.

## Commands

Build the complete release from the repository root:

```console
make build
```

The build installs frontend dependencies from the exact lockfile, recreates the ignored production assets, and then runs a locked Cargo release build. The resulting binary is `elucid/target/release/elucid`.

Install the exact lockfile:

```console
npm ci
```

Run the local development server:

```console
npm run dev
```

The development server proxies `/api` to the Elucid server at `http://127.0.0.1:58080`. Set `ELUCID_UI_API_TARGET` before starting Vite to use another Elucid listener. Production remains same-origin and does not use this proxy.

Run the complete frontend gate:

```console
npm run check
```

Individual checks remain available as `npm run format:check`, `npm run lint`, `npm test`, `npm run typecheck`, and `npm run build`.
