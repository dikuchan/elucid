# Elucid UI

The Elucid UI is a same-origin browser application for querying events and inspecting bounded operational state. Its production assets are built ahead of time and embedded into the Elucid server; the release does not require a Node.js runtime or a separate frontend service.

## Current slice

The current implementation is the bounded query workspace. It reads the source list and active schema from the same-origin API, runs and cancels synchronous queries, validates every response before it enters application state, maps Rust UTF-8 diagnostic spans into the editor, and renders typed rows and execution statistics. Ingestion and storage state and production asset embedding are separate delivery slices.

## Stack

- React and Vite provide the client-only SPA and production asset build.
- Mantine Core and Mantine Hooks provide the design system, accessibility behavior, theme tokens, layout primitives, controls, and dense data presentation.
- TanStack Query owns remote server state, bounded read retries, request races, invalidation, GET cancellation, and the non-retrying query mutation.
- TanStack Table will own the result-grid model without changing the server-defined row order or query semantics.
- TanStack Virtual will be added only if configured result limits or measurements justify virtualization.
- TanStack Router will be added when the application has a second addressable screen or useful URL state. The single workspace does not need it yet.
- TanStack Start is excluded because the Rust process already owns the HTTP server, API, and embedded asset delivery; a second server runtime, SSR, and server functions do not serve this application.
- CodeMirror 6 provides the query editor, keyboard execution, and source-span decorations.
- Zod validates unknown JSON once at the HTTP boundary before values enter application code.
- Strict TypeScript, type-aware ESLint, Prettier, and focused Vitest contract tests form the frontend quality boundary.

Dependencies are installed only in the change that first uses them. TanStack Table, TanStack Virtual, TanStack Router, and browser-component test tooling remain deferred until their owning behavior justifies them.

## State and network boundaries

- TanStack Query manages source and schema reads in this slice; status, segment, and dead-letter reads join the same boundary when their views land. Read retries are limited to network, overload, timeout, and server failures; permanent HTTP and invalid-response failures are not retried.
- Synchronous query execution uses a mutation with automatic retries disabled. Retrying a POST can duplicate expensive work even though Elucid stores no durable query execution.
- Query cancellation aborts the underlying fetch so the Rust service observes the disconnect and cancels in-process execution.
- Editor text, explicit UTC range, selected source, and presentation preferences are local UI state. They do not belong in the remote-state cache.
- Every successful and error response enters through one runtime-decoding module. TypeScript declarations alone are not accepted as proof that Rust JSON matches the browser contract.

## Layout

- The header identifies the application and reserves space for deployment and service health.
- The left rail owns bounded source selection.
- The center workspace owns the explicit UTC range, output-row limit, query editor, source-span diagnostics, run and cancel controls, result table, completion state, and execution statistics.
- The right rail owns the active schema, field details, and immutable schema history for the selected source.
- Operational ingestion and storage views will reuse this shell in the next UI slice instead of adding an unrelated dashboard.

The application uses system fonts and local bundled styles. Production code must not load scripts, fonts, styles, source maps, or other assets from external origins.

## Commands

Install the exact lockfile:

```console
npm ci
```

Run the local development server:

```console
npm run dev
```

The development server proxies `/api` to the validation server at `http://127.0.0.1:58080`. Set `ELUCID_UI_API_TARGET` before starting Vite to use another Elucid listener. Production remains same-origin and does not use this proxy.

Run the complete frontend gate:

```console
npm run check
```

Individual checks remain available as `npm run format:check`, `npm run lint`, `npm test`, `npm run typecheck`, and `npm run build`.
