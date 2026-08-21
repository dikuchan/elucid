# Elucid UI

The Elucid UI is a same-origin browser application for querying events and inspecting bounded operational state. Its production assets are built ahead of time and embedded into the Elucid server; the release does not require a Node.js runtime or a separate frontend service.

## Current slice

The current implementation is a static layout reference. It contains representative source, schema, diagnostic, and result data so the information hierarchy can be reviewed before application behavior is added. It performs no HTTP requests, owns no query state machine, and does not claim that the displayed values came from a running server.

## Stack

- React and Vite provide the client-only SPA and production asset build.
- Mantine Core and Mantine Hooks provide the design system, accessibility behavior, theme tokens, layout primitives, controls, and dense data presentation.
- TanStack Query will own remote server state, bounded retries, request races, invalidation, and GET cancellation.
- TanStack Table will own the result-grid model without changing the server-defined row order or query semantics.
- TanStack Virtual will be added only if configured result limits or measurements justify virtualization.
- TanStack Router will be added when the application has a second addressable screen or useful URL state. The single workspace does not need it yet.
- TanStack Start is excluded because the Rust process already owns the HTTP server, API, and embedded asset delivery; a second server runtime, SSR, and server functions do not serve this application.
- CodeMirror 6 will provide the query editor and source-span decorations.
- Zod will validate unknown JSON once at the HTTP boundary before values enter application code.
- Strict TypeScript, type-aware ESLint, Prettier, and focused Vitest and Testing Library tests form the frontend quality boundary.

Dependencies are installed only in the change that first uses them. The static layout therefore installs React, Mantine, and build-time quality tooling, while TanStack, CodeMirror, Zod, and test dependencies remain documented decisions until their owning behavior lands.

## State and network boundaries

- TanStack Query will manage source, schema, status, segment, and dead-letter reads. Retry policy must distinguish network and retryable service failures from permanent HTTP and invalid-response failures.
- Synchronous query execution will use a mutation with automatic retries disabled. Retrying a POST can duplicate expensive work even though Elucid stores no durable query execution.
- Query cancellation must abort the underlying fetch so the Rust service observes the disconnect and cancels in-process execution.
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

Run the complete frontend gate:

```console
npm run check
```

Individual checks remain available as `npm run format:check`, `npm run lint`, `npm run typecheck`, and `npm run build`.
