---
name: rust
description: Implement idiomatic, safe and efficient Rust code.
---

# Core philosophy

## Safety first

- `unsafe` is forbidden unless explicitly requested and accompanied by a rationale.
- Every `unsafe` block must be wrapped in a `// SAFETY:` comment explaining why the operation is sound.
- Never use `unsafe` to bypass the borrow checker. Redesign the data structure instead.

## Type-driven design

- Make invalid states unrepresentable. Use `enum`s to encode state machines and sealed vocabularies.
- Use newtypes to give semantic meaning to raw values (e.g., `struct TableName(String)` instead of passing bare `String`).
- Prefer compile-time guarantees over runtime checks.

## Ownership as documentation

- A function that takes ownership communicates that it needs the value beyond the call.
- A function that borrows communicates temporary, non-consuming access.
- Design APIs so the ownership model reflects the intent.

# Error handling

## Crate-level strategy

- Library crates (`elucid-language`, `elucid-engine`): use `thiserror` to define typed error enums.
- Application crates (`elucid-cli`): use `anyhow` for flexible, context-rich error handling.
- Every library error type must implement `std::error::Error` + `Display` + `Debug`.

## Rules

- Never use `unwrap()` or `expect()` in non-test library code.
- Propagate errors with `?`. Never silently ignore errors.
- Wrap lower-level errors with context before propagating: `anyhow::Context`, or custom error variants with `#[source]`.
- Always return `Result` from fallible functions. Never panic.
- In CLI code, use `anyhow::Result` with `.context()` to add human-readable context.
- Match on specific error variants when recovery is possible; use `?` when it is not.

# Naming and API design

## Naming conventions (RFC 430)

- Types, traits, enums: `UpperCamelCase`.
- Functions, methods, variables, modules: `snake_case`.
- Constants and statics: `SCREAMING_SNAKE_CASE`.
- Lifetime parameters: short and meaningful (`'a`, `'src`, `'input`).

## Conversion conventions

- `as_` methods: cheap, borrowed, never fail (e.g., `as_str()`).
- `to_` methods: owned, may allocate (e.g., `to_string()`).
- `into_` methods: consume `self`, return owned (e.g., `into_inner()`).
- `From`/`Into` for idiomatic conversions; prefer `impl From<A> for B` over `fn into_b(self)`.

## Getters and setters

- Getters: no `get_` prefix. Use `span.start()`, not `span.get_start()`.
- Boolean getters may use `is_`, `has_`, `can_` prefixes (e.g., `is_empty()`).
- Setters use `set_` prefix when the type also has a getter.

## Constructors and builders

- Use `new()` for the primary, obvious constructor.
- Use descriptive names for alternative constructors (e.g., `from_parts()`, `with_config()`).
- Use the builder pattern for types with many optional fields.
- Builders should consume `self` and return `Result` if construction can fail.

# Type design

## Derives

- Derive `Debug` on all public types. Always.
- Derive `Clone` when copying is meaningful and cheap.
- Derive `PartialEq`/`Eq` when value comparison makes semantic sense.
- Derive `Hash` alongside `Eq` when the type may be used as a map key.
- Derive `Copy` only for small, fixed-size types where bitwise copy is correct.
- Do not duplicate trait bounds on struct definitions. Put them on the impl block.

## Struct design

- All struct fields are private by default. Expose via getters or builder methods.
- Use `#[non_exhaustive]` on public enums and structs to allow future extension without breaking changes.
- Prefer small, focused structs over large catch-all types.

## Enums

- Use `enum`s to model state machines. Make invalid transitions unrepresentable at compile time.
- Use `match` exhaustively. Avoid `_` catch-all unless the enum is `#[non_exhaustive]`.
- `unreachable!()` is acceptable for branches that are structurally impossible, but must include a comment explaining why.

## Traits

- Keep traits small and focused (single responsibility).
- Use sealed traits (private supertrait) to prevent downstream implementations when the trait is an implementation detail.
- Prefer trait bounds on impl blocks, not on struct definitions.
- Implement `Display` manually for types that have a canonical string representation. Do not derive it.

# Concurrency

## Async runtime

- I/O must be async. Use `tokio` as the async runtime.
- Never block the async runtime. CPU-bound work goes in `tokio::task::spawn_blocking`.

## Tasks

- Spawned tasks must be `'static` + `Send`. Do not borrow local data across `.await` boundaries without ownership.
- Use `tokio::spawn` for fire-and-forget concurrent work.
- Use `JoinHandle` when you need to await the result of a spawned task.
- Prefer `tokio::spawn` over `select!` when tasks are independent and long-lived.
- Prefer `select!` over `tokio::spawn` when you need to multiplex on the same task.

## Shared state

- Prefer channels over shared mutable state.
- Use `std::sync::Mutex` (not `tokio::sync::Mutex`) when the lock is not held across `.await`.
- Never hold a `MutexGuard` across `.await`. Restructure into a non-async method or use a block scope to drop before `.await`.
- Use `Arc` for shared ownership across tasks. Be intentional — `Arc` has a cost.
- For high-contention maps, consider sharding or `dashmap`.
- Avoid `Rc` in async code. `Rc` is `!Send` and will break `tokio::spawn`.

## Channels

- Always use bounded channels. Unbounded channels hide backpressure and can exhaust memory.
- `mpsc`: multi-producer, single-consumer. Use for command queues, worker dispatch.
- `oneshot`: single-producer, single-consumer, single value. Use for request-response patterns.
- `broadcast`: multi-producer, multi-consumer, all receivers see all messages. Use for pub-sub.
- `watch`: multi-producer, multi-consumer, only latest value. Use for configuration or state updates.

## Select

- Use `tokio::select!` to wait on multiple async operations concurrently within a single task.
- In loops, use `tokio::pin!` when resuming an async operation across `select!` iterations.
- Use pattern matching in `select!` branches to handle `None` (closed channel) gracefully.
- Include an `else` branch when pattern matching could match nothing.
- `select!` randomly picks which branch to poll first, preventing starvation.

## Cancellation and shutdown

- Cancellation is dropping the future. Ensure `Drop` implementations clean up resources.
- Use `CancellationToken` from `tokio_util` for cooperative graceful shutdown.
- Use `TaskTracker` from `tokio_util` to wait for groups of tasks to complete.
- Handle `tokio::signal::ctrl_c()` for clean OS signal shutdown.
- Design shutdown as: signal → cancel tokens → wait for tasks → drop resources.

## Bridging sync and async

- Use `tokio::task::spawn_blocking` for CPU-bound or blocking operations from async context.
- Use `runtime.block_on()` to enter async context from sync code, not `#[tokio::main]`.
- For embedded runtimes in sync applications, use `new_current_thread()` runtime on a dedicated thread.

# Observability

## Tracing over print

- Use `tracing` instead of `println!`, `eprintln!`, or `dbg!` in any non-test code.
- Register the tracing subscriber early in `main`. Library crates must never set a global subscriber.
- Use `#[tracing::instrument]` to automatically create spans for function entry/exit.
- Use `#[tracing::instrument(skip(self, large_field))]` to avoid logging large or sensitive values.

## Levels

- `error!`: unrecoverable failure requiring immediate attention.
- `warn!`: degraded behavior, unexpected but handled.
- `info!`: application lifecycle events (startup, shutdown, configuration).
- `debug!`: diagnostic information useful during development.
- `trace!`: very verbose, per-request or per-iteration detail.

## Structured logging

- Use fields for structured data: `tracing::info! { table = %name, rows = count, "query executed" }`.
- Never log secrets, credentials, tokens, PII, or full request bodies.
- Use `%` display modifier for `Display` types, `?` for `Debug` types in tracing macros.

# Testing

## Structure

- Unit tests: `#[cfg(test)] mod tests` at the bottom of the source file.
- Integration tests: `tests/` directory at the crate root.
- Test helpers: module-level helper functions (e.g., `parse_ok()`) that provide clear failure diagnostics.
- Use `insta` for snapshot testing of complex output (AST, IR, diagnostics).

## Async testing

- Use `#[tokio::test]` for async test functions.
- Use `tokio::time::pause()` to control time in tests that depend on timers.

## Coverage

- Test error paths, not just happy paths.
- Every public function should be testable from outside the module.
- Test edge cases: empty input, maximum values, boundary conditions, malformed input.
- Parser tests should cover both valid and invalid inputs.

# Module structure

## Visibility

- Everything is private by default. Add `pub` only when external access is required.
- Re-export the public API from `lib.rs` with `pub use`.
- Keep implementation details (internal helpers, parsing machinery) private.

## Organization

- One primary type or concern per file.
- Use `mod.rs` for sub-module directories.
- Group related functionality into modules.
- Module hierarchy should reflect the domain, not the implementation.

## Re-exports

- `lib.rs` is the crate's public surface. Re-export types users need.
- Users should not need to `use` internal module paths.
- `pub use ast::*` is acceptable when the AST types are the module's entire public API.

# Performance

## Borrowing and ownership

- Borrow when you do not need ownership: `&str` over `String`, `&[u8]` over `Vec<u8>`, `&Path` over `PathBuf`.
- Prefer iterators over index-based loops. They compose, avoid bounds checks, and are zero-cost.
- Be intentional about `.clone()`. If you clone, know why. If you can borrow, prefer borrowing.

## Allocations

- Pre-allocate collections when the size is known: `Vec::with_capacity()`, `HashMap::with_capacity()`.
- Reuse buffers across iterations instead of allocating fresh ones.
- Avoid `format!()` in hot paths; prefer `write!()` to a reusable `String` or `std::fmt` display.

## Abstractions

- Trait-based generics are zero-cost. Trust monomorphization.
- `dyn Trait` has a vtable cost. Use it only when dynamic dispatch is required.
- `Arc` adds atomic reference counting overhead. Do not wrap everything reflexively.
- Prefer stack allocation over heap when the size is known at compile time.

# Code quality

## Tooling

- `cargo fmt` before every commit. No exceptions.
- `cargo clippy` must pass with no warnings. Use `#![deny(clippy::all)]` if the project enforces it.
- `cargo test` must pass. No partial commits with known test failures.

## Hygiene

- No `todo!()` or `unimplemented!()` in committed code without a tracking issue number in a comment.
- No commented-out code in committed files.
- No `panic!()` in library code outside of `unreachable!()` with justification.
- No `dbg!()` in committed code.

## Documentation

- First and foremost: **DON'T** add comments just for the sake of it. Write self-explanatory code in the first place. Write a doc comment only when it's really needed (e.g. complex invariants).
- Document panics (`# Panics`), errors (`# Errors`), and safety (`# Safety`) sections **WHEN APPLICABLE**. Not all errors deserve a dedicated section.
- Examples in doc comments use `?` for error handling, not `unwrap()`.

# Dependencies

- Prefer `std` over a crate when the standard library provides what you need.
- Write it yourself if the dependency is trivial and does not involve subtle correctness (e.g., crypto, parsers for complex formats, Unicode algorithms).
- Before adding a dependency, check: does it pull in heavy transitive deps? Is it actively maintained? Is it widely used in the ecosystem?
- Re-evaluate dependencies that are used for only a single function. Inline it if it is simple.
- Never add a dependency that duplicates functionality already available from an existing dependency in the workspace.

# Project-specific conventions

## Workspace

- Edition 2024.
- Every normal, development, build, target-specific, external, and internal path dependency used by a workspace member is declared once under `[workspace.dependencies]` in the workspace root `Cargo.toml`.
- Member crates inherit dependencies with `workspace = true`. A member may add only the dependency features it requires.
