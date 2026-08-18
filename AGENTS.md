# Engineering principles

## Simplicity and necessity

- Correctness comes first. Within a correct design, apply KISS and YAGNI before adding abstraction, configuration, extensibility, or infrastructure.
- Build the smallest complete solution for the approved requirements.
- Prefer a little obvious duplication to a premature abstraction.
- Keep changes local and data flow visible. Delete obsolete code and avoid wrappers that add no semantic value.
- Do not use *Clean Code*, class counts, method length, or similar aesthetic doctrine as design authority. Judge code by correctness, clarity in context, change cost, and resource behavior.

## Strong types

- Use the strictest available compiler, type-checker, linter, and warning mode. Treat warnings as errors.
- Use strong typing in dynamic languages, including Python and Lua.
- Model domain distinctions, units, states, nullability, and errors explicitly. Make invalid states unrepresentable inside the domain model; reject invalid external states at boundaries.
- Accept untyped data only at an external boundary. Validate it once and convert it into domain types before it enters the program.
- Never suppress type checking for convenience. A narrow, documented escape hatch is allowed only inside an adapter for an external library that genuinely cannot be annotated or wrapped more safely.

```text
# Avoid: uncertainty spreads through the program.
fn amount(response: Any):
    return response["payment"]["amount"]

# Prefer: uncertainty ends at one checked boundary.
fn decode_payment(raw: Unknown) -> Result[Payment, DecodeError]:
    object = expect_object(raw)?
    return Payment(
        id=PaymentId(expect_string(object["id"])?),
        amount=Money.parse(object["amount"])?,
    )
```

## Functional design

- Prefer pure functions, immutable data, explicit transformations, and composition.
- Use a **functional core and imperative shell**. Keep decisions and domain logic deterministic; keep I/O, clocks, randomness, concurrency, and mutation at explicit boundaries.
- Prefer total functions and exhaustive handling of tagged unions. Model expected failures as typed data.
- Pass dependencies and capabilities explicitly. Avoid service locators, dependency-injection containers, and hidden mutable global state.
- Prefer functions, modules, records, and tagged unions to classes, inheritance, and service objects.
- Do not use OOP as the default organizing model. Use a stateful object only when an external API requires it or it directly models a necessary resource or lifecycle.
- Keep ownership of state, resources, and policy explicit at the caller boundary.

```text
# Functional core.
fn renew(subscription: Subscription, now: Instant) -> Renewal:
    return Renewal(
        subscription=with_expiry(subscription, now + 30 days),
        notification=Renewed(subscription.owner),
    )

# Imperative shell.
fn handle_renewal(id: SubscriptionId) -> Result[Unit, RenewalError]:
    current = database.load(id)?
    renewal = renew(current, clock.now())
    database.save(renewal.subscription)?
    email.send(renewal.notification)?
    return Ok(Unit)
```

## Safety and resources

- Bound loops, recursion, retries, queues, batches, concurrency, memory, and execution time. Make limits and units explicit.
- Validate external input. Assert internal invariants. Check errors and return values; never discard failure silently.
- Check arithmetic, indexing, sizes, conversions, cancellation, cleanup, backpressure, and overload behavior where relevant.
- Always account for CPU, memory, allocations, copies, I/O, network round trips, storage, latency, and operational load.
- Prefer simple control flow, cohesive functions, narrow scope, static analysis, and zero unexplained warnings.
- Treat external input as hostile, apply least privilege, and never expose secrets through logs, errors, command arguments, or persisted diagnostics.

```text
# Avoid: input size controls memory and concurrency.
items = read_all(stream)
await gather(process(item) for item in items)

# Prefer: resource limits are explicit and enforced.
limits = ProcessingLimits(batch_size=256, concurrency=8)
for batch in batches(stream, max_items=limits.batch_size):
    await parallel_map(batch, process, max_concurrency=limits.concurrency)
```

## Dependencies and architecture

- Prefer the standard library, existing dependencies, and existing infrastructure.
- Every new dependency or service must justify its code, security, upgrade, failure, resource, and operational costs.
- Do not introduce an operational component when an existing one reliably meets the contract.
- Design around contracts, invariants, data flow, and failure boundaries rather than frameworks or fashionable patterns.
- Introduce an abstraction only for demonstrated variation or a real boundary.

# Testing principles

## Every test must earn its place

The default is not to add a test. For every candidate, explain:

- the important and plausible regression it detects;
- the observable behavior it protects;
- why it survives a correct internal refactoring;
- what confidence it adds beyond existing tests and the type system;
- why its level (unit, integration, end-to-end) is the cheapest reliable one;
- its determinism, feedback time, readability, and maintenance cost.

Judge its value qualitatively as:

> regression protection × refactoring resistance × feedback speed × maintainability

A near-zero factor can make the whole test worthless. Redesign or omit it. A small suite of valuable tests is better than a large mediocre suite. Coverage is diagnostic information, never a target.

## Test observable behavior

- Test units of behavior, not classes, methods, or implementation structure.
- Prefer output-based tests of the functional core.
- Use state-based tests when output alone cannot express the contract.
- Use communication-based tests only when the interaction itself is an externally observable contract.
- Use realistic integration tests for application-owned managed dependencies when they provide the most faithful and economical proof.
- Mock unmanaged external boundaries selectively. Do not mock internal collaborators merely to manufacture isolation.
- Keep end-to-end tests few and focused on critical journeys.
- Use as many assertions as needed to describe one observable behavior. Do not split a coherent scenario to satisfy "one assertion per test" dogma.
- Let tests shape public contracts, not production internals. Do not widen visibility, add interfaces, or introduce abstractions solely to mock or reach implementation details.

```text
# Avoid: freezes an internal interaction without proving the result.
test "checkout saves":
    repository = mock()
    checkout(repository, cart)
    assert repository.save_called_once()

# Prefer: protects an observable domain rule.
test "checkout rejects a total above the customer limit":
    result = quote(cart, customer_with_limit(100))
    assert result == Error(LimitExceeded(total=120, limit=100))
```

The preferred example still needs a credible regression risk and must not duplicate stronger existing coverage.

## Do not add tests that merely

- Exercise private methods, trivial accessors, constructors, or language and library guarantees.
- Mirror production logic or assert incidental call sequences.
- Freeze incidental SQL, serialization, logs, snapshots, or object structure without a real compatibility contract.
- Add one test per method, class, branch, or coverage gap by ritual.
- Duplicate another test at a more expensive level.

If automation has insufficient value, omit it, state why, and use the most appropriate alternative verification.

## Test-first integrity

- For new behavior or a bug fix, demonstrate before implementation that the new test fails for the expected reason. A compile failure is acceptable only when the approved public API does not exist yet.
- Characterization tests for behavior-preserving refactors may start green; they must protect valuable observable behavior rather than implementation accidents.
- Do not write production behavior during the test-only phase.
- Do not weaken or rewrite an approved test merely to make implementation pass.
- If a test exposes a flaw in the specification, return to human review instead of encoding a silent assumption.
