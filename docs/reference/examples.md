---
id: examples
title: "Examples"
---

The `examples` sbt module (`examples/src/main/scala/examples/`) holds runnable-shaped showcase
code — longer, more realistic than the snippets on the other reference pages. Deliberately
`package examples`, not `zio.dynamodb`, so it only ever sees the same public API a real
external consumer does.

```sh
sbt examples/compile   # exercises every example, including the Docker-free showcases below
sbt examples/test      # runs the ones with real ZIO Test specs (ZIOBatchStreamingUtils)
```

## `OrdersCE` / `OrdersZio`

Scala 3 showcases of the [High-Level API](crud/high-level.md) — an `Order`/`Status` model,
`put`/`query`/`update` via `CompanionOptics`-generated `Lens`es, one on
`CEInterpreter`/Cats Effect (`OrdersCE`) and one on `ZioInterpreter`/ZIO (`OrdersZio`).

Client lifecycle is handled two different ways deliberately, one per effect system's idiom:
`OrdersCE` uses `cats.effect.Resource`, `OrdersZio` uses `ZLayer.scoped`/
`ZIO.acquireRelease` — both close the underlying `DynamoDbAsyncClient` on completion.

## `OrdersConfigured`

The same `Order`/`Status` model, focused on **codec configuration** in the High-Level API,
done entirely in code — the model carries no `@Modifier` annotations. Derives the codec with
`withFieldNameMapper` / `withCaseNameMapper` (deriver-wide) and `.modifier(Order.customerId,
Modifier.rename(...))` (per-field, by optic), and passes it to `Table(name, codec)`. Shows the
precedence: the per-field rename wins over the deriver-wide field-name mapper.

## `CapacityWarnings`

A stateless [`ResponseInterceptor`](interceptor.md) that logs a warning whenever a single
call's total consumed capacity crosses a threshold — sums `readCapacityUnits`/
`writeCapacityUnits` across `ConsumedCapacity` (single-item ops) or `Chunk[ConsumedCapacity]`
(batch/transact ops). See [Interceptor / Observability](interceptor.md#worked-examples) for
the full walkthrough.

## `CapacityAccumulator`

A stateful `ResponseInterceptor` that sums consumed capacity across every request in a
session via one `Ref`, reusing `CapacityWarnings.capacityUnitsOf` — contrast with
`CapacityWarnings`, which holds no state and decides per call. See
[Interceptor / Observability](interceptor.md#worked-examples) for the full walkthrough.

## `RetryPolicyBasics`

Request-level [retry policies](retries.md): a stateless linear backoff, a stateful custom
curve via `RetryPolicy.statefulCustom`, and opting one query out via `RetryPolicy.NoRetry` —
all via `.withRetryPolicy(...)`, regardless of the interpreter's configured default. See
[Retries](retries.md#three-ways-to-shape-a-curve).

## `RetryPolicyDefaults`

Interpreter-level [retry policies](retries.md): leaving `defaultRetryPolicy` at its off
default (`defaultRetryPolicy = None`) and opting in with a `zio.Schedule`-backed one via
`ZioRetryPolicies.fromSchedule` — run against both a plain `getItem` and a `batchGetItem` with
no policy of its own. The batch's unprocessed keys are governed separately, by
`defaultBatchUnprocessedRetryPolicy` (on by default; the no-retry interpreter sets it to `None`).
See [Retries](retries.md#attachment-points).

## `ZIOBatchStreamingUtils`

`batchGetItems` — grouping a `ZStream` of primary keys into `BatchGetItem` batches of up to
100, running each through `interp.run` (unprocessed keys resubmitted per the interpreter's default,
or an explicit `RetryPolicy` via an overload), and turning
`Incomplete`/`Failed` batch outcomes into log output instead of a fatal stream error. See
[Batch Operations](crud/batch.md) for the Low-Level batch API this builds on. Backed by a
real `ZIOSpecDefault` test
(`ZIOBatchStreamingUtilsSpec`) against a stub interpreter.

Named `ZIO`-prefixed deliberately — this is a `zio.stream.ZStream`-specific implementation,
not a cross-interpreter utility. The same grouping/retry/log-and-continue shape would be
trivial to implement against `fs2.Stream` for a Cats Effect equivalent; it just hasn't been
written, since one worked example per effect system is enough to show the pattern.
