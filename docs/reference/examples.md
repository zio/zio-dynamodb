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
sbt examples/test      # runs the ones with real ZIO Test specs (ZIOStreamingUtils)
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
done entirely on the `Table` value via `.deriving` — the model carries no `@Modifier`
annotations and nothing is resolved from implicit scope. Chains `withFieldNameMapper` /
`withCaseNameMapper` (deriver-wide) and `withModifier(typeId, field, Modifier.rename(...))`
(per-field) on the `DynamoDBCodecDeriverConfig` value passed into `.deriving`. Shows the
precedence: the per-field `withModifier` rename wins over the table-wide field-name mapper.

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

## `ZIOStreamingUtils`

`batchGetItems` — grouping a `ZStream` of primary keys into `BatchGetItem` batches of up to
100, running each through `interp.run` with a `RetryPolicy`, and turning
`Incomplete`/`Failed` batch outcomes into log output instead of a fatal stream error. See
[Batch Operations](crud/batch.md#batch-and-the-high-level-api) for why this lives here rather
than as a library-provided API. Backed by a real `ZIOSpecDefault` test
(`ZIOStreamingUtilsSpec`) against a stub interpreter.

Named `ZIO`-prefixed deliberately — this is a `zio.stream.ZStream`-specific implementation,
not a cross-interpreter utility. The same grouping/retry/log-and-continue shape would be
trivial to implement against `fs2.Stream` for a Cats Effect equivalent; it just hasn't been
written, since one worked example per effect system is enough to show the pattern.
