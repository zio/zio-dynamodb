---
id: examples
title: "Examples"
---

The `examples` sbt module (`examples/src/main/scala/zio/dynamodb/`) holds runnable-shaped
showcase code — longer, more realistic than the snippets on the other reference pages, and
compiled on every build so they can't silently drift out of date. It depends on all three
interpreters (`zioInterpreter`, `ceInterpreter`, `futureInterpreter`) plus `schemaDynamodb`/
`schemaDdbExpr`, so it's a good place to see cross-module usage in one file.

```sh
sbt examples/compile   # exercises every example, including the Docker-free showcases below
sbt examples/test      # runs the ones with real ZIO Test specs (ZIOStreamingUtils, RateLimitedReads)
```

## `OrdersCE` / `OrdersZio`

Scala 3 showcases of the [High-Level API](crud/high-level.md) — an `Order`/`Status` model,
`put`/`query`/`update` via `CompanionOptics`-generated `Lens`es, one on
`CEInterpreter`/Cats Effect (`OrdersCE`) and one on `ZioInterpreter`/ZIO (`OrdersZio`).

Client lifecycle is handled two different ways deliberately, one per effect system's idiom:
`OrdersCE` uses `cats.effect.Resource`, `OrdersZio` uses `ZLayer.scoped`/
`ZIO.acquireRelease` — both close the underlying `DynamoDbAsyncClient` on completion.

## `RateLimitedReads`

A token-bucket rate limiter built entirely from the public API — see
[Interceptor / Observability](interceptor.md#worked-example-rate-limiting-on-consumed-capacity)
for the full walkthrough. Backed by a real `ZIOSpecDefault` test (`RateLimitedReadsSpec`)
using `TestClock`, not just compiled.

## `ZIOStreamingUtils`

`batchGetItems` — grouping a `ZStream` of primary keys into `BatchGetItem` batches of up to
100, running each through `interp.run` with a `RetryPolicy`, and turning
`Incomplete`/`Failed` batch outcomes into log output instead of a fatal stream error. See
[Batch Operations](crud/batch.md#why-no-high-level-batch-api) for why this lives here rather
than as a library-provided API. Backed by a real `ZIOSpecDefault` test
(`ZIOStreamingUtilsSpec`) against a stub interpreter.

Named `ZIO`-prefixed deliberately — this is a `zio.stream.ZStream`-specific implementation,
not a cross-interpreter utility. The same grouping/retry/log-and-continue shape would be
trivial to implement against `fs2.Stream` for a Cats Effect equivalent; it just hasn't been
written, since one worked example per effect system is enough to show the pattern.
