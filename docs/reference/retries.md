---
id: retries
title: "Retries"
---

Every interpreter (`ZioInterpreter`/`CEInterpreter`/`FutureInterpreter`) retries transient
DynamoDB errors on its own — no configuration required — and any query can override that for
itself via `.withRetryPolicy(...)`.

## Default: on, AWS-recommended, zero config

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.dynamodb._

val interp: ZioInterpreter = ZioInterpreter.fromAsyncClient(DynamoDbAsyncClient.builder().build())
```

`fromAsyncClient(sdkClient)` (and the interceptor-taking overload) attach AWS's own recommended
decorrelated-jitter algorithm (`sleep = min(cap, random_between(base, previous * 3))`) as the
interpreter's default: 8 retries, 100ms base delay, 20s cap. Retried errors, via
`RetryPolicy.isRetryable`: `ProvisionedThroughputExceededException`, `RequestLimitExceeded`,
`ServiceUnavailable`, `ThrottlingException`.

## Two attachment points

| Level | Type | Set via | Scope |
|---|---|---|---|
| Request | `RetryPolicy` (pure) | `query.withRetryPolicy(policy)` | one query |
| Interpreter | `EffectfulRetryPolicy[F]` | `fromAsyncClient(sdkClient, defaultRetryPolicy)` | every query on that interpreter with no policy of its own |

**Precedence: request always wins.** A query's own `.withRetryPolicy(...)` is used if present;
the interpreter's `defaultRetryPolicy` only runs otherwise. Pass `defaultRetryPolicy = None` to
turn retrying off for an interpreter, still overridable per query:

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.dynamodb._

val noRetryInterp: ZioInterpreter =
  ZioInterpreter.fromAsyncClient(DynamoDbAsyncClient.builder().build(), defaultRetryPolicy = None)

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery.getItem("orders", PrimaryKey("orderId" -> "ord-1")).withRetryPolicy(RetryPolicy.NoRetry)
```

`EffectfulRetryPolicy[F]` can only attach at the interpreter level: `DynamoDBQuery` carries no
effect type, so it can never hold an `F`-typed value — only a plain `RetryPolicy` fits at the
query level.

## Three ways to shape a curve

| Kind | Type | Constructors | State model |
|---|---|---|---|
| Stateless | `RetryPolicy` | `RetryPolicy.custom(f)` | none |
| Stateful, pure | `RetryPolicy` | `RetryPolicy.statefulCustom(...)`, `RetryPolicy.awsRecommended(...)` | a `var` scoped to one query execution |
| Stateful, effectful | `EffectfulRetryPolicy[F]` | `<Module>RetryPolicies.statefulCustom(...)`, `.awsRecommended(...)` | the effect system's own primitive |

All three share the same guarantee: state is created fresh once per query execution
(`newAttempt()`), never shared across concurrent executions of the same policy value. Built-in
policies: `RetryPolicy.NoRetry`, `RetryPolicy.ExponentialBackoff(maxRetries, initialDelay,
factor, maxDelay, jitter)`, `RetryPolicy.awsRecommended(maxRetries, baseDelay, maxDelay)`.

```scala mdoc:compile-only
import zio.dynamodb._

import scala.concurrent.duration.FiniteDuration

// stateless — delay grows linearly with the attempt number
val linear: RetryPolicy =
  RetryPolicy.custom(attempt => if (attempt >= 5) None else Some(FiniteDuration(200L * (attempt + 1), "milliseconds")))

// stateful, pure — a custom curve, state closed over per execution
val custom: RetryPolicy =
  RetryPolicy.statefulCustom { () =>
    var previous = 0
    (attempt: Int) => { previous += 1; Some(FiniteDuration(previous.toLong * 100, "milliseconds")) }
  }
```

### Effectful, per module

| Module | `F` | Constructors | State primitive |
|---|---|---|---|
| `zio` | `Task` | `ZioRetryPolicies.awsRecommended(...)`, `.fromSchedule(schedule)` | `Ref` |
| `ce` | `IO` | `CatsRetryPolicies.awsRecommended(...)`, `.statefulCustom(initial)(next)` | `Ref` |
| `future` | `Future` | `FutureRetryPolicies.awsRecommended(...)`, `.statefulCustom(initial)(next)` | a `var` (`Future` has no `Ref`/STM equivalent) |

`ZioRetryPolicies.fromSchedule` is ZIO's standout option — it wraps a real `zio.Schedule` value
directly, reusing its combinators instead of hand-writing a curve:

```scala mdoc:compile-only
import zio._
import zio.dynamodb._

val scheduleBacked: EffectfulRetryPolicy[Task] =
  ZioRetryPolicies.fromSchedule(Schedule.exponential(50.millis) && Schedule.recurs(5))
```

Cats Effect and `Future` have no comparable combinator library to wrap, so their effectful
constructors buy idiom (`Ref`-modeled state) over the pure stateful path, not new capability —
`RetryPolicy.statefulCustom`/`.awsRecommended` already cover the same curves at the request
level.

## Batch operations: two independent loops

`batchGetItem`/`batchWriteItem` retry twice over per call, both consulting the same resolved
policy (request-level, else interpreter default): effect-level (a single `BatchGetItem`/
`BatchWriteItem` call failing with a retryable error) and response-level (AWS returning
unprocessed keys/items). See [Batch Operations](crud/batch.md#retry-behavior).

## Worked examples

- `examples/src/main/scala/examples/RetryPolicyBasics.scala` — request-level: stateless,
  stateful pure, and opting a single query out via `RetryPolicy.NoRetry`.
- `examples/src/main/scala/examples/RetryPolicyDefaults.scala` — interpreter-level: disabling
  the default, and swapping in a `zio.Schedule`-backed one.
