---
id: retries
title: "Retries"
---

Every interpreter (`ZioInterpreter`/`CEInterpreter`/`FutureInterpreter`) retries nothing on its
own, zero config — matching the AWS SDK, whose own standard retry mode is already on by
default for every operation (see AWS's
[SDKs and Tools Reference Guide, "Retry behavior"](https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html)).
Any query can attach its own policy via `.withRetryPolicy(...)`, and an interpreter can attach
a fallback for every query that doesn't via `defaultRetryPolicy`.

## Recommended: disable the SDK client's own retries, then attach one here

Running both layers at once double-retries: `withRetry`'s loop only ever sees the SDK client's
*final* outcome after the SDK's own internal retry cycle has already run, so a still-retryable
error triggers a second, uncoordinated retry cycle on top of the first — up to 8× more real
attempts than either layer's own `maxRetries` suggests (DynamoDB clients default to 8 max
attempts per call, higher than other AWS service clients), and it drains the SDK's own
client-scoped retry-quota circuit breaker faster than intended. Pick one layer, not both:

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import zio.dynamodb._

val noSdkRetries = ClientOverrideConfiguration.builder()
  .retryStrategy(AwsRetryStrategy.standardRetryStrategy().toBuilder().maxAttempts(1).build())
  .build()

val client = DynamoDbAsyncClient.builder().overrideConfiguration(noSdkRetries).build()

val interp: ZioInterpreter =
  ZioInterpreter.fromAsyncClient(client, defaultRetryPolicy = Some(ZioRetryPolicies.fullJitter()))
```

Doing this gives up nothing: `retryQuota` (below) is zio-dynamodb's own parity mechanism for
the one thing the SDK's retry layer provides that a backoff curve alone doesn't — a circuit
breaker independent of any one call's own curve.

For `batchGetItem`/`batchWriteItem`, zio-dynamodb offers retry functionality the SDK doesn't:
automatic resubmission of partial failures (unprocessed keys/items), which has no SDK-level
retry counterpart at all — see [Batch operations](#batch-operations-two-independent-loops)
below.

## Retry quota: a circuit breaker independent of the backoff curve

`retryQuota` mirrors the AWS SDK's own token-bucket retry quota — a client-scoped budget that
stops retrying once recent traffic looks bad, regardless of what any individual call's
`RetryPolicy` would otherwise allow. On by default via `fromAsyncClient`
(`<Module>RetryQuota.standard()`, 500 tokens, matching AWS's own default); pass
`retryQuota = None` to disable.

- Each retry attempt debits a cost: 5 tokens for a throttling error, 14 for other transient
  errors — mirrors AWS's own split, since a sustained wide outage and a sustained narrow one
  are different threats.
- A call that succeeds without retrying credits 1 token back; a call that succeeds after
  retrying credits back exactly what its own retries cost, no more.
- Once exhausted, a new retry attempt is denied immediately — the same outcome shape as the
  backoff curve itself running out, not a distinct error.
- Scoped to the whole interpreter, not any one query — like AWS's own quota, there's no
  per-query override.
- Doesn't gate batch's response-level resubmission loop (resubmitting unprocessed keys/items):
  that's a fresh logical request built from a successful response, not a retry of a failed
  one, so AWS's own token bucket doesn't apply there either.

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.dynamodb._

// keep the retry policy, disable just the quota
val interp: ZioInterpreter =
  ZioInterpreter.fromAsyncClient(
    DynamoDbAsyncClient.builder().build(),
    defaultRetryPolicy = Some(ZioRetryPolicies.fullJitter()),
    retryQuota = None
  )
```

## Two attachment points

| Level | Type | Set via | Scope |
|---|---|---|---|
| Request | `RetryPolicy` (pure) | `query.withRetryPolicy(policy)` | one query |
| Interpreter | `EffectfulRetryPolicy[F]` | `fromAsyncClient(sdkClient, defaultRetryPolicy)` | every query on that interpreter with no policy of its own |

**Precedence: request always wins.** A query's own `.withRetryPolicy(...)` is used if present;
the interpreter's `defaultRetryPolicy` (`None` unless set) only runs otherwise:

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.dynamodb._

val interp: ZioInterpreter =
  ZioInterpreter.fromAsyncClient(DynamoDbAsyncClient.builder().build(), defaultRetryPolicy = Some(ZioRetryPolicies.fullJitter()))

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery.getItem("orders", PrimaryKey("orderId" -> "ord-1")).withRetryPolicy(RetryPolicy.NoRetry)
```

`EffectfulRetryPolicy[F]` can only attach at the interpreter level: `DynamoDBQuery` carries no
effect type, so it can never hold an `F`-typed value — only a plain `RetryPolicy` fits at the
query level.

### `UpdateItem` is the one exception: explicit opt-in only

Every other operation below falls back to the interpreter's `defaultRetryPolicy` when it has
no policy of its own. `UpdateItem` never does — omitting `.withRetryPolicy(...)` on an
`UpdateItem` means no retry at all, regardless of what the interpreter is configured with.

Reason: `UpdateItem`'s `Action` DSL mixes idempotent updates (`.set(value)`) with
non-idempotent ones (`.add`/`.increment`/`.decrement`/`.appendList`/`.prependList` — each
applies a delta).
Retrying a request whose outcome is ambiguous (a `ServiceUnavailable`/network failure where
the write may have already landed) would silently double-apply that delta. The framework has
no way to tell which kind of action a given `UpdateItem` uses, so it can't safely default
retries on the way it does for the other operations. The request-level attachment point still
works exactly as normal — attach a policy explicitly via `query.withRetryPolicy(...)`, only
when you know the update is actually idempotent.

| Operation | Falls back to `defaultRetryPolicy`? |
|---|---|
| `GetItem`, `PutItem`, `DeleteItem`, `Query`, `Scan`, `BatchGetItem`, `BatchWriteItem` | Yes |
| `UpdateItem` | No — explicit `.withRetryPolicy(...)` only |

## Three ways to shape a curve

| Kind | Type | Constructors | State model |
|---|---|---|---|
| Stateless | `RetryPolicy` | `RetryPolicy.custom(f)` | none |
| Stateful, pure | `RetryPolicy` | `RetryPolicy.statefulCustom(...)` | a `var` scoped to one query execution |
| Stateful, effectful | `EffectfulRetryPolicy[F]` | `<Module>RetryPolicies.statefulCustom(...)` | the effect system's own primitive |

All three share the same guarantee: state is created fresh once per query execution
(`newAttempt()`), never shared across concurrent executions of the same policy value. Built-in
policies: `RetryPolicy.NoRetry`, `RetryPolicy.ExponentialBackoff(maxRetries, initialDelay,
factor, maxDelay, jitter)`, and `RetryPolicy.fullJitter(maxRetries, baseDelay, maxDelay)` — a
stateless preset over `ExponentialBackoff` fixing `factor = 2.0`/`jitter = true`, matching what
AWS SDKs actually implement as their own default retry mode.

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
| `zio` | `Task` | `ZioRetryPolicies.fullJitter(...)`, `.statefulCustom(...)`, `.fromSchedule(schedule)` | none for `fullJitter`; `Ref` otherwise |
| `ce` | `IO` | `CatsRetryPolicies.fullJitter(...)`, `.statefulCustom(initial)(next)` | none for `fullJitter`; `Ref` otherwise |
| `future` | `Future` | `FutureRetryPolicies.fullJitter(...)`, `.statefulCustom(initial)(next)` | none for `fullJitter`; a `var` otherwise (`Future` has no `Ref`/STM equivalent) |

`ZioRetryPolicies.fromSchedule` is ZIO's standout option — it wraps a real `zio.Schedule` value
directly, reusing its combinators instead of hand-writing a curve:

```scala mdoc:compile-only
import zio._
import zio.dynamodb._

val scheduleBacked: EffectfulRetryPolicy[Task] =
  ZioRetryPolicies.fromSchedule(Schedule.exponential(50.millis) && Schedule.recurs(5))
```

Cats Effect and `Future` have no comparable combinator library to wrap, so their effectful
`statefulCustom` constructor buys idiom (`Ref`-modeled state) over the pure stateful path, not
new capability — `RetryPolicy.statefulCustom` already covers the same curves at the request
level.

## Batch operations: two independent loops

`batchGetItem`/`batchWriteItem` retry twice over per call, both consulting the same resolved
policy (request-level, else interpreter default): effect-level (a single `BatchGetItem`/
`BatchWriteItem` call failing with a retryable error) and response-level (AWS returning
unprocessed keys/items). See [Batch Operations](crud/batch.md#retry-behavior).

## Worked examples

- `examples/src/main/scala/examples/RetryPolicyBasics.scala` — request-level: stateless,
  stateful pure, and opting a single query out via `RetryPolicy.NoRetry`.
- `examples/src/main/scala/examples/RetryPolicyDefaults.scala` — interpreter-level: leaving
  `defaultRetryPolicy` at its off default, and opting in with a `zio.Schedule`-backed one
  instead; includes a `batchGetItem` with no policy of its own, run against both interpreters
  to show the same fallback governs batch's response-level loop too.

## Which exceptions actually retry, and what the quota looks like at real volume

Two test suites double as runnable documentation here — each assertion is also a concrete,
checked claim about behavior:

- `aws/src/test/scala/zio/dynamodb/RealAwsInterpreterRetryLoopSpec.scala` — exceptions that
  retry (`ProvisionedThroughputExceededException`, `RequestLimitExceededException`, a generic
  `ThrottlingException` error code, 500, 503) next to ones in the 400 range that don't
  (`ResourceNotFoundException`, `ConditionalCheckFailedException`, a generic
  `ValidationException` error code).
- `aws/src/test/scala/zio/dynamodb/RealAwsInterpreterRetryQuotaSpec.scala` — `retryQuota`
  at AWS's real 500-token default: a mixed-error volume comfortably under capacity (crediting
  recovers the budget), and a sustained failure volume over capacity (the quota, not the
  backoff curve, stops it — with the exact attempt count at which it does).
