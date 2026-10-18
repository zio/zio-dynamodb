---
id: retries
title: "Retries"
---

Three retry loops can act on a call:

| Loop | Retries | Default | Configured via |
|---|---|---|---|
| AWS SDK retry | a failed request, inside the SDK client | **on** — standard mode; DynamoDB clients default to 8 retries (9 attempts) | the `DynamoDbAsyncClient`'s `ClientOverrideConfiguration` ([AWS: Retry behavior](https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html)) |
| Effect-level retry | a failed request whose error is retryable, around the SDK call | **off** | `.withRetryPolicy(policy)` per call, `defaultRetryPolicy` per interpreter |
| Batch unprocessed retry | the unprocessed keys / items of a **successful** `batchGetItem` / `batchWriteItem` response | **on** — `RetryPolicy.fullJitter()` | `.withBatchUnprocessedRetryPolicy(policy)` per call, `defaultBatchUnprocessedRetryPolicy` per interpreter |

Each setting governs only its own loop. Both of zio-dynamodb's loops share the interpreter's
`retryQuota` (on by default), a client-wide circuit breaker.

## Default behavior

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.dynamodb._

val interp: ZioInterpreter = ZioInterpreter.fromAsyncClient(DynamoDbAsyncClient.builder().build())
```

- **Ordinary operations** are retried by the SDK only.
- **Batch unprocessed keys / items** are resubmitted by zio-dynamodb: up to 8 times, 100 ms base
  delay doubling to a 20 s cap, full jitter. Each resubmission costs `retryQuota` 5 tokens. What
  is still unprocessed at the end is returned as `Incomplete`.

The two don't overlap: the SDK never resubmits unprocessed items, since a partial result isn't
an error to it.

The downsides of leaving failed-request retries to the SDK:

- **No observability:** the SDK retries internally, with no hook for your code. zio-dynamodb
  reports each of its retries to an interceptor — see [Observing retries](#observing-retries).
- **No per-query curve:** the SDK applies one retry strategy to every call on the client.
  zio-dynamodb takes a policy per call (`.withRetryPolicy`).
- **Slow unhappy-path tests:** tests wait out the SDK's real retry delays. zio-dynamodb's own
  delays can be skipped with ZIO's `TestClock` or Cats Effect's `TestControl` (`Future` always
  waits in real time).

To avoid these, see [Moving failed-request retry into zio-dynamodb](#moving-failed-request-retry-into-zio-dynamodb).

To leave unprocessed items for you to handle, opt out per interpreter or per call:

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.dynamodb._

val interp: ZioInterpreter =
  ZioInterpreter.fromAsyncClient(DynamoDbAsyncClient.builder().build(), defaultBatchUnprocessedRetryPolicy = None)

val oneCall =
  DynamoDBQuery
    .batchGetItem(List("alice", "bob"))(id => DynamoDBQuery.GetItem("customers", PrimaryKey("customerId" -> id)))
    .withBatchUnprocessedRetryPolicy(RetryPolicy.NoRetry)
```

## Moving failed-request retry into zio-dynamodb

Turn the SDK's retries off and set an effect-level policy instead:

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

This gives `RetryInterceptor` visibility, per-query curves (`.withRetryPolicy`) and unhappy-path
tests that don't wait for real retry delays. `retryQuota` takes over the role of the SDK's own
retry quota. `fullJitter()` uses the same curve shape as the SDK's standard mode.

### Never both for failed requests

With the SDK's retries **and** effect-level retry on:

1. The SDK runs its whole retry cycle inside one call.
2. The effect-level loop only sees that call's final outcome, and retries it — running the SDK's
   cycle again.
3. Worst case: `(SDK attempts) × (effect-level attempts)` — 9 × 9 = 81 requests with both at
   8 retries — and the SDK's own retry quota drains faster than intended.

The batch unprocessed retry is exempt: it resubmits from a successful response, which the SDK
never retries.

## Batch operations: two independent loops

One `batchGetItem` / `batchWriteItem` call runs as follows:

1. The request is sent. A failed request goes through the SDK's retries and, if configured, the
   effect-level loop.
2. If the response has unprocessed keys / items, the batch unprocessed policy picks a delay. If
   the policy is exhausted, or `retryQuota` can't pay the 5-token cost, the call ends with
   `Incomplete`.
3. Otherwise `BatchUnprocessedRetryInterceptor` fires, the loop sleeps, and only the unprocessed
   keys / items are resubmitted — back to step 1. Items fetched by earlier attempts are kept.
4. The call ends with one of:
   - `Complete` — nothing left unprocessed; the resubmissions' quota cost is credited back.
   - `Incomplete` — the response, including what is still unprocessed.
   - `Failed(cause, unprocessedRetries, effectRetries)` — a request failed after all retries.

Non-fatal failures are returned as values, not raised; see [Batch Operations](crud/batch.md).

The loops nest: with both on, every resubmission runs through the effect-level loop, so the worst
case is effect-level attempts × unprocessed attempts.

| SDK retries | Effect-level | Batch unprocessed | Outcome |
|---|---|---|---|
| On | Off | On | **Default.** SDK retries failed requests; zio-dynamodb resubmits unprocessed items |
| Off | On | On | Both handled by zio-dynamodb, each with its own policy, sharing `retryQuota` |
| On | Off | Off | Unprocessed items returned as `Incomplete` for you to handle |
| On | On | either | Avoid — failed requests are retried twice over (see [above](#never-both-for-failed-requests)) |

Tuning the curve for one call:

```scala mdoc:compile-only
import zio.dynamodb._

import scala.concurrent.duration.DurationInt

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .batchGetItem(List("alice", "bob"))(id => DynamoDBQuery.GetItem("customers", PrimaryKey("customerId" -> id)))
    .withBatchUnprocessedRetryPolicy(RetryPolicy.ExponentialBackoff(maxRetries = 5, initialDelay = 50.millis))
```

## Reference

### Attachment points

| Setting | Level | Type | Governs | Default |
|---|---|---|---|---|
| `.withRetryPolicy(policy)` | call | `RetryPolicy` | effect-level | — |
| `defaultRetryPolicy` | interpreter (`fromAsyncClient`) | `EffectfulRetryPolicy[F]` | effect-level | `None` |
| `.withBatchUnprocessedRetryPolicy(policy)` | call | `RetryPolicy` | batch unprocessed | — |
| `defaultBatchUnprocessedRetryPolicy` | interpreter (`fromAsyncClient`) | `RetryPolicy` | batch unprocessed | `Some(RetryPolicy.fullJitter())` |
| `retryQuota` | interpreter (`fromAsyncClient`) | `RetryQuota[F]` | both | `<Module>RetryQuota.standard()` |

A call's own policy always wins over the interpreter's. `RetryPolicy.NoRetry` turns a loop off for
one call. `EffectfulRetryPolicy[F]` attaches only at the interpreter: `DynamoDBQuery` carries no
effect type, so it can't hold an `F`-typed value.

```scala mdoc:compile-only
import zio.dynamodb._

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery.getItem("orders", PrimaryKey("orderId" -> "ord-1")).withRetryPolicy(RetryPolicy.NoRetry)
```

### Which operations zio-dynamodb retries

| Operation | `.withRetryPolicy` | `defaultRetryPolicy` |
|---|---|---|
| `GetItem`, `PutItem`, `DeleteItem`, `Query`, `Scan`, `BatchGetItem`, `BatchWriteItem` | Yes | Yes |
| `UpdateItem` | Yes | No |
| `TransactGetItems`, `TransactWriteItems`, `CreateTable`, `DeleteTable`, `DescribeTable` | No — ignored | No |

Operations zio-dynamodb doesn't retry are still retried by the SDK, if its retries are on.

`UpdateItem` needs its own `.withRetryPolicy(...)` because its actions include non-idempotent
ones (`.add`, `.increment`, `.decrement`, `.appendList`, `.prependList`): retrying an ambiguous
failure — a timeout where the write may have landed — would apply the delta twice. Attach a
policy only when the update is idempotent.

### Three ways to shape a curve

| Kind | Type | Constructors | State model |
|---|---|---|---|
| Stateless | `RetryPolicy` | `RetryPolicy.custom(f)` | none |
| Stateful, pure | `RetryPolicy` | `RetryPolicy.statefulCustom(...)` | a `var` scoped to one execution |
| Stateful, effectful | `EffectfulRetryPolicy[F]` | `<Module>RetryPolicies.statefulCustom(...)` | the effect system's own primitive |

State is created fresh per execution (`newAttempt()`), never shared across concurrent executions
of the same policy value. Built-ins: `RetryPolicy.NoRetry`,
`RetryPolicy.ExponentialBackoff(maxRetries, initialDelay, factor, maxDelay, jitter)`, and
`RetryPolicy.fullJitter(maxRetries = 8, baseDelay = 100.millis, maxDelay = 20.seconds)` —
`ExponentialBackoff` with `factor = 2.0` and full jitter.

```scala mdoc:compile-only
import zio.dynamodb._

import scala.concurrent.duration.FiniteDuration

// stateless — delay grows linearly with the attempt number
val linear: RetryPolicy =
  RetryPolicy.custom(attempt => if (attempt >= 5) None else Some(FiniteDuration(200L * (attempt + 1), "milliseconds")))

// stateful, pure — state closed over per execution
val custom: RetryPolicy =
  RetryPolicy.statefulCustom { () =>
    var previous = 0
    (attempt: Int) => { previous += 1; Some(FiniteDuration(previous.toLong * 100, "milliseconds")) }
  }
```

Effectful policies, per module:

| Module | `F` | Constructors | State primitive |
|---|---|---|---|
| `zio` | `Task` | `ZioRetryPolicies.fullJitter(...)`, `.statefulCustom(...)`, `.fromSchedule(schedule)` | none for `fullJitter`; `Ref` otherwise |
| `ce` | any `F[_]: cats.effect.Async` | `CERetryPolicies.fullJitter[F](...)`, `.statefulCustom[F, S](initial)(next)` | none for `fullJitter`; `Ref` otherwise |
| `future` | `Future` | `FutureRetryPolicies.fullJitter(...)`, `.statefulCustom(initial)(next)` | none for `fullJitter`; a `var` otherwise |

`ZioRetryPolicies.fromSchedule` wraps a `zio.Schedule`, reusing its combinators:

```scala mdoc:compile-only
import zio._
import zio.dynamodb._

val scheduleBacked: EffectfulRetryPolicy[Task] =
  ZioRetryPolicies.fromSchedule(Schedule.exponential(50.millis) && Schedule.recurs(5))
```

### Retry quota

A client-scoped token bucket mirroring the AWS SDK's own retry quota: it stops retrying once
recent traffic looks bad, whatever each call's policy would allow. `<Module>RetryQuota.standard()`
holds 500 tokens, matching AWS's default; pass `retryQuota = None` to disable.

| Event | Tokens |
|---|---|
| Effect-level retry of a throttling error | −5 |
| Effect-level retry of another transient error | −14 |
| Batch unprocessed resubmission | −5 |
| Request that succeeds on its first attempt (with an effect-level policy set; batch requests always) | +1 |
| Success after retrying | + what those retries cost |

An exhausted quota denies the retry immediately: the effect-level loop fails with the last error
(`Failed` for a batch), the batch unprocessed retry ends with `Incomplete`. The quota is shared by every call on the
interpreter; there is no per-call override.

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

### Observing retries

`fromAsyncClient`'s `interceptors = InterceptorConfig(...)` takes two retry observers, called
just before each sleep:

| Field | Type | Called with |
|---|---|---|
| `retry` | `RetryInterceptor[F]` | `onRetry(meta, error, attempt)` — each effect-level retry |
| `batchUnprocessedRetry` | `BatchUnprocessedRetryInterceptor[F]` | `onUnprocessedKeysRetry(keys, attempt)` / `onUnprocessedItemsRetry(puts, deletes, attempt)` — each batch resubmission |

Neither sees the SDK's internal retries. See also [Interceptor / Observability](interceptor.md).

### Which exceptions retry

Applies to the effect-level loop; the SDK classifies its own retries.

| Error | Examples | Retried | Quota cost |
|---|---|---|---|
| Throttling (`AwsServiceException#isThrottlingException`) | `ProvisionedThroughputExceededException`, `RequestLimitExceeded`, `ThrottlingException` | Yes | 5 |
| Server error, status 500 / 502 / 503 / 504 | `InternalServerError`, `ServiceUnavailable` | Yes | 14 |
| Client-side failure (`SdkClientException`) | connection reset, timeout, DNS failure | Yes | 14 |
| `NonRetryableException` | a request invalid however often it's sent | No | — |
| Any other 4xx | `ResourceNotFoundException`, `ConditionalCheckFailedException`, `ValidationException` | No | — |

Checked in `aws/src/test/scala/zio/dynamodb/`: `RealAwsInterpreterRetrySpec` (classification),
`RealAwsInterpreterRetryLoopSpec` (through the retry loop) and `RealAwsInterpreterRetryQuotaSpec`
(the quota at its 500-token default, under and over capacity).

### Worked examples

- `examples/src/main/scala/examples/RetryPolicyBasics.scala` — call-level policies: stateless,
  stateful, and `RetryPolicy.NoRetry`.
- `examples/src/main/scala/examples/RetryPolicyDefaults.scala` — interpreter-level: both loops
  off, and a `zio.Schedule`-backed `defaultRetryPolicy`.
- `examples/src/main/scala/examples/lowerlevelapi/Batch.scala` — overriding the batch
  unprocessed curve for one call.
