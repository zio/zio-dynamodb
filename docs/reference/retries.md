---
id: retries
title: "Retries"
---

zio-dynamodb's own retry is off by default on every interpreter (`ZioInterpreter`/
`CEInterpreter`/`FutureInterpreter`) — deliberately, since the AWS SDK client's own standard
retry mode is already on by default for every operation (see AWS's
[SDKs and Tools Reference Guide, "Retry behavior"](https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html)).
The two defaults are complementary, not redundant: leaving zio-dynamodb's retry off avoids
stacking a second, uncoordinated retry cycle on top of the SDK's own. Any query can opt in via
`.withRetryPolicy(...)`, and an interpreter can set a fallback for every query that doesn't via
`defaultRetryPolicy`.

## Choosing a configuration

Two independent toggles govern retry behavior: the AWS SDK client's own retry strategy (on by
default) and zio-dynamodb's own retry (off by default, attached via `.withRetryPolicy`/
`defaultRetryPolicy`). `batchGetItem`/`batchWriteItem` add a third, independent toggle — the
response-level resubmission loop (`.withResponseRetryPolicy`) — since that loop has no
SDK-level equivalent to conflict with in the first place.

### General operations

| SDK retries | zio-dynamodb retry | Outcome | Recommendation |
|---|---|---|---|
| Off | On | one coordinated retry cycle, `retryQuota` as the circuit breaker | **Recommended** — full control; see [below](#recommended-disable-the-sdk-clients-own-retries-then-attach-one-here) |
| On | Off | the SDK's own retry cycle, zero config, invisible to `RetryInterceptor` | Acceptable default — fine until you need retry observability or a custom curve |
| On | On | two uncoordinated retry cycles stacked on the same failure, up to 8× the attempts either layer's `maxRetries` suggests | Avoid |
| Off | Off | no retries at all | Only if something upstream of zio-dynamodb already retries |

The main reason to pick `Off`/`On` over the zero-config default: `RetryInterceptor` only ever
sees retries zio-dynamodb itself drives — there's no hook into the SDK's own internal retry
loop, so turning the SDK's retries off is the only way to get retry observability (or a
per-query/custom curve) at all.

### Batch operations (`batchGetItem`/`batchWriteItem`)

Same two toggles, plus the response-level one. Only the response-level loop is exempt from
the SDK conflict — effect-level batch retry has exactly the same double-stacking problem as
the general case above, batch isn't special there:

| SDK retries | Effect-level (`.withRetryPolicy`) | Response-level (`.withResponseRetryPolicy`) | Outcome | Recommendation |
|---|---|---|---|---|
| On | Off | On | SDK handles whole-call failures invisibly, same as doing nothing; zio-dynamodb only resubmits unprocessed keys/items — no overlap with the SDK at all | **Recommended** when the SDK's own transient-failure handling is enough and you only want unprocessed-item handling added on top; see below |
| Off | On | On (same policy, inherited via `.withRetryPolicy` alone) | one coordinated policy for both loops, `retryQuota` as the circuit breaker | **Recommended** for full control |
| On | On | On | effect-level stacks with the SDK's own retries (same problem as the general case); response-level is still fine | Avoid the effect-level half — set `.withResponseRetryPolicy` directly instead of relying on `.withRetryPolicy`'s fallback |
| On | Off | Off | zero config; a partial failure surfaces as `Incomplete` for you to handle yourself | Acceptable default |
| Off | Off | Off | no retries anywhere, including unprocessed items | Only if you handle `Incomplete` entirely yourself |

The `On`/`Off`/`On` row is not a complete retry story: an effect-level failure (the whole call
throwing, after the SDK's own retries already ran) surfaces as `Failed` immediately —
exactly as if no retry policy were set at all, since effect-level retry is off. It only adds
automatic unprocessed-item handling on top of the SDK's existing, invisible transient-failure
handling; it doesn't give you back any visibility or control over effect-level failures. If
you also want that (e.g. `RetryInterceptor` observability, a custom curve), use `Off`/`On`/`On`
instead — see [below](#why-split-them-only-one-loop-is-safe-to-double-up-with-the-sdks-own-retries).

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
breaker independent of any one call's own curve. In exchange it buys what the SDK's own retry
can't offer at any configuration: `RetryInterceptor` observability (the SDK retries
internally, invisible to any hook on this side), a per-query curve, and deterministic,
`TestClock`-driven retry tests — `RetryInterceptor` is the sharpest reason to make this
switch, since there's no way to get that visibility with the SDK's retries left on.

For `batchGetItem`/`batchWriteItem`, zio-dynamodb offers retry functionality the SDK doesn't:
automatic resubmission of partial failures (unprocessed keys/items), which has no SDK-level
retry counterpart at all. Unlike the general advice above, that particular loop is actually
safe to use even with the SDK client's own retries left on — see
[Batch operations](#batch-operations-two-independent-loops) below.

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
the interpreter's `defaultRetryPolicy` (`None` unless set) only runs otherwise. This precedence
is purely about which zio-dynamodb policy applies — it says nothing about the SDK client's own
retries. Whichever level supplies the policy, the SDK client's own retries still need to be
off to avoid double-retrying; there's no precedence rule that lets a request-level policy
safely coexist with the SDK's own retry on that same call.

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

`batchGetItem`/`batchWriteItem` retry twice over per call: effect-level (a single
`BatchGetItem`/`BatchWriteItem` call failing with a retryable error) and response-level (AWS
returning unprocessed keys/items). Each loop has its own attachment point:

| Loop | Set via | Falls back to |
|---|---|---|
| Effect-level | `.withRetryPolicy(policy)` | `defaultRetryPolicy`, then none |
| Response-level | `.withResponseRetryPolicy(policy)` | `.withRetryPolicy(policy)`, then `defaultRetryPolicy`, then none |

Setting only `.withRetryPolicy(...)` (the common case) governs both loops identically, exactly
as before `.withResponseRetryPolicy` existed. `.withResponseRetryPolicy(...)` is additive: set
it to give the response-level loop its own curve, independent of — and taking precedence over
— whatever `.withRetryPolicy` resolves to for that call. Effect-level retry never consults
`responseRetryPolicy`.

### Why split them: only one loop is safe to double up with the SDK's own retries

[Recommended](#recommended-disable-the-sdk-clients-own-retries-then-attach-one-here) above is
to disable the SDK client's own retries and use `defaultRetryPolicy`/`.withRetryPolicy`
instead, since running both at once double-retries a single failing call. That problem is
specific to the *effect-level* loop — a whole `BatchGetItem`/`BatchWriteItem` call throwing a
retryable exception is exactly the case the SDK's own retry strategy already handles, so
stacking zio-dynamodb's effect-level retry on top re-retries what the SDK already retried.

The *response-level* loop never has that problem: it resubmits `unprocessedKeys`/
`unprocessedItems` from a **successful** response — a case the SDK's retry strategy was never
involved in and never will be. Neither the low-level `DynamoDbAsyncClient` nor the DynamoDB
Enhanced Client (which zio-dynamodb doesn't use) auto-resubmits unprocessed items regardless
of retry configuration — both require the caller to do it, per AWS's own documentation for
each client layer. So it's safe to set `.withResponseRetryPolicy(...)` on a batch query while
leaving the SDK client's own retries on for everything else, including that same batch call's
effect-level failures:

```scala mdoc:compile-only
import zio.dynamodb._

import scala.concurrent.duration.DurationInt

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .batchGetItem(List("alice", "bob"))(id => DynamoDBQuery.GetItem("customers", PrimaryKey("customerId" -> id)))
    .withResponseRetryPolicy(RetryPolicy.ExponentialBackoff(maxRetries = 5, initialDelay = 50.millis))
```

No `.withRetryPolicy(...)` here — the SDK client's own retry strategy (left at its default)
handles effect-level failures for this call, same as every other operation; only the
response-level resubmission of unprocessed keys is zio-dynamodb's to do, since the SDK has no
equivalent for it either way. See [Batch Operations](crud/batch.md#retry-behavior).

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
