# Batch API execution-semantics design

**Status: implemented** on branch `3.x_batch_unify_execution` (2026-08-04). See
"Implementation notes" at the end for two consequences discovered while implementing that
weren't spelled out in the design above.

Addresses item 0 in `zd_3_todo.md`: *"This is a mismatch between the Crud API and the Batch
API execution semantics."* Scope is deliberately narrow — how a batch query is **invoked**
and what its **result shape** is, not the query-building API. Adding schema-aware batch
builders to `DdbExprApi` (`zd_3_todo.md` item 1) is a natural follow-on but is out of scope
here and shouldn't block this change; it's noted under "Non-goals."

## Problem statement

```scala
// Crud — single entry point, typed result
for {
  _      <- interpreter.run(DdbExprApi.put(table, Task("t1", Priority.High)))
  result <- interpreter.run(DdbExprApi.get[Task](table)(Task.id.partitionKey === "t1"))
} yield assertTrue(result == Right(Task("t1", Priority.High)))

// Batch — different entry point, different result shape
for {
  _      <- Batch.runWriteItem(interp)(
              DynamoDBQuery.batchWriteItem(items)(i => DynamoDBQuery.putItem(table, i))
            )
  result <- Batch.runGetItem(interp)(
              DynamoDBQuery.batchGetItem(ids)(id => DynamoDBQuery.GetItem(table, PrimaryKey("id" -> id)))
            )
} yield assert(result)(isSubtype[Batch.GetResult.Complete](anything))
```

Every other `DynamoDBQuery` — `GetItem`, `PutItem`, `QuerySome`, `TransactGetItems`, even
`BatchGetItem`/`BatchWriteItem` themselves for a single attempt — executes through the one
polymorphic entry point, `AwsInterpreter[F]#run[Out](query: DynamoDBQuery[_, Out]): F[Out]`
(`core/.../Interpreter.scala:27`). Batch is the only operation family that requires a
second, parallel entry point — `Batch.runWriteItem`/`Batch.runGetItem`
(`core/.../Batch.scala:99,129`) — which takes the interpreter as an explicit curried
argument instead of the query carrying its own execution, the way `.run(query)` does for
everything else.

## Root cause

`interpreter.run(batchQuery)` already works today and already does *effect-level* retry
(transient failures — throttling, network) via the query's own `retryPolicy` field, exactly
like `GetItem`/`PutItem`/`DeleteItem` (`Interpreter.scala:238-241`):

```scala
case q: DynamoDBQuery.BatchGetItem =>
  q.retryPolicy.fold(runBatchGetItem(q))(p => withRetry(p)(runBatchGetItem(q))).asInstanceOf[F[Any]]
```

What it does *not* do is *response-level* retry — resubmitting a new `BatchGetItem`/
`BatchWriteItem` built from `response.unprocessedKeys`/`unprocessedItems` until either
nothing is left over or the policy is exhausted. That loop only exists in
`Batch.retryGet`/`Batch.retryWrite` (`Batch.scala:145-198`), which is why reaching it
requires bypassing `.run` entirely and going through `Batch.runGetItem`/`runWriteItem`
instead.

This asymmetry is architecturally deliberate, not an oversight — `AwsInterpreter`'s effect
primitives are explicitly widened to `private[dynamodb]` so that `Batch` (same package, not
a subclass) can drive this second loop:

> `core/.../Interpreter.scala:41-42` — "flatMap and pure are widened to private[dynamodb]
> so BatchUtils (same package, not a subclass) can drive the residual retry loop."

Concretely, three things differ between the two call shapes:

1. **Entry point.** `interpreter.run(query)` vs. `Batch.runXItem(interp)(query)` — a
   second, differently-shaped function the caller has to know exists.
2. **Result type.** CRUD returns the decoded/typed value directly in `F`
   (`F[Either[ItemError, From]]`, `F[Option[Item]]`). Batch returns a bespoke outcome ADT
   (`Batch.WriteResult`/`Batch.GetResult`: `Complete`/`Incomplete`/`Failed`) wrapping the
   *raw* `BatchGetItem.Response`/`BatchWriteItem.Response`, which itself needs a further
   manual step (`BatchGetItem.toGetItemResponses`, a key-subset match against
   `Set[Item]`) to get anything usable.
3. **Retry mechanism.** CRUD's retry is a single loop, fully inside `runAny`. Batch's is
   two loops (effect-level + response-level) that live outside `runAny`, re-implementing
   `withRetryTracked` bookkeeping that never touches the generic interpreter path.

One thing that is *not* a bug and must be preserved: batch effect-level failures are
captured as a *value* (`WriteResult.Failed`/`GetResult.Failed`) rather than raised as a
failed `F`. That's a confirmed, deliberate decision (memory `project_error_handling`,
"Option C") — a batch is inherently partial (some items can succeed while others don't),
so collapsing that into a single failed effect would lose information CRUD doesn't have to
represent in the first place. The design below keeps this distinction; it does not try to
force batch onto CRUD's raise-on-failure model.

## Proposed design

Move the response-level retry loop from `Batch.scala` into `Interpreter.runAny`, and change
what `BatchGetItem`/`BatchWriteItem` declare as their `Out` type, so `.run` becomes
sufficient on its own.

### 1. Fold the response-level loop into `runAny`

Replace the current batch cases in `Interpreter.scala:238-241` with a private recursive
helper per operation, directly ported from `Batch.retryGet`/`Batch.retryWrite` — same
primitives (`withRetryTracked`, `flatMap`, `sleep`, `pure`), just relocated inside the class
that already owns them instead of reached through a `private[dynamodb]`-widened back door:

```scala
case q: DynamoDBQuery.BatchGetItem   =>
  runBatchGetItemRetrying(q, q.retryPolicy.getOrElse(RetryPolicy.NoRetry), attempt = 0).asInstanceOf[F[Any]]
case q: DynamoDBQuery.BatchWriteItem =>
  runBatchWriteItemRetrying(q, q.retryPolicy.getOrElse(RetryPolicy.NoRetry), attempt = 0).asInstanceOf[F[Any]]

// ...

private def runBatchGetItemRetrying(
  q: DynamoDBQuery.BatchGetItem,
  policy: RetryPolicy,
  attempt: Int
): F[Batch.GetResult] =
  flatMap(withRetryTracked(policy)(runBatchGetItem(q))) {
    case Left((cause, effectRetries)) =>
      pure(Batch.GetResult.Failed(cause, responseRetries = attempt, effectRetries = effectRetries))
    case Right(response) if response.unprocessedKeys.isEmpty =>
      pure(Batch.GetResult.Complete(response))
    case Right(response) =>
      policy.nextDelay(attempt) match {
        case None    => pure(Batch.GetResult.Incomplete(response))
        case Some(d) =>
          flatMap(sleep(d)) { _ =>
            runBatchGetItemRetrying(DynamoDBQuery.BatchGetItem(requestItems = response.unprocessedKeys), policy, attempt + 1)
          }
      }
  }
// runBatchWriteItemRetrying mirrors this for BatchWriteItem.Response.unprocessedItems
```

`withRetryTracked` here fully subsumes the plain `withRetry` used by every other
`retryPolicy`-bearing node — no double-retry risk, since (unlike `Batch.scala` today) there
is no second `interp.run(q.copy(retryPolicy = None))` call recursing back through the
retry-wrapped path.

### 2. Change the declared `Out` type of the batch ADT nodes

```scala
// was: extends Constructor[Any, BatchGetItem.Response]
private[dynamodb] final case class BatchGetItem(...) extends Constructor[Any, Batch.GetResult]

// was: extends Constructor[Any, BatchWriteItem.Response]
private[dynamodb] final case class BatchWriteItem(...) extends Constructor[Any, Batch.WriteResult]
```

`DynamoDBQuery.batchGetItem`/`batchWriteItem` (`DynamoDBQuery.scala:431,436`) already return
the concrete `BatchGetItem`/`BatchWriteItem` type, not a `DynamoDBQuery[Any, Response]`
alias, so this is not a breaking change at the builder call site — only `interpreter.run`'s
inferred `Out` changes, from `F[BatchGetItem.Response]` to `F[Batch.GetResult]`. No
information is lost: the raw response is still reachable via `.response` on
`Complete`/`Incomplete`.

### 3. Retire `Batch.runWriteItem`/`Batch.runGetItem`

Once (1) and (2) land, these two methods are exactly what `interpreter.run(batchQuery)`
does. Delete them outright rather than deprecate-and-keep — `series/3.x` is pre-release
with a "no history" migration policy (per `CLAUDE.md`), so there's no compatibility
surface to preserve. `Batch.WriteResult`/`Batch.GetResult` stay exactly as they are; only
the two `run*` entry points go away.

A side effect: `AwsInterpreter`'s `flatMap`/`pure`/`sleep`/`attempt`/`raiseError` no longer
need the `private[dynamodb]` widening called out in the `Interpreter.scala:41-42` comment —
nothing outside the class calls them anymore (confirmed: `Batch.scala` is currently the only
external caller in `main` sources; a few CE/Future interpreter tests call `sleep`/`attempt`
directly on a concrete interpreter instance, unaffected by tightening visibility on the
trait). Tightening this is optional cleanup, not required for the unification itself.

### Resulting call site

```scala
for {
  _      <- interpreter.run(DynamoDBQuery.batchWriteItem(items)(i => DynamoDBQuery.putItem(table, i)))
  result <- interpreter.run(DynamoDBQuery.batchGetItem(ids)(id => DynamoDBQuery.GetItem(table, PrimaryKey("id" -> id))))
} yield assert(result)(isSubtype[Batch.GetResult.Complete](anything))
```

Same `.run(query)` shape as the CRUD example; `Batch.GetResult`/`WriteResult` remain the
result type (preserving the deliberate errors-as-values semantics), just reached the same
way everything else is reached.

## Non-goals

- **Schema-aware batch builders** (`DdbExprApi.batchGet`/`batchPut`, `zd_3_todo.md` item 1)
  — building typed batch queries from `[A: Schema]` items, and decoding
  `Batch.GetResult.Complete.response` back into `Either[ItemError, A]` per item (mirroring
  `DdbExprApi.get`'s decode step). This design makes that follow-on strictly additive: once
  `.run` returns `Batch.GetResult` uniformly, `DdbExprApi.batchGet` only needs to build the
  query and post-process the same `Batch.GetResult` this design already produces.
- **Unifying failure semantics** — batch keeps "errors as values"; CRUD keeps "errors as a
  failed effect / `Either` in the success channel." These are different because the
  operations are different (partial multi-item vs. single-item), not because of an API
  inconsistency.
- **Transact operations** (`zd_3_todo.md` item 2) — `transactGetItems`/`transactWriteItems`
  already run through the ordinary `.run` path with no separate entry point; they're
  unaffected by this change and don't need one.

## Migration impact

`Batch.runWriteItem`/`Batch.runGetItem` are referenced in four files (all tests; no other
`main` source depends on them beyond `Batch.scala` itself):

- `core/src/test/scala/zio/dynamodb/BatchSpec.scala`
- `it/src/test/scala/zio/dynamodb/BatchSpec.scala`
- `zio/src/test/scala/zio/dynamodb/RetrySpec.scala`
- `zio/src/test/scala/zio/dynamodb/StreamingUtils.scala`

Each call site changes from `Batch.runWriteItem(interp)(q)` to `interp.run(q)` (and the
`GetItem`/`WriteItem` equivalent) — a mechanical rewrite, no test-assertion changes expected
since `Batch.WriteResult`/`GetResult` are unchanged.

## Open questions

- Should `runBatchGetItemRetrying`/`runBatchWriteItemRetrying` live in `Interpreter.scala`
  directly (as sketched above), or in a private helper trait mixed into `AwsInterpreter` to
  keep `runAny` shorter? Either works; `Interpreter.scala` is already the single home for
  all `runAny` dispatch logic, so keeping them there (like the existing `validateCE`/
  `validateKCE` private helpers) seems most consistent with the file's current shape.
- `RetryPolicy.NoRetry` becomes the implicit default the moment a batch query has no
  `retryPolicy` attached (matching `Batch.runWriteItem`'s current fallback exactly). Worth
  confirming this is still the desired default now that it's the *only* path, rather than
  an opt-in fallback inside a dedicated batch runner.

## Pros vs Cons

### Pros

- **Single execution model.** `interpreter.run(query)` becomes sufficient for every
  `DynamoDBQuery` variant, batch included — one thing to teach/document/discover instead of
  two parallel call shapes.
- **No information loss.** The raw `BatchGetItem.Response`/`BatchWriteItem.Response` stays
  reachable via `.response` on `Complete`/`Incomplete`; nothing `Batch.runX` exposes today
  is dropped, only relocated.
- **Removes duplicated retry bookkeeping.** `Batch.retryGet`/`retryWrite` and `runAny`'s
  per-node retry dispatch currently reimplement adjacent logic (`withRetryTracked` vs.
  `withRetry`) in two places; folding removes one of them.
- **Shrinks `AwsInterpreter`'s exposed surface.** The `private[dynamodb]` widening of
  `flatMap`/`pure`/`sleep`/`attempt`/`raiseError` exists solely so `Batch.scala` can reach
  them from outside the class (`Interpreter.scala:41-42`). Once nothing outside
  `Interpreter.scala` needs them, they can go back to `protected`.
- **Sets up item 1 cleanly.** Schema-aware batch builders (`DdbExprApi.batchGet`/`batchPut`)
  become purely additive — build a query, post-process the `Batch.GetResult` `.run` already
  produces — rather than also having to pick which of two runners to call.

### Cons

- **Wider breaking-change surface than the four `Batch.runX`-referencing test files.**
  Grepping for direct `interpreter.run(batchQuery)` calls (bypassing `Batch.runX` entirely)
  turns up more call sites:
    - `core/src/test/scala/zio/dynamodb/Client.scala:34` explicitly type-ascribes
      `DummyIO[BatchWriteItem.Response]` — stops compiling once `Out` changes to
      `Batch.WriteResult`.
    - `it/src/test/scala/zio/dynamodb/DynamoDBLowLevelApiSpec.scala:701` binds
      `response <- interpreter.run(batch.asInstanceOf[DynamoDBQuery.BatchGetItem])` and
      then calls `batch.toGetItemResponses(response)`, which needs the raw `Response` — a
      real rewrite (unwrap `.response` from `Complete`/`Incomplete` first), not just a
      call-site rename.
    - A handful of other `it` call sites (`DynamoDBLowLevelApiSpec.scala:264,280,281,295,322`,
      `InterceptorSpec.scala:250`) discard the result (`_ <- interpreter.run(...)`) and
      compile unaffected, but are worth a pass to confirm none relied on `Response`-shaped
      inference elsewhere.

  The "Migration impact" section above should be read as the `Batch`-facing subset, not the
  complete list of files to touch.
- **Couples `Interpreter.scala` to `Batch`.** Today `Interpreter.scala` has zero references
  to `Batch`; afterward, `runAny`'s batch cases return `Batch.GetResult`/`WriteResult`
  directly, so the core ADT-dispatch file — shared by every interpreter (zio, ce, future) —
  now depends on a type defined elsewhere. A one-way coupling that didn't exist before.
- **Removes the "just give me the raw response" escape hatch.** Calling
  `interpreter.run(batchQuery)` directly today is a legitimate way to get one unwrapped
  attempt with no `Complete`/`Incomplete`/`Failed` ceremony, and several `it` tests do
  exactly that (see above). After this change every batch execution is wrapped in the
  outcome ADT, even for callers who'd rather handle `unprocessedKeys`/`unprocessedItems`
  themselves without response-level retry semantics imposed on them.
- **`runAny` takes on more responsibility.** The two batch cases become the only branches
  in `runAny` that loop/resubmit based on a runtime value rather than dispatching once;
  that logic has to live somewhere, and moving it into the shared dispatcher makes that one
  file carry more weight than its current one-case-per-line dispatch table.
- **No incremental rollout.** `Batch.runWriteItem`/`runGetItem` are deleted outright rather
  than deprecated-and-kept, so this lands as one atomic change — reasonable for a
  pre-release module with no external compatibility surface, but there's no intermediate
  state where both call shapes work side by side.

## Implementation notes

Two consequences surfaced during implementation that the design above didn't spell out
explicitly. Both are direct, logical entailments of the approved design — not new forks —
but are worth recording since they changed test *assertions*, not just call-site syntax.

1. **`interpreter.run(batchQuery)` now captures effect-level failures as a value even with
   no `retryPolicy` attached, including when called directly (not through `Batch`).**
   Previously, calling `interpreter.run(batchQuery)` directly (bypassing `Batch.runWriteItem`/
   `runGetItem`) propagated a client exception as a failed effect — only going through
   `Batch.runX` gave you the "capture as `Failed`" behavior. Since `runAny`'s batch cases now
   *always* wrap in `withRetryTracked` (this is exactly the "just give me the raw response"
   escape hatch removal called out in Cons above), a non-retryable exception is captured as
   `Batch.GetResult.Failed`/`WriteResult.Failed` unconditionally — there is no longer a way to
   get a batch query to fail its enclosing effect for a non-fatal error, regardless of retry
   policy. Two tests in `aws/src/test/scala/zio/dynamodb/BatchGetItemSpec.scala` and
   `BatchWriteItemSpec.scala` (`error propagation` suite) asserted the old "fails as an
   effect" behavior for exactly this direct-call path; both were rewritten to assert the new
   `Failed`-as-value behavior instead.
2. **The "wrong query type" defensive branch has no replacement, by design.** The old
   `Batch.runWriteItem`/`runGetItem` took a loosely-typed `DynamoDBQuery[Any, Response]`
   parameter and defended against a runtime type mismatch (reachable only via `asInstanceOf`)
   with an explicit `case other => Failed(IllegalArgumentException(...))` branch. `runAny`
   dispatches on concrete ADT case types the same way for every operation and has no
   equivalent guard anywhere else (a `GetItem` mistakenly cast to another Out type just runs
   as a `GetItem`) — so removing this asymmetric guard is itself part of "treat batch like
   everything else." The two tests in `core/src/test/scala/zio/dynamodb/BatchSpec.scala` that
   exercised this branch were deleted (they tested removed behavior) and replaced with two
   simpler tests exercising the empty-batch case via `interp.run` directly.

Full cross-build (`sbt +Test/compile`, Scala 2.13.18 and 3.3.8) and the full test suite
(`sbt test`, including Docker-backed `it` and `ceInterpreter` suites) pass: 591 core, 165
aws, 30 zio, 14 ce, 359 it — 0 failures.
