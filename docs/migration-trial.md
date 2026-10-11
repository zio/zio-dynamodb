# Migration trial: prototype HL API (TEMPORARY)

> **Temporary.** Exists only for the DX trial of branch `series/3.x_zio_error_channel_prototype`
> in an external migration project. Delete this file once the trial is over (tracked in the
> zio-dynamodb 3.x TODO list). Not part of the published docs.

This branch prototypes a High-Level (HL) API change: **return types scale with the complexity of
the operation**. Expected failures are part of the return type, decided by the kind of operation;
infrastructure failures (throttling, timeouts, SDK errors) stay in the failed effect. The
Low-Level (LL) API is unchanged.

## Using the branch

```sh
git fetch origin
git checkout series/3.x_zio_error_channel_prototype
sbt 'set ThisBuild / version := "3.0.0-errmodel-SNAPSHOT"' publishLocal    # +publishLocal for 2.13 too
```

Point the project's zio-dynamodb dependencies at `3.0.0-errmodel-SNAPSHOT`. The distinct version
avoids clobbering the regular `3.0.0-SNAPSHOT` in the local ivy cache.

## What changed (HL API, `import zio.dynamodb.blocks.ddbexpr.dsl._`)

| Call | Before | Now |
|---|---|---|
| `put` / `update` / `deleteFrom` | `F[Option[A]]` | `F[Unit]` |
| `... .where(cond)` | `F[Option[A]]`; a failed condition fails `F` with the raw SDK `ConditionalCheckFailedException` | `F[Either[ConditionFailed, Unit]]` |
| `... .where(cond).returnValuesOnConditionCheckFailure(AllOld)` | item only on the raw SDK exception | `ConditionFailed.item: Option[Item]` (raw `Item`, not decoded) |
| `... .returning(ReturnValues.X)` (new) | `put(...).returns(X)` via implicit conversion; undecodable item silently `None` | `F[Option[Either[DecodingError, A]]]`; with `.where`: `F[Either[ConditionFailed, Option[Either[DecodingError, A]]]]` |
| `get` | `F[Either[ItemError, A]]`, not found = `Left(ValueNotFound)` | `F[Option[Either[DecodingError, A]]]`: not found = `None` |
| `transactWriteItems(...)` from `dsl` (new) | n/a | `F[Either[TransactionCancelled, Unit]]` |
| `CancellationReason.code` | `String` | sealed `CancellationCode` (`ConditionalCheckFailed`, `TransactionConflict`, `NoError`, ..., `Other(value)`) |
| `query` / `scan` | `F[Page[Either[ItemError, A]]]` | unchanged |

Now rejected at compile time (with a message saying what to use instead):

- `put(t, a).returns(...)` - use `.returning(...)`.
- `put(t, a).returnValuesOnConditionCheckFailure(...)` without `.where` - add `.where(...)` first.

Unchanged: the LL API (`DynamoDBQuery.putItem/.where/...`, `DynamoDBQuery.transactWriteItems`,
which still returns `F[Unit]` and fails with `TransactionCancelled`).

## Migration recipes

| Before | After |
|---|---|
| `put(t, a).where(c).execute.attempt.map { case Left(e: ConditionalCheckFailedException) => ... }` | `put(t, a).where(c).execute.map { case Left(cf) => ... }` - infrastructure errors no longer reach this match |
| `DynamoDBQuery.transactWriteItems(...)` + catch `TransactionCancelled` | `transactWriteItems(...)` from `dsl` → `Either[TransactionCancelled, Unit]` |
| `reasons.exists(_.code == "ConditionalCheckFailed")` | `reasons.exists(_.code == CancellationCode.ConditionalCheckFailed)` |
| `get(...)` then `case Left(_: ValueNotFound) => None` | `get(...)` is already `Option[Either[DecodingError, A]]` |
| `put(...).execute.void` | `put(...).execute` |
| `put(t, a).returns(ReturnValues.AllOld)` | `put(t, a).returning(ReturnValues.AllOld)` |

`get`, `query` and HL `transactWriteItems` return a plain `DynamoDBQuery`, so `.execute` on them
needs `import zio.dynamodb.ExecuteSyntax._`; the write builders have `.execute` built in.

**Tagless final: drop the explicit effect type.** `.execute[F]` no longer yields `F[Out]` - it
resolves to the explicit-interpreter overload and becomes a function `Interpreter[F] => F[Out]`,
so `.execute[F].attempt` / `.flatMap` fail to compile with a confusing message. Write `.execute`;
the `Interpreter[F]` in implicit scope still gives `F[Out]`.

## Watch for: compiles, then fails at runtime

Seen while migrating this repo's own tests - check every hit:

| Old code | Why it still compiles | Fix |
|---|---|---|
| `....where(...).execute.attempt` then `.isLeft` | `.attempt` works on any `F`; a failed condition is now `Right(Left(cf))` | drop `.attempt` |
| `interpreter.run(....where(...)).either` then `isLeft` | same | drop `.either` |
| `result == Right(a)` on a `get` | universal `==` compares `Option` to `Either` - always false | `result.contains(Right(a))` |

## Known gaps on the branch

- `docs/reference/**` not updated: the HL "Conditional writes" section still describes the old
  behavior (raw `ConditionalCheckFailedException`).
- LL API unchanged (parity is an open decision).
- The typed ZIO interpreter (`ZioInterpreter.fromAsyncClientWithTypedErrors`, `IO[DynamoFailure, _]`)
  is opt-in and irrelevant to a Cats Effect project.

## What to record

1. Did the old catch-all over expected + infrastructure failures (the `WriteError` retry bug)
   become a compile error or an obviously separate branch?
2. Hits of each "compiles, then fails at runtime" pattern, and whether tests caught them.
3. Code removed (`ValueNotFound` → `None` handlers, `.void`, hand-rolled cancellation wrapping)
   vs code added.
4. Compile errors hit and whether the messages pointed to the fix.
5. Any LL call sites that would have benefited from the same change.

Design background (if available): `ice-docs/zd_3x/error_model_design.md` §6-7.
