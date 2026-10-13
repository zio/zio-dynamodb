# Migration trial: prototype HL API (TEMPORARY)

> **Temporary.** Exists only for the DX trial of branch `series/3.x_simple_error_model` in an
> external migration project. Delete this file once the trial is over (tracked in the
> zio-dynamodb 3.x TODO list). Not part of the published docs.

Design (`error_model_design.md` §9): **failures fail the effect** - for every operation and every
effect type - with a typed case only for the two *expected* AWS exceptions; every other failure is
the AWS SDK exception itself. Success types scale with the operation (`Unit` unless you ask for
more). The earlier `Either`-based design is preserved on `series/3.x_zio_error_channel_prototype`.

## Using the branch

```sh
git fetch origin
git checkout series/3.x_simple_error_model
sbt 'set ThisBuild / version := "3.0.0-errmodel-SNAPSHOT"' publishLocal    # +publishLocal for 2.13 too
```

Point the project's zio-dynamodb dependencies at `3.0.0-errmodel-SNAPSHOT`. The distinct version
avoids clobbering the regular `3.0.0-SNAPSHOT` in the local ivy cache.

## What changed vs `series/3.x`

| Call | `series/3.x` | This branch |
|---|---|---|
| HL `put` / `update` / `deleteFrom` | `F[Option[A]]` | `F[Unit]` |
| `... .where(cond)`, condition false | fails `F` with the raw SDK `ConditionalCheckFailedException` | fails `F` with `DynamoDBError.ItemError.ConditionFailed(message, item)` |
| existing item on a failed condition | only with `.returnValuesOnConditionCheckFailure(AllOld)`, on the raw SDK exception | **on by default** for HL `.where`: `ConditionFailed.item: Option[Item]` (raw `Item`); `.withoutItemOnConditionFailure` turns it off; inside transactions, on the failing action's `CancellationReason.item` |
| `... .returning(ReturnValues.X)` (HL, new) | `put(...).returns(X)` via implicit conversion; undecodable item silently `None` | `F[Option[Either[DecodingError, A]]]` |
| HL `get` | `F[Either[ItemError, A]]`, not found = `Left(ValueNotFound)` | `F[Option[Either[DecodingError, A]]]`: not found = `None` |
| `DynamoDBQuery.transactWriteItems(...)` (the transaction entry point, taking HL writes) | fails `F` with `TransactionCancelled` | unchanged |
| `CancellationReason.code` | `String` | sealed `CancellationCode` (`ConditionalCheckFailed`, `TransactionConflict`, `NoError`, ..., `Other(value)`) |
| `FutureInterpreter` failures | `java.util.concurrent.CompletionException` wrapping the SDK exception | the SDK exception itself (as CE / ZIO); `TransactionCancelled` translation now also works on `Future` |
| `query` / `scan`, `.execute[F]` | | unchanged |

Rejected at compile time (with a message saying what to use instead):

- `put(t, a).returns(...)` - use `.returning(...)`.
- `.returnValuesOnConditionCheckFailure(...)` on HL writes - it's on by default; remove the call (or use `.withoutItemOnConditionFailure`).

## Recipes: narrow handlers

Handle only the expected failure; everything else passes through untouched (and is never wrapped
or re-retried by app code):

```scala
// Cats Effect
put(table, a).where(cond).execute
  .flatMap(_ => onWritten)
  .recoverWith { case _: DynamoDBError.ItemError.ConditionFailed => onConditionFailed }

DynamoDBQuery.transactWriteItems(writes: _*).execute
  .flatMap(_ => onWritten)
  .recoverWith {
    case tc: DynamoDBError.TransactionError.TransactionCancelled
        if tc.reasons.exists(_.code == CancellationCode.ConditionalCheckFailed) => onConditionFailed
  }
// ZIO: the same with catchSome; Future: recover / recoverWith
```

| Before | After |
|---|---|
| `.execute.attempt.flatMap { ... case Left(error) => writeError(...) }` | `.execute.flatMap(...).recoverWith { case _: ConditionFailed => ... }` - no `.attempt`, no catch-all, no `reRaise` |
| `case _: ConditionalCheckFailedException =>` | `case _: DynamoDBError.ItemError.ConditionFailed =>` (see hazards) |
| `reasons.exists(_.code == "ConditionalCheckFailed")` | `reasons.exists(_.code == CancellationCode.ConditionalCheckFailed)` |
| `get(...)` then `case Left(_: ValueNotFound) => None` | `get(...)` is already `Option[Either[DecodingError, A]]` |
| `put(...).execute.void` | `put(...).execute` |
| `put(t, a).returns(ReturnValues.AllOld)` | `put(t, a).returning(ReturnValues.AllOld)` |
| `.where(c).returnValuesOnConditionCheckFailure(AllOld)` | `.where(c)` (on by default) |

`recoverWith` covers every step chained before it, not just the write - keep it next to the write
if later steps do real work.

## Watch for: compiles, then misbehaves at runtime

| Old code | Why it still compiles | Fix |
|---|---|---|
| `case _: ConditionalCheckFailedException =>` on a single conditional write | the SDK class still exists; the failure is now `ConditionFailed`, so the case **never matches** and the failure propagates | match `ConditionFailed` |
| `result == Right(a)` on a `get` | universal `==` compares `Option` to `Either` - always false | `result.contains(Right(a))` |
| a handler like `handleErrorWith { case ex: Exception => ... }` | `ConditionFailed` / `TransactionCancelled` extend `Throwable`, not `Exception` → `MatchError` if one reaches it | `case NonFatal(ex) =>` (or handle the condition failure before it) |

## Known gaps on the branch

- `ConditionFailed` does not yet expose the original SDK exception as `getCause`.
- `docs/reference/**`: only the "Conditional writes" sections and the transactions page are updated.

## What to record

1. Did the narrow-handler recipes read clearly, and remove `.attempt` / catch-all / `reRaise`?
2. Hits of each "watch for" pattern, and whether tests caught them.
3. Code removed (`ValueNotFound` → `None` handlers, `.void`) vs code added.
4. Compile errors hit and whether the messages pointed to the fix.

Design background (if available): `ice-docs/zd_3x/error_model_design.md` §9.
