# mdoc Code Block Scanner & Fixer Guide

Ported from zio-blocks' `.claude/scripts/` (originally written for zio-http's docs; the
decision tree and modifier semantics below are generic mdoc knowledge). **One premise did
NOT carry over and is corrected below** — verify it empirically again if this guide is ever
ported to a third project, since it depends on that project's specific mdoc/plugin setup.

## Overview

`find-unmodified-mdoc-blocks.sh` identifies Scala code blocks in documentation that lack mdoc
modifiers.

## Why This Matters — corrected for zio-dynamodb's actual mdoc behavior

The zio-blocks/zio-http version of this guide claimed a bare ` ```scala ` fence (no modifier)
is compiled *and executed* by mdoc by default. **That is not true for zio-dynamodb's own
`docs/compileDocs`/`docs/mdoc` setup** — verified empirically (2026-08-16, `series/3.x`): a
bare ` ```scala ` fence in `docs/index.md` passed through byte-identical, in 0.14s (i.e. never
even attempted), while the same block with the fence changed to ` ```scala mdoc ` took 3.5s
and genuinely type-checked (0 errors). **In this project, a fence needs the literal `mdoc`
token to be processed at all — a bare `scala` fence is inert.**

This flips the actual risk from what the original guide describes:

- ❌ **NOT the risk here**: an unmodified block breaking the docs build if it fails to compile
  or has side effects — it can't, because mdoc never touches it.
- ✅ **The actual risk**: a *real, meant-to-compile* example (a full worked program, not
  pseudocode) sitting in an unmodified fence is **silently unverified** — it can drift out of
  sync with the real API (a renamed method, a changed signature) and nothing will ever catch
  it, since mdoc skips it entirely on every future doc build too.

So for zio-dynamodb, the fix for a real (not illustrative) example isn't "leave it alone
since it's not breaking anything" — it's "give it the `mdoc` token (bare `mdoc` or
`mdoc:compile-only`) specifically *to start verifying it*," the opposite instinct from a
project where bare fences are checked by default. The decision tree below (which modifier,
once you've decided a block should be checked) is still accurate and portable; it's *whether
an unmodified block is currently being checked at all* that differs per project.

This is powerful for code examples that should actually run, but problematic for:
- Illustrative code that can't compile standalone
- Code that would have unwanted side effects (e.g. real DynamoDB calls against a live table)
- Architecture/pattern examples that are conceptual

## Quick Start

```bash
# Find all unmodified blocks in docs/
./.claude/scripts/find-unmodified-mdoc-blocks.sh docs

# Show code context (surrounding code in the file)
./.claude/scripts/find-unmodified-mdoc-blocks.sh docs --show-context
```

## Decision Tree: Which Modifier to Use

### Question 1: Should this code execute?

**YES (Code should actually run)**
- Use **no modifier** or **`:silent`**
- `:silent` hides the output (good if output is verbose or not useful)
- No modifier shows output (good for demonstrating behavior)

**NO (Code is illustrative, can't/shouldn't compile)**
- Go to Question 2

### Question 2: Why can't it compile?

**Imports/types undefined** (references external API, complex setup)
- Use **`:compile-only`**
- Indicates "this is pseudocode showing the pattern, not real code"

**Needs setup code first** (needs variables from previous examples)
- Previous example: **`:silent`** (hidden setup)
- Main example: **no modifier** (show the actual code you care about)
- Between them: add **`:reset`** to clear scope if next section is unrelated

**Should fail to compile** (anti-pattern, what NOT to do)
- Use **`:fail`**
- Compiler error becomes part of the documentation

**Complex initialization** (multiple lines of setup before actual example, e.g. constructing a
`DynamoDBExecutor` layer or a `Schema`)
- Setup block: **`:silent:nest:1`** (silent + nested in function)
- Main block: **`:reset`** then show the real code

## What to Look For

```scala
// ❌ This needs :compile-only — references an executor/table context that
// isn't set up in this snippet
import zio.dynamodb._

DynamoDBQuery.get(primaryKey).execute
```

```scala
// ✅ This is okay without a modifier (self-contained, compiles)
val x = 42
val y = x * 2
println(y)  // Output: 84
```

```scala
// ⚠️ Needs :silent:nest:1 (setup code with a side effect — a real table write)
val layer = DynamoDBExecutor.live
DynamoDBQuery.put(item).execute.provideLayer(layer)
```

## Fixing Strategies

### Strategy 1: Add :compile-only (Safest)

When in doubt, use `:compile-only`. It tells mdoc:
- "Try to compile this"
- "Don't execute it"
- "It might reference undefined things, that's okay"

```markdown
Before:
\`\`\`scala
import zio.dynamodb._
DynamoDBQuery.get(primaryKey).execute
\`\`\`

After:
\`\`\`scala mdoc:compile-only
import zio.dynamodb._
DynamoDBQuery.get(primaryKey).execute
\`\`\`
```

### Strategy 2: Make It Self-Contained (Better)

Rewrite the example to be standalone (no external dependencies):

```markdown
Before:
\`\`\`scala
val result = DynamoDBQuery.get(primaryKey).execute
\`\`\`

After (compile-only, shows API usage):
\`\`\`scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ProjectionExpression.$

val primaryKey = $("id").partitionKey === "some-id"
val result     = DynamoDBQuery.get(primaryKey).execute
\`\`\`
```

### Strategy 3: Use :silent for Hidden Setup

When you need to set up state that's not interesting to show:

```markdown
\`\`\`scala mdoc:silent
val data = loadSomeData()  // Boring setup, hide it
\`\`\`

Then show the interesting part:
\`\`\`scala
val result = data.map(transform)
println(result)  // Shows output
\`\`\`
```

## Scanning Results Interpretation

For a file with many unmodified blocks, check:
1. Are these blocks meant to illustrate patterns?
2. Do they reference external types (an executor, a live table, a `Schema`)?
3. Would adding all necessary imports/setup make them unwieldy?

**If YES to any**: Mark as **`:compile-only`**
**If NO to all**: They should execute → leave as-is

## Common Pitfalls

### Pitfall 1: Unqualified References

```scala
// ❌ This will fail - 'executor' is undefined
val result = query.execute.provide(executor)

// ✅ Fix with :compile-only
\`\`\`scala mdoc:compile-only
val result = query.execute.provide(executor)  // Assumes 'executor' from context
\`\`\`
```

### Pitfall 2: Side Effects

```scala
// ❌ This will execute during doc generation — a real write against a live table!
\`\`\`scala
DynamoDBQuery.put(Item("id" -> "1")).execute
\`\`\`

// ✅ Hide with :silent:nest:1, or make it :compile-only if it can't run standalone
\`\`\`scala mdoc:compile-only
DynamoDBQuery.put(Item("id" -> "1")).execute
\`\`\`
```

### Pitfall 3: Multi-block Examples

```scala
// Block 1: Setup
\`\`\`scala mdoc:silent
val data = List(1, 2, 3)
\`\`\`

// Block 2: Use data (references 'data' from Block 1)
\`\`\`scala
val sum = data.sum
println(sum)  // Output: 6
\`\`\`

// Block 3: Unrelated, needs fresh scope
\`\`\`scala mdoc:reset
val x = 42  // Doesn't see 'data' anymore
println(x)
\`\`\`
```

## Testing Your Fixes

After adding modifiers, verify docs compile:

```bash
sbt "docs/mdoc --in docs/index.md"
```

Should show: `Compiled in X.XXs (0 errors)`

If there are still errors, the modifier didn't fix the issue — likely needs more setup or
`:fail`.
