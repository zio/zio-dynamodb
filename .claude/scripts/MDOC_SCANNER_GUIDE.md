# mdoc Code Block Scanner & Fixer Guide

Ported from zio-blocks' `.claude/scripts/` (originally written for zio-http's docs; the
decision tree and modifier semantics are generic mdoc knowledge, so this applies as-is to
zio-dynamodb's `docs/` tree, which uses the same `zio-sbt-website`/mdoc pipeline).

## Overview

`find-unmodified-mdoc-blocks.sh` identifies Scala code blocks in documentation that lack mdoc
modifiers.

## Why This Matters

Code blocks without mdoc modifiers are treated as **runnable code by default** during mdoc
compilation (`sbt docs/compileDocs` / `sbt docs/buildWebsite`). This means:

- ✅ **They will be compiled** against the actual project code
- ✅ **They will be executed** and output will be captured
- ❌ **If they fail to compile**, the entire docs build fails
- ❌ **If they have side effects**, those will execute during doc generation

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
