# Documentation Scripts for zio-dynamodb

Utility scripts for documentation maintenance and quality assurance, ported from zio-blocks'
`.claude/scripts/` (originally written for zio-http's docs — the tooling is generic to any
`zio-sbt-website`/mdoc-based docs pipeline, which zio-dynamodb also uses).

## mdoc Code Block Scanner & Fixer Tools

### Overview

Two scripts plus a decision guide for managing mdoc modifiers in Scala code blocks:

1. **find-unmodified-mdoc-blocks.sh** — Scan and report
2. **apply-mdoc-modifiers.sh** — Batch apply modifiers
3. **MDOC_SCANNER_GUIDE.md** — Decision guide

### Problem They Solve

Code blocks without mdoc modifiers are compiled and executed by mdoc during
`sbt docs/compileDocs` / `sbt docs/buildWebsite`. This causes:

- ✅ **Good for**: Real examples that demonstrate working code
- ❌ **Bad for**: Illustrative pseudocode, code with side effects (e.g. real DynamoDB calls),
  incomplete examples

These scripts help identify and fix blocks that need modifiers.

---

## Script 1: find-unmodified-mdoc-blocks.sh

**Purpose**: Identify all Scala code blocks lacking mdoc directives

**Usage**:
```bash
# Basic scan
./.claude/scripts/find-unmodified-mdoc-blocks.sh docs

# Show context around each block
./.claude/scripts/find-unmodified-mdoc-blocks.sh docs --show-context
```

**What It Does**:
- Finds all `\`\`\`scala$` patterns (Scala blocks without modifiers)
- Shows file and line number for each
- Optionally shows surrounding code context
- Lists common modifiers and decision guide

---

## Script 2: apply-mdoc-modifiers.sh

**Purpose**: Batch-apply mdoc modifiers to unmodified blocks

**Usage**:
```bash
# Dry run (show what would change)
./.claude/scripts/apply-mdoc-modifiers.sh docs/index.md compile-only --dry-run

# Apply modifier to all blocks in file
./.claude/scripts/apply-mdoc-modifiers.sh docs/index.md compile-only

# Apply to specific line range
./.claude/scripts/apply-mdoc-modifiers.sh docs/index.md silent --start-line 100 --end-line 200
```

**Supported Modifiers**:
- `compile-only` — Compile but don't execute
- `silent` — Compile and run, hide output
- `reset` — Clear scope (between unrelated examples)
- `fail` — Code should fail to compile (anti-pattern)
- `silent:nest:1` — Silent execution, nested in function

**Example**:

Before:
```markdown
\`\`\`scala
val result = DynamoDBQuery.get(primaryKey).execute
\`\`\`
```

After applying `compile-only`:
```markdown
\`\`\`scala mdoc:compile-only
val result = DynamoDBQuery.get(primaryKey).execute
\`\`\`
```

Note: this port fixes a bug in the original zio-blocks script — it used `sed -i` directly,
which errors out under BSD/macOS `sed` (no in-place suffix argument supplied). This version
writes to the temp file the script already allocates and moves it into place, which works
under both GNU and BSD `sed`.

---

## Script 3: MDOC_SCANNER_GUIDE.md

**Purpose**: Comprehensive guide for deciding which modifier to use

**Key Decision Tree**:

1. **Should code execute?**
   - YES → Use no modifier or `:silent`
   - NO → Go to next question

2. **Why can't it compile?**
   - References external types (executor, live table, `Schema`) → Use `:compile-only`
   - Needs setup first → Use `:silent` before, `:reset` between sections
   - Should intentionally fail → Use `:fail`
   - Complex initialization → Use `:silent:nest:1` for setup

**Example Scenarios**:

| Scenario | Modifier | Reason |
|----------|----------|--------|
| Working code example | (none) | Demonstrates real behavior |
| API usage illustration | `:compile-only` | Shows pattern, references external context |
| Setup/initialization | `:silent` | Hidden boilerplate |
| Anti-pattern example | `:fail` | Demonstrates what NOT to do |
| Multiple independent blocks | `:reset` | Clears state between examples |

---

## Suggested Workflow

### Phase 1: Scan
```bash
./.claude/scripts/find-unmodified-mdoc-blocks.sh docs --show-context
```

### Phase 2: Analyze
For each block, read `MDOC_SCANNER_GUIDE.md` to determine the right modifier:
- Is it illustrative or executable?
- Does it reference external types?
- Would it have side effects if executed?

### Phase 3: Apply
```bash
# Dry run first
./.claude/scripts/apply-mdoc-modifiers.sh docs/index.md compile-only --dry-run

# Review changes
git diff docs/index.md

# Apply
./.claude/scripts/apply-mdoc-modifiers.sh docs/index.md compile-only
```

### Phase 4: Verify
```bash
# Test docs compilation
sbt "docs/mdoc --in docs/index.md"

# Should output: Compiled in X.XXs (0 errors)
```

---

## Reference: All mdoc Modifiers

| Modifier | Behavior | Use Case |
|----------|----------|----------|
| (none) | Compile + Execute + Show output | Real working examples |
| `:compile-only` | Compile only, no execution | Illustrative pseudocode |
| `:silent` | Compile + Execute, hide output | Setup code, verbose output |
| `:reset` | Clear previous scope | New section, unrelated examples |
| `:fail` | Should fail to compile | Anti-patterns, wrong approaches |
| `:nest:1` | Nest in outer scope | Helper functions |
| `:nest:1:reset` | Combination | Complex setup |

---

## For Documentation Contributors

When writing new code examples:

1. **Self-contained examples** → Plain `` ```scala ``
2. **Illustrative patterns** → `` ```scala mdoc:compile-only ``
3. **Hidden setup** → `` ```scala mdoc:silent ``
4. **Between sections** → Use `` ```scala mdoc:reset ``

Ask yourself: **"Would someone copy-paste this code and run it against a real table?"**
- YES → Plain block
- NO → Add appropriate modifier
