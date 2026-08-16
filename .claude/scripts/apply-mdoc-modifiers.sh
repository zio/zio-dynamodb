#!/bin/bash

# Script: Apply mdoc modifiers to unmodified Scala code blocks
# Purpose: Batch-fix Scala code blocks in documentation files
# Usage: ./.claude/scripts/apply-mdoc-modifiers.sh <file> <modifier> [--dry-run] [--start-line N] [--end-line N]
#
# Examples:
#   # Add :compile-only to all unmodified blocks in a file
#   ./apply-mdoc-modifiers.sh docs/index.md compile-only
#
#   # Dry run (show what would change without making changes)
#   ./apply-mdoc-modifiers.sh docs/index.md compile-only --dry-run
#
#   # Add :silent to blocks starting on lines within [100, 200] only
#   ./apply-mdoc-modifiers.sh docs/index.md silent --start-line 100 --end-line 200
#
# Ported from zio-blocks' .claude/scripts/ (originally added for zio-http's docs).
# Fixed here, both found by actually running this against zio-dynamodb's docs/index.md:
#   1. The original used `sed -i` directly, which errors out under BSD/macOS sed (no
#      in-place suffix argument) — this version writes to the temp file the script
#      already allocates and moves it into place, working under both GNU and BSD sed.
#   2. --start-line/--end-line were accepted as positional args but never actually
#      used anywhere in the sed call — silently a no-op regardless of what you passed.
#      This version parses them as named flags and scopes the sed substitution's
#      line address range accordingly.

set -e

FILE=""
MODIFIER=""
DRY_RUN=""
START_LINE=""
END_LINE=""

POSITIONAL=()
while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run)    DRY_RUN="1"; shift ;;
        --start-line) START_LINE="$2"; shift 2 ;;
        --end-line)   END_LINE="$2"; shift 2 ;;
        *)            POSITIONAL+=("$1"); shift ;;
    esac
done
FILE="${POSITIONAL[0]:-}"
MODIFIER="${POSITIONAL[1]:-}"

if [ -z "$FILE" ] || [ -z "$MODIFIER" ]; then
    echo "Usage: $(basename "$0") <file> <modifier> [--dry-run] [--start-line N] [--end-line N]"
    echo ""
    echo "Modifiers: compile-only, silent, reset, fail, silent:nest:1"
    echo ""
    echo "Examples:"
    echo "  $(basename "$0") docs/index.md compile-only"
    echo "  $(basename "$0") docs/index.md compile-only --dry-run"
    echo "  $(basename "$0") docs/index.md silent --start-line 100 --end-line 200"
    exit 1
fi

if [ ! -f "$FILE" ]; then
    echo "❌ File not found: $FILE"
    exit 1
fi

# Line-address range for the sed substitution: defaults to the whole file, narrowed
# if --start-line/--end-line were given (only affects blocks starting in-range).
ADDR_START="${START_LINE:-1}"
ADDR_END="${END_LINE:-\$}"
SED_ADDR="${ADDR_START},${ADDR_END}"

# Create temporary file
TEMP_FILE=$(mktemp)
trap "rm -f $TEMP_FILE" EXIT

# Count matches within the address range
if [ -n "$START_LINE" ] || [ -n "$END_LINE" ]; then
    MATCH_COUNT=$(sed -n "${SED_ADDR} s/^\`\`\`scala\$/&/p" "$FILE" | wc -l | tr -d ' ')
else
    MATCH_COUNT=$(grep -c "^\`\`\`scala$" "$FILE" || true)
fi

if [ "$MATCH_COUNT" -eq 0 ]; then
    echo "✓ No unmodified \`\`\`scala blocks found in $FILE${START_LINE:+ within lines $ADDR_START-$ADDR_END}"
    exit 0
fi

echo "📝 Processing: $FILE"
echo "   Modifier: mdoc:$MODIFIER"
if [ -n "$START_LINE" ] || [ -n "$END_LINE" ]; then
    echo "   Line range: $ADDR_START-$ADDR_END"
fi
echo "   Blocks found: $MATCH_COUNT"
echo ""

if [ -n "$DRY_RUN" ]; then
    echo "🔍 DRY RUN MODE - No changes will be made"
    echo ""
    echo "Changes that would be applied:"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    sed -n "${SED_ADDR} =" "$FILE" > "$TEMP_FILE" 2>/dev/null || true
    grep -n "^\`\`\`scala$" "$FILE" | while IFS=: read -r linenum _; do
        if [ "$linenum" -ge "$ADDR_START" ] 2>/dev/null && { [ -z "$END_LINE" ] || [ "$linenum" -le "$END_LINE" ]; }; then
            echo "  Line $linenum: \`\`\`scala  →  \`\`\`scala mdoc:$MODIFIER"
        fi
    done
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "To apply these changes, run without --dry-run:"
    echo "  ./.claude/scripts/apply-mdoc-modifiers.sh \"$FILE\" \"$MODIFIER\""
else
    # Apply the changes, scoped to the address range — write to a temp file and
    # move it into place, rather than sed -i, so this works identically under
    # GNU and BSD sed.
    sed "${SED_ADDR} s/^\`\`\`scala\$/\`\`\`scala mdoc:$MODIFIER/g" "$FILE" > "$TEMP_FILE"
    mv "$TEMP_FILE" "$FILE"

    echo "✅ Applied mdoc:$MODIFIER to $MATCH_COUNT code block(s)"
    echo "   File: $FILE"
    echo ""
    echo "🔍 Verify the changes:"
    echo "   git diff $FILE"
    echo ""
    echo "🧪 Test the docs build:"
    echo "   sbt \"docs/mdoc --in $FILE\""
fi
