package zio.dynamodb

import zio.blocks.chunk.Chunk

/**
 * Result of a single `scanSome` or `querySome` operation.
 *
 * @param items            Items returned by this page (empty when `Select.Count` is used).
 * @param lastEvaluatedKey Pagination cursor; `None` on the final page.
 * @param count            Items matched after any filter expression — equals `items.size`
 *                         in the normal case; the meaningful value when `Select.Count` is set.
 * @param scannedCount     Items evaluated before applying any filter expression.
 *                         `scannedCount > count` indicates a filter-heavy query that reads
 *                         more items than it returns.
 */
final case class Page[A](
  items: Chunk[A],
  lastEvaluatedKey: LastEvaluatedKey,
  count: Int,
  scannedCount: Int
)
