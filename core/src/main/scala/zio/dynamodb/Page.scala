/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
