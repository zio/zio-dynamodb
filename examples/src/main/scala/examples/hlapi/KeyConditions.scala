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

package examples.hlapi

import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.blocks.ddbexpr.DdbKeyExpr
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * The three key-condition shapes: partition-key-only, `Composite` (partition + sort
 * equality — accepted by `get`/`update`/`deleteFrom`), and `Extended` (partition equality
 * plus a sort range — accepted only by `query`, since a range doesn't address a single item).
 */
object KeyConditions {

  // Partition-key-only table: no sort key field at all.
  final case class LogEntry(source: String, message: String)

  object LogEntry extends CompanionOptics[LogEntry] {
    implicit val schema: Schema[LogEntry] = Schema.derived

    val source: Lens[LogEntry, String]  = $(_.source)
    val message: Lens[LogEntry, String] = $(_.message)
  }

  val logEntries: Table[LogEntry] = Table[LogEntry]("log-entries")

  val partitionOnly: DdbKeyExpr.PartitionKeyEquals[LogEntry, String] = LogEntry.source.partitionKey === "svc-a"

  // Composite (partition + sort) key table.
  final case class Event(streamId: String, seq: Long, payload: String)

  object Event extends CompanionOptics[Event] {
    implicit val schema: Schema[Event] = Schema.derived

    val streamId: Lens[Event, String] = $(_.streamId)
    val seq: Lens[Event, Long]        = $(_.seq)
    val payload: Lens[Event, String]  = $(_.payload)
  }

  val events: Table[Event] = Table[Event]("events")

  val composite: DdbKeyExpr.Composite[Event, String, Long] =
    Event.streamId.partitionKey === "s1" && Event.seq.sortKey === 1L

  val extendedGt: DdbKeyExpr.Extended[Event, String]         = Event.streamId.partitionKey === "s1" && Event.seq.sortKey > 1L
  val extendedGte: DdbKeyExpr.Extended[Event, String]        =
    Event.streamId.partitionKey === "s1" && Event.seq.sortKey >= 1L
  val extendedLt: DdbKeyExpr.Extended[Event, String]         = Event.streamId.partitionKey === "s1" && Event.seq.sortKey < 100L
  val extendedLte: DdbKeyExpr.Extended[Event, String]        =
    Event.streamId.partitionKey === "s1" && Event.seq.sortKey <= 100L
  val extendedBetween: DdbKeyExpr.Extended[Event, String]    =
    Event.streamId.partitionKey === "s1" && Event.seq.sortKey.between(1L, 100L)
  val extendedBeginsWith: DdbKeyExpr.Extended[Event, String] =
    Event.streamId.partitionKey === "s1" && Event.payload.sortKey.beginsWith("order-")
}
