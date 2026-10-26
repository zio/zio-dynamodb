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

package examples.highlevelapi

import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{ DynamoDBError, DynamoDBQuery, Page }
import zio.dynamodb.blocks.ddbexpr.DdbKeyExpr
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * The three key-condition shapes: partition-key-only, `Composite` (partition + sort
 * equality — accepted by `get`/`update`/`deleteFrom`), and `KeyRange` (partition equality
 * plus a sort-key range — accepted only by `query`, since a range doesn't address a single item).
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

  val rangeGt: DdbKeyExpr.KeyRange[Event, String]         = Event.streamId.partitionKey === "s1" && Event.seq.sortKey > 1L
  val rangeGte: DdbKeyExpr.KeyRange[Event, String]        =
    Event.streamId.partitionKey === "s1" && Event.seq.sortKey >= 1L
  val rangeLt: DdbKeyExpr.KeyRange[Event, String]         = Event.streamId.partitionKey === "s1" && Event.seq.sortKey < 100L
  val rangeLte: DdbKeyExpr.KeyRange[Event, String]        =
    Event.streamId.partitionKey === "s1" && Event.seq.sortKey <= 100L
  val rangeBetween: DdbKeyExpr.KeyRange[Event, String]    =
    Event.streamId.partitionKey === "s1" && Event.seq.sortKey.between(1L, 100L)
  val rangeBeginsWith: DdbKeyExpr.KeyRange[Event, String] =
    Event.streamId.partitionKey === "s1" && Event.payload.sortKey.beginsWith("order-")

  // Wired into the six CRUD operations — see KeyConditionsSpec for these actually run. get/
  // update/deleteFrom need a PrimaryKey, so they use the Composite key; query alone can take
  // the KeyRange.
  val putQuery: DynamoDBQuery[Event, Unit] =
    put(events, Event("s1", 1L, "order-1"))

  val getQuery: DynamoDBQuery[Event, Option[Either[DynamoDBError.ItemError.DecodingError, Event]]] =
    get(events)(composite)

  val updateQuery: DynamoDBQuery[Event, Unit] =
    update(events)(composite)(Event.payload.set("order-2"))

  val deleteQuery: DynamoDBQuery[Event, Unit] =
    deleteFrom(events)(composite)

  val queryQuery: DynamoDBQuery[Event, Page[Either[DynamoDBError.ItemError, Event]]] =
    query(events, limit = 20).whereKey(rangeBetween)

  val scanQuery: DynamoDBQuery[Event, Page[Either[DynamoDBError.ItemError, Event]]] =
    scan(events, limit = 20).filter(Event.payload.attributeExists)
}
