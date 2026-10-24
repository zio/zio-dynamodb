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

import cats.effect.IO
import fs2.Stream
import zio.Task
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{ DynamoDBError, Interpreter, LastEvaluatedKey, PrimaryKey }
import zio.dynamodb.ExecuteSyntax._
import zio.dynamodb.blocks.ddbexpr.dsl._
import zio.stream.ZStream

/**
 * Reading every page of a `query` as a stream: each page's `lastEvaluatedKey` is passed to
 * `.startKey(...)` for the next page until it's `None`. Pages are fetched only as the stream is
 * consumed, so a large result never has to fit in memory. The same shape works for `scan`.
 */
object Pagination {

  final case class Event(streamId: String, seq: Long, payload: String)

  object Event extends CompanionOptics[Event] {
    implicit val schema: Schema[Event] = Schema.derived

    val streamId: Lens[Event, String] = $(_.streamId)
    val seq: Lens[Event, Long]        = $(_.seq)
  }

  val events: Table[Event] = Table[Event]("events")

  type ErrorOrDecoded = Either[DynamoDBError.ItemError, Event]

  private def page(streamId: String, pageSize: Int, startKey: LastEvaluatedKey) =
    query(events, limit = pageSize).whereKey(Event.streamId.partitionKey === streamId).startKey(startKey)

  // The state is the cursor to start from; `Some(next)` continues, `None` ends the stream.
  def eventsZio(streamId: String, pageSize: Int = 100)(implicit
    interpreter: Interpreter[Task]
  ): ZStream[Any, Throwable, ErrorOrDecoded] =
    ZStream
      .paginateZIO(Option.empty[PrimaryKey]) { startKey =>
        page(streamId, pageSize, startKey).execute.map(p => (p.items, p.lastEvaluatedKey.map(Option(_))))
      }
      .mapConcat(identity)

  def eventsCE(streamId: String, pageSize: Int = 100)(implicit
    interpreter: Interpreter[IO]
  ): Stream[IO, ErrorOrDecoded] =
    Stream
      .unfoldLoopEval(Option.empty[PrimaryKey]) { startKey =>
        page(streamId, pageSize, startKey).execute.map(p => (p.items, p.lastEvaluatedKey.map(Option(_))))
      }
      .flatMap(items => Stream.iterable(items))
}
