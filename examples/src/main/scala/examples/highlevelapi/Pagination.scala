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
import zio.{ Task, ZIO }
import zio.blocks.chunk.Chunk
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{ DynamoDBError, Interpreter, LastEvaluatedKey }
import zio.dynamodb.ExecuteSyntax._
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * Reading every page of a `query`: pass each page's `lastEvaluatedKey` to `.startKey(...)` on the
 * next call until it's `None`. The same loop works for `scan`; only `flatMap` and the "done" value
 * differ between ZIO and Cats Effect.
 */
object Pagination {

  final case class Event(streamId: String, seq: Long, payload: String)

  object Event extends CompanionOptics[Event] {
    implicit val schema: Schema[Event] = Schema.derived

    val streamId: Lens[Event, String] = $(_.streamId)
    val seq: Lens[Event, Long]        = $(_.seq)
  }

  val events: Table[Event] = Table[Event]("events")

  type Decoded = Either[DynamoDBError.ItemError, Event]

  private def page(streamId: String, pageSize: Int, startKey: LastEvaluatedKey) =
    query(events, limit = pageSize).whereKey(Event.streamId.partitionKey === streamId).startKey(startKey)

  def allEventsZio(streamId: String, pageSize: Int = 100)(implicit
    interpreter: Interpreter[Task]
  ): Task[Chunk[Decoded]] = {
    def loop(startKey: LastEvaluatedKey, acc: Chunk[Decoded]): Task[Chunk[Decoded]] =
      page(streamId, pageSize, startKey).execute.flatMap { p =>
        p.lastEvaluatedKey match {
          case None => ZIO.succeed(acc ++ p.items)
          case next => loop(next, acc ++ p.items)
        }
      }
    loop(None, Chunk.empty)
  }

  def allEventsCE(streamId: String, pageSize: Int = 100)(implicit
    interpreter: Interpreter[IO]
  ): IO[Chunk[Decoded]] = {
    def loop(startKey: LastEvaluatedKey, acc: Chunk[Decoded]): IO[Chunk[Decoded]] =
      page(streamId, pageSize, startKey).execute.flatMap { p =>
        p.lastEvaluatedKey match {
          case None => IO.pure(acc ++ p.items)
          case next => loop(next, acc ++ p.items)
        }
      }
    loop(None, Chunk.empty)
  }
}
