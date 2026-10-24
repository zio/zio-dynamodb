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
import cats.effect.unsafe.implicits.global
import examples.highlevelapi.Pagination.{ Decoded, Event }
import zio.{ Task, ZIO }
import zio.blocks.chunk.Chunk
import zio.dynamodb.{ DynamoDBQuery, Interpreter, Page, PrimaryKey }
import zio.test._

/** Runs `Pagination`'s loops against a stub interpreter that replays two pages - no network, no Docker. */
object PaginationSpec extends ZIOSpecDefault {

  private val e1 = Event("s1", 1L, "a")
  private val e2 = Event("s1", 2L, "b")
  private val e3 = Event("s1", 3L, "c")

  private def twoPages: List[Page[Decoded]] =
    List(
      Page(Chunk[Decoded](Right(e1), Right(e2)), Some(PrimaryKey("streamId" -> "s1", "seq" -> 2L)), 2, 2),
      Page(Chunk[Decoded](Right(e3)), None, 1, 1)
    )

  // Answers each run with the next canned page, counting the requests.
  private final class ReplayingInterpreter[F[_]](lift: Page[Decoded] => F[Page[Decoded]]) extends Interpreter[F] {
    private var remaining                              = twoPages
    var requests                                       = 0
    def run[Out](query: DynamoDBQuery[_, Out]): F[Out] = {
      requests += 1
      val next = remaining.head
      remaining = remaining.tail
      lift(next).asInstanceOf[F[Out]]
    }
  }

  def spec = suite("Pagination")(
    test("the ZIO loop follows lastEvaluatedKey until it's None and keeps every page's items") {
      val interpreter = new ReplayingInterpreter[Task](p => ZIO.succeed(p))
      for {
        all <- Pagination.allEventsZio("s1")(interpreter)
      } yield assertTrue(all == Chunk[Decoded](Right(e1), Right(e2), Right(e3)), interpreter.requests == 2)
    },
    test("the Cats Effect loop does the same") {
      val interpreter = new ReplayingInterpreter[IO](p => IO.pure(p))
      val all         = Pagination.allEventsCE("s1")(interpreter).unsafeRunSync()
      assertTrue(all == Chunk[Decoded](Right(e1), Right(e2), Right(e3)), interpreter.requests == 2)
    }
  )
}
