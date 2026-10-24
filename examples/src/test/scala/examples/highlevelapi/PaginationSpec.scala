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
import examples.highlevelapi.Pagination.{ ErrorOrDecoded, Event }
import zio.{ Task, ZIO }
import zio.blocks.chunk.Chunk
import zio.dynamodb.{ DynamoDBQuery, Interpreter, Page, PrimaryKey }
import zio.test._

/** Runs `Pagination`'s streams against a stub interpreter that replays two pages - no network, no Docker. */
object PaginationSpec extends ZIOSpecDefault {

  private val e1 = Event("s1", 1L, "a")
  private val e2 = Event("s1", 2L, "b")
  private val e3 = Event("s1", 3L, "c")

  private def twoPages: List[Page[ErrorOrDecoded]] =
    List(
      Page(Chunk[ErrorOrDecoded](Right(e1), Right(e2)), Some(PrimaryKey("streamId" -> "s1", "seq" -> 2L)), 2, 2),
      Page(Chunk[ErrorOrDecoded](Right(e3)), None, 1, 1)
    )

  // Answers each run with the next canned page. Like a real interpreter, the request happens when the
  // effect runs, not when `run` builds it.
  private final class ReplayingInterpreter[F[_]](suspend: (() => Page[ErrorOrDecoded]) => F[Page[ErrorOrDecoded]])
      extends Interpreter[F] {
    private var remaining                              = twoPages
    var requests                                       = 0
    def run[Out](query: DynamoDBQuery[_, Out]): F[Out] =
      suspend { () =>
        requests += 1
        val next = remaining.head
        remaining = remaining.tail
        next
      }.asInstanceOf[F[Out]]
  }

  def spec = suite("Pagination")(
    test("the ZStream follows lastEvaluatedKey until it's None and emits every page's items") {
      val interpreter = new ReplayingInterpreter[Task](page => ZIO.succeed(page()))
      for {
        all <- Pagination.eventsZio("s1")(interpreter).runCollect
      } yield assertTrue(all.toList == List[ErrorOrDecoded](Right(e1), Right(e2), Right(e3)), interpreter.requests == 2)
    },
    test("the ZStream fetches only the pages it's pulled") {
      val interpreter = new ReplayingInterpreter[Task](page => ZIO.succeed(page()))
      for {
        firstTwo <- Pagination.eventsZio("s1")(interpreter).take(2).runCollect
      } yield assertTrue(firstTwo.size == 2, interpreter.requests == 1)
    },
    test("the fs2 Stream follows lastEvaluatedKey until it's None and emits every page's items") {
      val interpreter = new ReplayingInterpreter[IO](page => IO(page()))
      val all         = Pagination.eventsCE("s1")(interpreter).compile.toList.unsafeRunSync()
      assertTrue(all == List[ErrorOrDecoded](Right(e1), Right(e2), Right(e3)), interpreter.requests == 2)
    },
    test("the fs2 Stream fetches only the pages it's pulled") {
      val interpreter = new ReplayingInterpreter[IO](page => IO(page()))
      val firstTwo    = Pagination.eventsCE("s1")(interpreter).take(2).compile.toList.unsafeRunSync()
      assertTrue(firstTwo.size == 2, interpreter.requests == 1)
    }
  )
}
