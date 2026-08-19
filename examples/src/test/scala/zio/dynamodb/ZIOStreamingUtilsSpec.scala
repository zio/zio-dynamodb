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

import zio._
import zio.stream.ZStream
import zio.blocks.chunk.Chunk
import zio.test._

import scala.collection.immutable.{ Map => ScalaMap }
import scala.concurrent.duration.{ FiniteDuration, MILLISECONDS }

object ZIOStreamingUtilsSpec extends ZIOSpecDefault {

  private def makeInterp(
    batchGetResponses: List[DynamoDBQuery.BatchGetItem.Response] = Nil,
    batchGetItemEffect: Option[Task[DynamoDBQuery.BatchGetItem.Response]] = None
  ): ZIO[Any, Nothing, AwsInterpreter[Task]] =
    Ref.make(batchGetResponses).map { getRef =>
      new AwsInterpreter[Task] {
        private[dynamodb] def pure[A](a: A): Task[A]                                    = ZIO.succeed(a)
        private[dynamodb] def map[A, B](fa: Task[A])(f: A => B): Task[B]                = fa.map(f)
        private[dynamodb] def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B]      = fa.flatMap(f)
        protected def product[A, B](fa: Task[A], fb: Task[B]): Task[(A, B)]             = fa.zip(fb)
        protected def productPar[A, B](fa: Task[A], fb: Task[B]): Task[(A, B)]          = fa.zipPar(fb)
        protected def fail[A](e: DynamoDBError): Task[A]                                = ZIO.fail(e)
        protected def absolve[A](fa: Task[Either[DynamoDBError.ItemError, A]]): Task[A] =
          fa.flatMap(ZIO.fromEither(_))

        private[dynamodb] def sleep(d: FiniteDuration): Task[Unit]                =
          ZIO.sleep(zio.Duration.fromScala(d))
        private[dynamodb] def attempt[A](fa: Task[A]): Task[Either[Throwable, A]] = fa.either
        private[dynamodb] def raiseError[A](t: Throwable): Task[A]                = ZIO.fail(t)

        protected def runGetItem(q: DynamoDBQuery.GetItem): Task[Option[Item]]                          = ZIO.succeed(None)
        protected def runPutItem(q: DynamoDBQuery.PutItem): Task[Option[Item]]                          = ZIO.succeed(None)
        protected def runUpdateItem(q: DynamoDBQuery.UpdateItem): Task[Option[Item]]                    = ZIO.succeed(None)
        protected def runDeleteItem(q: DynamoDBQuery.DeleteItem): Task[Option[Item]]                    = ZIO.succeed(None)
        protected def runQuerySomeItem(q: DynamoDBQuery.QuerySome): Task[Page[Item]]                    =
          ZIO.succeed(Page(Chunk.empty, None, 0, 0))
        protected def runScanSome(q: DynamoDBQuery.ScanSome): Task[Page[Item]]                          =
          ZIO.succeed(Page(Chunk.empty, None, 0, 0))
        protected def runCreateTable(q: DynamoDBQuery.CreateTable): Task[Unit]                          = ZIO.succeed(())
        protected def runDeleteTable(q: DynamoDBQuery.DeleteTable): Task[Unit]                          = ZIO.succeed(())
        protected def runDescribeTable(
          q: DynamoDBQuery.DescribeTable
        ): Task[DynamoDBQuery.DescribeTableResponse]                                                    =
          ZIO.succeed(DynamoDBQuery.DescribeTableResponse("arn:stub", DynamoDBQuery.TableStatus.Active, 0L, 0L))
        protected def runBatchGetItem(
          q: DynamoDBQuery.BatchGetItem
        ): Task[DynamoDBQuery.BatchGetItem.Response]                                                    =
          batchGetItemEffect.getOrElse(
            getRef.modify {
              case head :: tail => (head, tail)
              case Nil          => (DynamoDBQuery.BatchGetItem.Response(), Nil)
            }
          )
        protected def runBatchWriteItem(
          q: DynamoDBQuery.BatchWriteItem
        ): Task[DynamoDBQuery.BatchWriteItem.Response]                                                  =
          ZIO.succeed(DynamoDBQuery.BatchWriteItem.Response(None))
        protected def runTransactGetItems(q: DynamoDBQuery.TransactGetItems): Task[Chunk[Option[Item]]] =
          ZIO.succeed(Chunk.fill(q.getItems.length)(None))
        protected def runTransactWriteItems(q: DynamoDBQuery.TransactWriteItems): Task[Unit]            =
          ZIO.unit
      }
    }

  private def completeResponse(tableName: String, items: Item*): DynamoDBQuery.BatchGetItem.Response =
    DynamoDBQuery.BatchGetItem.Response(
      responses = items.foldLeft(MapOfSet.empty[String, Item])((acc, i) => acc + (tableName -> i))
    )

  private def incompleteResponse(
    tableName: String,
    processed: Seq[Item],
    unprocessedPks: Set[PrimaryKey]
  ): DynamoDBQuery.BatchGetItem.Response =
    DynamoDBQuery.BatchGetItem.Response(
      responses = processed.foldLeft(MapOfSet.empty[String, Item])((acc, i) => acc + (tableName -> i)),
      unprocessedKeys = ScalaMap(
        tableName -> DynamoDBQuery.BatchGetItem.TableGet(
          keysSet = unprocessedPks,
          projectionExpressionSet = Set.empty
        )
      )
    )

  def spec = suite("ZIOStreamingUtilsSpec")(
    test("emits all items from a single Complete batch") {
      val pks   = List("a", "b", "c").map(id => PrimaryKey("id" -> id))
      val items = List("a", "b", "c").map(id => Item("id" -> id))
      for {
        interp <- makeInterp(batchGetResponses = List(completeResponse("t", items: _*)))
        result <- ZIOStreamingUtils.batchGetItems(interp, "t")(ZStream.fromIterable(pks)).runCollect
      } yield assertTrue(result.toSet == items.toSet)
    },

    test("paginates across two batches when input exceeds batch size") {
      val ids    = (1 to 150).map(i => s"k$i")
      val pks    = ids.map(id => PrimaryKey("id" -> id))
      val batch1 = ids.take(100).map(id => Item("id" -> id))
      val batch2 = ids.drop(100).map(id => Item("id" -> id))
      for {
        interp <- makeInterp(batchGetResponses =
                    List(
                      completeResponse("t", batch1: _*),
                      completeResponse("t", batch2: _*)
                    )
                  )
        result <- ZIOStreamingUtils.batchGetItems(interp, "t")(ZStream.fromIterable(pks)).runCollect
      } yield assertTrue(result.size == 150)
    },

    test("emits processed items and logs warning when batch is Incomplete") {
      val processed   = Item("id" -> "a")
      val unprocessed = PrimaryKey("id" -> "b")
      for {
        interp <- makeInterp(batchGetResponses =
                    List(
                      incompleteResponse("t", List(processed), Set(unprocessed))
                    )
                  )
        result <- ZIOStreamingUtils
                    .batchGetItems(interp, "t")(
                      ZStream(PrimaryKey("id" -> "a"), unprocessed)
                    )
                    .runCollect
        logs   <- ZTestLogger.logOutput
      } yield assertTrue(
        result.toSet == Set(processed) &&
          logs.exists(e => e.logLevel == LogLevel.Warning && e.message().contains("unprocessed"))
      )
    },

    test("emits nothing and logs error when batch effect fails, then continues with next batch") {
      val calls = new java.util.concurrent.atomic.AtomicInteger(0)
      for {
        interp <- makeInterp(
                    batchGetItemEffect = Some(
                      ZIO.succeed(calls.incrementAndGet()).flatMap { n =>
                        if (n == 1) ZIO.fail(new RuntimeException("ThrottlingException"))
                        else ZIO.succeed(completeResponse("t", Item("id" -> "x")))
                      }
                    )
                  )
        // Two batches: first fails, second succeeds
        result <- ZIOStreamingUtils
                    .batchGetItems(interp, "t")(
                      ZStream.fromIterable(
                        (1 to 101).map(i => PrimaryKey("id" -> s"k$i"))
                      )
                    )
                    .runCollect
        logs   <- ZTestLogger.logOutput
      } yield assertTrue(
        result.toSet == Set(Item("id" -> "x")) &&
          logs.exists(e => e.logLevel == LogLevel.Error && e.message().contains("ThrottlingException"))
      )
    }
  )
}
