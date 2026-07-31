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

import cats.effect.{ IO => CIO, Ref => CRef }
import cats.effect.unsafe.implicits.{ global => ceRuntime }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio._
import zio.blocks.chunk.Chunk
import zio.dynamodb.ProjectionExpression.$
import zio.test._
import zio.test.TestAspect

import scala.concurrent.ExecutionContext

object InterceptorSpec extends DynamoDBLocalSpec {

  // Base env — ZIO interpreter used only for test data setup.
  private val envLayer: URLayer[DynamoDbAsyncClient, DynamoDBEnv] =
    ZLayer(ZIO.serviceWith[DynamoDbAsyncClient](client => DynamoDBEnv(client, ZioInterpreter.fromAsyncClient(client))))

  private implicit val ec: ExecutionContext = ExecutionContext.global

  // -- Effect-system bridges --------------------------------------------------

  private def ceBridge(ceInterp: CEInterpreter): Interpreter[Task] =
    new Interpreter[Task] {
      def run[A](q: DynamoDBQuery[_, A]): Task[A] =
        ZIO.fromFuture(_ => ceInterp.run(q).unsafeToFuture()(ceRuntime))
    }

  private def futureBridge(interp: FutureInterpreter): Interpreter[Task] =
    new Interpreter[Task] {
      def run[A](q: DynamoDBQuery[_, A]): Task[A] =
        ZIO.fromFuture(_ => interp.run(q)).mapError {
          case e: java.util.concurrent.CompletionException if e.getCause != null => e.getCause
          case e                                                                 => e
        }
    }

  // -- Table helpers (same as InterceptorSpec original) ----------------------

  private def withInterceptingTable(
    f: (String, Interpreter[Task], DynamoDbAsyncClient) => ZIO[Any, Throwable, TestResult]
  ): ZIO[DynamoDBEnv & DynamoDbAsyncClient, Throwable, TestResult] =
    ZIO.scoped {
      for {
        env    <- ZIO.service[DynamoDBEnv]
        client <- ZIO.service[DynamoDbAsyncClient]
        table  <- managedTable(singleIdKeyTable)
        result <- f(table, env.interpreter, client)
      } yield result
    }

  private def withInterceptingYearTable(
    f: (String, Interpreter[Task], DynamoDbAsyncClient) => ZIO[Any, Throwable, TestResult]
  ): ZIO[DynamoDBEnv & DynamoDbAsyncClient, Throwable, TestResult] =
    ZIO.scoped {
      for {
        env    <- ZIO.service[DynamoDBEnv]
        client <- ZIO.service[DynamoDbAsyncClient]
        table  <- managedTable(idAndYearKeyTable)
        result <- f(table, env.interpreter, client)
      } yield result
    }

  // -- Accumulator factories --------------------------------------------------
  //
  // Each factory produces, for a given client, an intercepting Interpreter[Task]
  // and a Task that reads the metadata accumulated so far.

  private type AccFactory =
    DynamoDbAsyncClient => Task[(Interpreter[Task], Task[Chunk[DynamoDBResponseMetadata]])]

  private val zioAccFactory: AccFactory = client =>
    ZioResponseInterceptor.accumulating.map { acc =>
      val itInterp: Interpreter[Task] = ZioInterpreter.fromAsyncClient(client, acc.interceptor)
      (itInterp, acc.results)
    }

  // CE uses cats.effect.Ref (shared across unsafeToFuture() boundaries) rather
  // than IOLocal so metadata written inside ceBridge calls is visible when we
  // read results from a separate unsafeToFuture() call.
  private val ceAccFactory: AccFactory = client =>
    ZIO.fromFuture { _ =>
      CRef
        .of[CIO, List[DynamoDBResponseMetadata]](List.empty)
        .map { ref =>
          val interceptor: ResponseInterceptor[CIO]          = new ResponseInterceptor[CIO] {
            def onResponse(meta: DynamoDBResponseMetadata): CIO[Unit] = ref.update(meta :: _)
          }
          val readMeta: CIO[Chunk[DynamoDBResponseMetadata]] =
            ref.get.map(xs => Chunk.fromIterable(xs.reverse))
          (CEInterpreter.fromAsyncClient(client, interceptor), readMeta)
        }
        .unsafeToFuture()(ceRuntime)
    }.map { case (ceInterp, readMetaCIO) =>
      val itInterp: Interpreter[Task]                     = ceBridge(ceInterp)
      val readMeta: Task[Chunk[DynamoDBResponseMetadata]] =
        ZIO.fromFuture(_ => readMetaCIO.unsafeToFuture()(ceRuntime))
      (itInterp, readMeta)
    }

  private val futureAccFactory: AccFactory = client =>
    ZIO.fromFuture(_ => FutureResponseInterceptor.accumulating).map { acc =>
      val itInterp: Interpreter[Task]                     = futureBridge(FutureInterpreter.fromAsyncClient(client, acc.interceptor))
      val readMeta: Task[Chunk[DynamoDBResponseMetadata]] = ZIO.succeed(acc.results())
      (itInterp, readMeta)
    }

  // -- Generic interceptor test suite ----------------------------------------

  private def interceptorSuite(
    name: String,
    factory: AccFactory
  ): Spec[DynamoDBEnv & DynamoDbAsyncClient, Throwable] =
    suite(name)(
      test("getItem fires interceptor with correct tableName and PK") {
        withInterceptingTable { (table, plainInterp, asyncClient) =>
          val pk = PrimaryKey("id" -> "alice")
          for {
            pair <- factory(asyncClient)
            (itInterp, readMeta) = pair
            _    <- plainInterp.run(DynamoDBQuery.putItem(table, Item("id" -> "alice")))
            _    <- itInterp.run(DynamoDBQuery.getItem(table, pk))
            meta <- readMeta
          } yield {
            val m = meta(0).asInstanceOf[DynamoDBResponseMetadata.GetItem]
            assertTrue(
              meta.length == 1 &&
                m.tableName == table &&
                m.correlation.primaryKey.contains(pk)
            )
          }
        }
      },
      test("putItem fires interceptor with PutItem metadata; correlation.primaryKey is None") {
        withInterceptingTable { (table, _, asyncClient) =>
          for {
            pair <- factory(asyncClient)
            (itInterp, readMeta) = pair
            _    <- itInterp.run(DynamoDBQuery.putItem(table, Item("id" -> "bob")))
            meta <- readMeta
          } yield {
            val m = meta(0).asInstanceOf[DynamoDBResponseMetadata.PutItem]
            assertTrue(
              meta.length == 1 &&
                m.tableName == table &&
                m.correlation.primaryKey.isEmpty
            )
          }
        }
      },
      test("updateItem fires interceptor with UpdateItem metadata and PK in correlation") {
        withInterceptingTable { (table, plainInterp, asyncClient) =>
          val pk = PrimaryKey("id" -> "charlie")
          for {
            pair <- factory(asyncClient)
            (itInterp, readMeta) = pair
            _    <- plainInterp.run(DynamoDBQuery.putItem(table, Item("id" -> "charlie", "score" -> 0)))
            _    <- itInterp.run(DynamoDBQuery.updateItem(table, pk)($("score").set(1)))
            meta <- readMeta
          } yield {
            val m = meta(0).asInstanceOf[DynamoDBResponseMetadata.UpdateItem]
            assertTrue(
              meta.length == 1 &&
                m.tableName == table &&
                m.correlation.primaryKey.contains(pk)
            )
          }
        }
      },
      test("deleteItem fires interceptor with DeleteItem metadata and PK in correlation") {
        withInterceptingTable { (table, plainInterp, asyncClient) =>
          val pk = PrimaryKey("id" -> "dave")
          for {
            pair <- factory(asyncClient)
            (itInterp, readMeta) = pair
            _    <- plainInterp.run(DynamoDBQuery.putItem(table, Item("id" -> "dave")))
            _    <- itInterp.run(DynamoDBQuery.deleteItem(table, pk))
            meta <- readMeta
          } yield {
            val m = meta(0).asInstanceOf[DynamoDBResponseMetadata.DeleteItem]
            assertTrue(
              meta.length == 1 &&
                m.tableName == table &&
                m.correlation.primaryKey.contains(pk)
            )
          }
        }
      },
      test("scanSome fires interceptor with Scan metadata") {
        withInterceptingTable { (table, plainInterp, asyncClient) =>
          for {
            pair <- factory(asyncClient)
            (itInterp, readMeta) = pair
            _    <- plainInterp.run(DynamoDBQuery.putItem(table, Item("id" -> "s1")))
            _    <- plainInterp.run(DynamoDBQuery.putItem(table, Item("id" -> "s2")))
            _    <- itInterp.run(DynamoDBQuery.scanSome(table, limit = 10))
            meta <- readMeta
          } yield {
            val m = meta(0).asInstanceOf[DynamoDBResponseMetadata.Scan]
            assertTrue(meta.length == 1 && m.tableName == table)
          }
        }
      },
      test("querySome fires interceptor with Query metadata") {
        withInterceptingYearTable { (table, plainInterp, asyncClient) =>
          for {
            pair <- factory(asyncClient)
            (itInterp, readMeta) = pair
            _    <- plainInterp.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2024")))
            _    <- itInterp.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "alice")
                    )
            meta <- readMeta
          } yield {
            val m = meta(0).asInstanceOf[DynamoDBResponseMetadata.Query]
            assertTrue(meta.length == 1 && m.tableName == table)
          }
        }
      },
      test("batchGetItem fires interceptor with BatchGetItem metadata") {
        withInterceptingTable { (table, plainInterp, asyncClient) =>
          for {
            pair <- factory(asyncClient)
            (itInterp, readMeta) = pair
            _    <- plainInterp.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "v" -> 1)))
            _    <- plainInterp.run(DynamoDBQuery.putItem(table, Item("id" -> "b", "v" -> 2)))
            batch =
              DynamoDBQuery.batchGetItem(List("a", "b"))(id => DynamoDBQuery.getItem(table, PrimaryKey("id" -> id)))
            _    <- itInterp.run(batch)
            meta <- readMeta
          } yield assertTrue(
            meta.length == 1 &&
              meta(0).isInstanceOf[DynamoDBResponseMetadata.BatchGetItem]
          )
        }
      },
      test("batchWriteItem fires interceptor with BatchWriteItem metadata") {
        withInterceptingTable { (table, _, asyncClient) =>
          val items = List(Item("id" -> "x", "v" -> 1), Item("id" -> "y", "v" -> 2))
          for {
            pair <- factory(asyncClient)
            (itInterp, readMeta) = pair
            _    <- itInterp.run(DynamoDBQuery.batchWriteItem(items)(item => DynamoDBQuery.putItem(table, item)))
            meta <- readMeta
          } yield assertTrue(
            meta.length == 1 &&
              meta(0).isInstanceOf[DynamoDBResponseMetadata.BatchWriteItem]
          )
        }
      },
      test("accumulation: two operations produce two metadata entries in order") {
        withInterceptingTable { (table, _, asyncClient) =>
          val pk = PrimaryKey("id" -> "acc-test")
          for {
            pair <- factory(asyncClient)
            (itInterp, readMeta) = pair
            _    <- itInterp.run(DynamoDBQuery.putItem(table, Item("id" -> "acc-test")))
            _    <- itInterp.run(DynamoDBQuery.getItem(table, pk))
            meta <- readMeta
          } yield assertTrue(
            meta.length == 2 &&
              meta(0).isInstanceOf[DynamoDBResponseMetadata.PutItem] &&
              meta(1).isInstanceOf[DynamoDBResponseMetadata.GetItem]
          )
        }
      }
    )

  def spec = suite("Interceptor IT")(
    interceptorSuite("ZIO interpreter", zioAccFactory),
    interceptorSuite("CE interpreter", ceAccFactory),
    interceptorSuite("Future interpreter", futureAccFactory),
    test("no-interceptor baseline: plain interpreter produces no metadata") {
      withSingleIdKeyTable { (table, plainInterp) =>
        for {
          _ <- plainInterp.run(DynamoDBQuery.putItem(table, Item("id" -> "baseline")))
          r <- plainInterp.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "baseline")))
        } yield assertTrue(r.isDefined)
      }
    }
  ).provideSome[DynamoDbAsyncClient](envLayer) @@ TestAspect.sequential
}
