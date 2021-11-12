package zio.dynamodb

import io.github.vigoo.zioaws.core.config
import zio.clock.{ Clock }
//import zio.dynamodb.PartitionKeyExpression.PartitionKey
//import zio.dynamodb.SortKeyExpression.SortKey
//
//import scala.collection.immutable.{ Map => ScalaMap }
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import zio._
import zio.dynamodb.DynamoDBQuery._
//import zio.dynamodb.ProjectionExpression._
import zio.test.{ assert, DefaultRunnableSpec, TestResult, ZSpec }
import zio.test.Assertion._
//import zio.test.environment.Live // still had trouble getting the test to sleep for real time without hanging
import zio.test.environment._
import zio.duration._
import zio.console.putStrLn

object LiveSpec extends DefaultRunnableSpec {

  val layer = http4s.default >>> config.default >>> dynamodb.live >>> DynamoDBExecutor.live

  def insertPeople(tName: String)                                                                                     =
    putItem(tName, Item("id" -> "first", "firstName" -> "avi", "age" -> 1)) *>
      putItem(tName, Item("id" -> "second", "firstName" -> "adam", "age" -> 2)) *>
      putItem(tName, Item("id" -> "third", "firstName" -> "john", "age" -> 3))

  private def awaitTableCreation(tableName: String): ZIO[Has[DynamoDBExecutor] with Clock, Throwable, Option[String]] =
    describeTable(tableName).execute.retry(Schedule.spaced(2.seconds) && Schedule.recurs(5))

  private def managedTable() =
    ZManaged.make(
      for {
        tableNumber <- random.nextUUID
        tableName    = s"zio-dynamodb-$tableNumber"
        _           <- createTable(tableName, KeySchema("id", "age"), BillingMode.PayPerRequest)(
                         AttributeDefinition.attrDefnString("id"),
                         AttributeDefinition.attrDefnNumber("age")
                       ).execute
        _           <- putStrLn(s"awaiting table creation")
//        _           <- live(sleep(10.seconds))
        arn         <- awaitTableCreation(tableName)
        _           <- putStrLn(s"table created: $arn")
      } yield tableName
    ) { tName =>
      for {
        good <- deleteTable(tName).execute.tapError { a =>
                  println(s"delete error: $a")
                  ZIO.unit
                }.isSuccess
        _     = println(s"delete worked: $good")
        a    <- ZIO.unit
      } yield a
    }

  private def withTemporaryTable[A](f: String => ZIO[Has[DynamoDBExecutor], Throwable, TestResult]) =
    managedTable().use { table =>
      for {
        _      <- putStrLn("aquiring table")
        result <- f(table)
        _      <- putStrLn(s"used table")
      } yield result
    }

  //  System.setProperty("sqlite4java.library.path", "lib")
  override def spec: ZSpec[TestEnvironment, Any] =
    suite("live test")(
      testM("put and get item") {
        withTemporaryTable { name =>
          println(s"table name in test: $name")
          for {
            _      <- putItem(name, Item("id" -> "first", "testName" -> "put and get item")).execute
            result <- getItem(name, PrimaryKey("id" -> "first")).execute
            _       = println(s"result: $result")
          } yield assert(result)(equalTo(Some(Item("id" -> "first", "testName" -> "put and get item"))))
        }.provideLayer(layer ++ TestEnvironment.live)

//        (for {
//          _      <- createTestTable(tName).execute
//          _      <- putItem(tName, Item("id" -> "first", "testName" -> "put and get item")).execute
//          result <- getItem(tName, PrimaryKey("id" -> "first")).execute
//          _      <- deleteTable(tName).execute
//        } yield assert(result)(equalTo(Some(Item("id" -> "first", "testName" -> "put and get item")))))
//          .provideCustomLayer(layer ++ TestEnvironment.live)
      }
//      testM("scan table") {
//        val tName = "zio-dynamodb-test-2"
//        (for {
//          _      <- createTestTable(tName).execute
//          _      <- insertPeople(tName).execute
//          stream <- scanAllItem(tName).execute
//          chunk  <- stream.runCollect
//          _      <- deleteTable(tName).execute
//
//        } yield assert(chunk)(
//          equalTo(
//            Chunk(
//              Item("id" -> "second3", "firstName" -> "john"),
//              Item("id" -> "second2", "firstName" -> "adam"),
//              Item("id" -> "second1", "firstName" -> "avi")
//            )
//          )
//        ))
//          .provideCustomLayer(layer ++ TestEnvironment.live)
//      },
//      suite("query tables")(
//        testM("query table") {
//          val tName = "zio-dynamodb-test-3"
//          (for {
//            _          <- createTestTable(tName).execute
//            _          <- insertPeople(tName).execute
//            (chunk, _) <- querySomeItem(tName, 10, $("firstName"))
//                            .whereKey(PartitionKey("id") === "first" && SortKey("age") < 2)
//                            .execute
//            _          <- deleteTable(tName).execute
//
//          } yield assert(chunk)(
//            equalTo(Chunk(Item("firstName" -> "avi")))
//          ))
//            .provideCustomLayer(layer ++ TestEnvironment.live)
//        },
//        testM("query table greater than") {
//          val tName = "zio-dynamodb-test-4"
//          (for {
//            _          <- (putItem(tName, Item("id" -> "first", "firstName" -> "avi", "age" -> 1)) *>
//                              putItem(tName, Item("id" -> "first", "firstName" -> "adam", "age" -> 2)) *>
//                              putItem(tName, Item("id" -> "first", "firstName" -> "john", "age" -> 3))).execute
//            (chunk, _) <- querySomeItem(tName, 10, $("firstName"))
//                            .whereKey(PartitionKey("id") === "first" && SortKey("age") > 0)
//                            .execute
//
//          } yield assert(chunk)(
//            equalTo(Chunk(Item("firstName" -> "avi"), Item("firstName" -> "adam"), Item("firstName" -> "john")))
//          )) // REVIEW(john): somehow getting chunk out of bound exception with empty queries (when results are empty)
//            .provideCustomLayer(layer ++ TestEnvironment.live)
//        }
//      ),
//      suite("update items")(
//        testM("update name") {
//          val tName = "zio-dynamodb-test-5"
//          (for {
//            _       <- createTestTable(tName).execute
//            _       <- putItem(tName, Item("id" -> "update", "firstName" -> "adam")).execute
//            _       <- updateItem(tName, PrimaryKey("id" -> "update"))($("firstName").set("notAdam")).execute
//            updated <- getItem(
//                         tableName,
//                         PrimaryKey("id" -> "update")
//                       ).execute // TODO(adam): for some reason adding a projection expression here results in none
//            _       <- deleteItem(tableName, PrimaryKey("id" -> "update")).execute
//          } yield assert(updated)(equalTo(Some(Item("id" -> "update", "firstName" -> "notAdam")))))
//            .provideCustomLayer(layer ++ TestEnvironment.live)
//        }
//      )
    )

}
