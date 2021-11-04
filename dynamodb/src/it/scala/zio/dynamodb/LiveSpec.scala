package zio.dynamodb

import io.github.vigoo.zioaws.core.config
import zio.dynamodb.PartitionKeyExpression.PartitionKey

//import scala.collection.immutable.{ Map => ScalaMap }
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import zio._
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression._
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }
import zio.test.Assertion._
//import zio.test.environment.Live // still had trouble getting the test to sleep for real time without hanging
import zio.test.environment.TestEnvironment

object LiveSpec extends DefaultRunnableSpec {

  val layer     = http4s.default >>> config.default >>> dynamodb.live >>> DynamoDBExecutor.live
  val tableName = "zio-dynamodb-test"

  //  System.setProperty("sqlite4java.library.path", "lib")
  override def spec: ZSpec[TestEnvironment, Any] =
    suite("live test")(
//      testM("put and get item") {
//        (for {
//          // TODO(adam): do not sleep here, put this in a loop and wait for a good response then continue
//          _      <- putItem(tableName, Item("id" -> "first", "testName" -> "put and get item")).execute
//          result <- getItem(tableName, PrimaryKey("id" -> "first")).execute
//          _      <- deleteItem(tableName, PrimaryKey("id" -> "first")).execute
//        } yield assert(result)(equalTo(Some(Item("id" -> "first", "testName" -> "put and get item")))))
//          .provideCustomLayer(layer ++ TestEnvironment.live)
//      },
//      testM("scan table") {
//        (for {
//          _      <- (putItem(tableName, Item("id" -> "second1", "firstName" -> "avi")) *>
//                        putItem(tableName, Item("id" -> "second2", "firstName" -> "adam")) *>
//                        putItem(tableName, Item("id" -> "second3", "firstName" -> "john"))).execute
//          stream <- scanAllItem(tableName).execute
//          chunk  <- stream.runCollect
//          _      <- (deleteItem(tableName, PrimaryKey("id" -> "second1")) *>
//                        deleteItem(tableName, PrimaryKey("id" -> "second2")) *>
//                        deleteItem(tableName, PrimaryKey("id" -> "second3"))).execute
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
      testM("query table") {
        (for {
          _          <- putItem(tableName, Item("id" -> "third1", "firstName" -> "avi")).execute
          (chunk, _) <- querySomeItem(tableName, 10, $("firstName"))
                          .whereKey(PartitionKey("id") === "third1")
                          .execute

        } yield assert(chunk)(equalTo(Chunk(Item("firstName" -> "avi")))))
          .provideCustomLayer(layer ++ TestEnvironment.live)
      }
    )

}
