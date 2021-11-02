package zio.dynamodb

import io.github.vigoo.zioaws.core.config

import scala.collection.immutable.{ Map => ScalaMap }
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import zio.Chunk
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.PartitionKeyExpression.PartitionKey
import zio.dynamodb.ProjectionExpression.$
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }
import zio.test.Assertion._
//import zio._
// import zio.test.environment.Live // this will allow me to use a real ZIO sleep
import zio.test.environment.TestEnvironment

object LiveSpec extends DefaultRunnableSpec {

//  System.setProperty("sqlite4java.library.path", "lib")
  override def spec: ZSpec[TestEnvironment, Any] =
    suite("live test")(
      testM("create table, put item, then retrieve") {
        val tableName = "zio-dynamodb-test"
        (for {
          // don't sleep, test to see if the table exists -- put that in a loop and then continue the test once
          // it returns
//          _      <- createTable(tableName, KeySchema("id"), BillingMode.PayPerRequest)(
//                      AttributeDefinition.attrDefnString("id")
//                    ).execute
          // REVIEW(john) Would like to sleep here for a few seconds???
          _      <- putItem(tableName, Item("id" -> "second", "firstName" -> "adam")).execute
          result <- getItem(tableName, PrimaryKey("id" -> "second")).execute
        } yield assert(result)(equalTo(Some(Item("id" -> "second", "firstName" -> "adam")))))
          .provideCustomLayer(layer ++ TestEnvironment.live)
      },
//      testM("scan table") {
//        val tableName2 = "zio-dynamodb-test-2"
//        (for {
////          _      <- createTable(tableName2, KeySchema("id"), BillingMode.PayPerRequest)(
////                      AttributeDefinition.attrDefnString("id")
////                    ).execute
//          _      <- (putItem(tableName2, Item("id" -> "first", "firstName" -> "avi")) *>
//                        putItem(tableName2, Item("id" -> "second", "firstName" -> "adam")) *>
//                        putItem(tableName2, Item("id" -> "third", "firstName" -> "john"))).execute
//          stream <- scanAllItem(tableName2).execute
//          chunk  <- stream.runCollect
//        } yield assert(chunk)(
//          // these are just out of order
//          equalTo(
//            Chunk(
//              Item("id" -> "first", "firstName"  -> "avi"),
//              Item("id" -> "second", "firstName" -> "adam"),
//              Item("id" -> "third", "firstName"  -> "john")
//            )
//          )
//        ))
//          .provideCustomLayer(layer ++ TestEnvironment.live)
//      },
      testM("query items") {
        val tableName3 = "zio-dynamodb-test-3"
        (for {
          _                 <- (putItem(tableName3, Item("id" -> "first", "firstName" -> "avi")) *>
                                   putItem(
                                     tableName3,
                                     Item("id" -> "third", "firstName" -> "john", "oth" -> ScalaMap("something" -> "cool"))
                                   )).execute
          (query, lastEval) <- querySomeItem(tableName3, 3, $("firstName"))
                                 .whereKey(PartitionKey("id") === "third")
                                 .execute
        } yield assert(query)(
          equalTo(Chunk(Item("id" -> "third", "firstName" -> "john", "oth" -> ScalaMap("something" -> "cool"))))
        ))
          .provideCustomLayer(layer ++ TestEnvironment.live)
      }
    )

  val layer = http4s.default >>> config.default >>> dynamodb.live >>> DynamoDBExecutor.live

}
