package zio.dynamodb

import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import zio.dynamodb.DynamoDBQuery.{ getItem, putItem }
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }
import zio.test.Assertion._
//import zio._
import zio.test.environment.TestEnvironment

object LiveSpec extends DefaultRunnableSpec {

  System.setProperty("sqlite4java.library.path", "lib")
  override def spec: ZSpec[TestEnvironment, Any] =
    suite("live test")(
      testM("create table and put item") {
        val tableName = "zio-dynamodb-test"
        (for {
//          _      <- createTable(tableName, KeySchema("id"), BillingMode.PayPerRequest)(
//                      AttributeDefinition.attrDefnString("id")
//                    ).execute
//          _      <- ZIO.effect(ZIO.succeed(Thread.sleep(5000)))
          _      <- putItem(tableName, Item("id" -> "first", "firstName" -> "adam")).execute
          result <- getItem(tableName, PrimaryKey("id" -> "first")).execute
        } yield assert(result)(equalTo(Some(Item("id" -> "first", "firstName" -> "adam")))))
          .provideCustomLayer(layer ++ TestEnvironment.live)
      }
    )

  val layer = http4s.default >>> config.default >>> dynamodb.live >>> DynamoDBExecutor.live

}
