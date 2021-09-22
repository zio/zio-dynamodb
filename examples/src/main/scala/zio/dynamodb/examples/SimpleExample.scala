package zio.dynamodb.examples

import zio.console.{ putStrLn, Console }
import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.fake.TestDynamoDBExecutor
import zio.dynamodb.fake.TestDynamoDBExecutor.TestDynamoDBExecutor
import zio.dynamodb.{ Item, PrimaryKey }
import zio.{ App, ExitCode, URIO, ZIO }

object SimpleExample extends App {
//  private val executorLayer = FakeDynamoDBExecutor.table("table1", pkFieldName = "id")().layer
  private val executorLayer = TestDynamoDBExecutor.test

  private val program: ZIO[Console with DynamoDBExecutor with TestDynamoDBExecutor, Exception, Unit] = for {
    _       <- TestDynamoDBExecutor.addTable("table1", pkFieldName = "id")()
    _       <- (putItem("table1", Item("id" -> 1, "name" -> "name1")) zip putItem(
                   "table1",
                   Item("id" -> 2, "name" -> "name2")
                 )).execute
    tuple   <- (getItem("table1", PrimaryKey("id" -> 1)) zip getItem("table1", PrimaryKey("id" -> 2))).execute
    _       <- putStrLn(s"found $tuple")
    stream1 <- scanAll("table1", indexName = "ignoredByFakeDb").execute
    xs      <- stream1.runCollect
    _       <- putStrLn(s"table scan results after 2 PutItems's: $xs")
    _       <- (deleteItem("table1", PrimaryKey("id" -> 1)) zip deleteItem("table1", PrimaryKey("id" -> 2))).execute
    stream2 <- scanAll("table1", indexName = "ignoredByFakeDb").execute
    xs2     <- stream2.runCollect
    _       <- putStrLn(s"table scan results after 2 DeleteItems's: $xs2")
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(executorLayer).exitCode
}
