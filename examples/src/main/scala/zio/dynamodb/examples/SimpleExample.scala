package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.{ DynamoDBExecutor, Item, PrimaryKey, TestDynamoDBExecutor }
import zio.ZIO
import zio.ZIOAppDefault
import zio.Console.printLine

object SimpleExample extends ZIOAppDefault {
  private val program: ZIO[DynamoDBExecutor with TestDynamoDBExecutor, Throwable, Unit] = for {
    _       <- TestDynamoDBExecutor.addTable("table1", partitionKey = "id")
    _       <- (putItem("table1", Item("id" -> 1, "name" -> "name1")) zip putItem(
                   "table1",
                   Item("id" -> 2, "name" -> "name2")
                 )).execute
    tuple   <- (getItem("table1", PrimaryKey("id" -> 1)) zip getItem("table1", PrimaryKey("id" -> 2))).execute
    _       <- printLine(s"found $tuple")
    stream1 <- scanAllItem("table1").execute
    xs      <- stream1.runCollect
    _       <- printLine(s"table scan results after 2 PutItems's: $xs")
    _       <- (deleteItem("table1", PrimaryKey("id" -> 1)) zip deleteItem("table1", PrimaryKey("id" -> 2))).execute
    stream2 <- scanAllItem("table1").execute
    xs2     <- stream2.runCollect
    _       <- printLine(s"table scan results after 2 DeleteItems's: $xs2")
  } yield ()

  override def run: ZIO[Any, Throwable, Unit] =
    program.provideLayer(DynamoDBExecutor.test)
}
