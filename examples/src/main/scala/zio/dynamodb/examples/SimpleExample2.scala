package zio.dynamodb.examples

import zio.console.{ putStrLn, Console }
import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.{ Item, PrimaryKey }
import zio.dynamodb.fake.FakeDynamoDBExecutor
import zio.schema.{ DeriveSchema, Schema }
import zio.{ App, ExitCode, URIO, ZIO }

object SimpleExample2 extends App {
  val nestedItem       = Item("id" -> 2, "name" -> "Avi", "flag" -> true)
  val parentItem: Item = Item("id" -> 1, "nested" -> nestedItem)

  final case class NestedCaseClass2(id: Int, nested: SimpleCaseClass3)
  implicit lazy val nestedCaseClass2: Schema[NestedCaseClass2] = DeriveSchema.gen[NestedCaseClass2]

  final case class SimpleCaseClass3(id: Int, name: String, flag: Boolean)
  implicit lazy val simpleCaseClass3: Schema[SimpleCaseClass3] = DeriveSchema.gen[SimpleCaseClass3]

  private val executorLayer = FakeDynamoDBExecutor.table("table1", pkFieldName = "id")().layer

  private val program: ZIO[Console with DynamoDBExecutor, Exception, Unit] = for {
    _    <- put("table1", NestedCaseClass2(1, SimpleCaseClass3(2, "Avi", true))).execute
//    _    <- putItem("table1", parentItem).execute
    item <- getItem("table1", PrimaryKey("id" -> 1)).execute
    _    <- putStrLn(s"found $item")
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(executorLayer).exitCode
}
