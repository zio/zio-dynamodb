package zio.dynamodb.examples

import zio.console.putStrLn
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.{ DynamoDBExecutor, Item, PrimaryKey, TestDynamoDBExecutor }
import zio.schema.{ DeriveSchema, Schema }
import zio.{ App, ExitCode, URIO }

object SimpleDecodedExample extends App {
  val nestedItem       = Item("id" -> 2, "name" -> "Avi", "flag" -> true)
  val parentItem: Item = Item("id" -> 1, "nested" -> nestedItem)

  final case class NestedCaseClass2(id: Int, nested: SimpleCaseClass3)
  implicit lazy val nestedCaseClass2: Schema[NestedCaseClass2] = DeriveSchema.gen[NestedCaseClass2]

  final case class SimpleCaseClass3(id: Int, name: String, flag: Boolean)
  implicit lazy val simpleCaseClass3: Schema[SimpleCaseClass3] = DeriveSchema.gen[SimpleCaseClass3]

  private val program = for {
    _         <- TestDynamoDBExecutor.addTable("table1", partitionKey = "id")
    _         <-
      put("table1", NestedCaseClass2(id = 1, SimpleCaseClass3(2, "Avi", flag = true))).execute // Save case class to DB
    caseClass <- get[NestedCaseClass2]("table1", PrimaryKey("id" -> 1)).execute // read case class from DB
    _         <- putStrLn(s"get: found $caseClass")
    either    <- scanSome[NestedCaseClass2]("table1", 10).execute
    _         <- putStrLn(s"scanSome: found $either")
    stream    <- scanAll[NestedCaseClass2]("table1").execute
    xs        <- stream.runCollect
    _         <- putStrLn(s"scanAll: found stream $xs")

    either2 <- querySome[NestedCaseClass2]("table1", 10).execute
    _       <- putStrLn(s"querySome: found $either2")
    stream2 <- queryAll[NestedCaseClass2]("table1").execute
    xs2     <- stream2.runCollect
    _       <- putStrLn(s"queryAll: found stream $xs2")

  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program.provideCustomLayer(DynamoDBExecutor.test).exitCode
}
