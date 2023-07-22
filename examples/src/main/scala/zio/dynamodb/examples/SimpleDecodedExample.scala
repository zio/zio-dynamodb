package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.{ DynamoDBExecutor, Item }
import zio.schema.{ DeriveSchema, Schema }
import zio.ZIOAppDefault
import zio.Console.printLine
import zio.dynamodb.ProjectionExpression

object SimpleDecodedExample extends ZIOAppDefault {
  val nestedItem       = Item("id" -> 2, "name" -> "Avi", "flag" -> true)
  val parentItem: Item = Item("id" -> 1, "nested" -> nestedItem)

  final case class NestedCaseClass2(id: Int, nested: SimpleCaseClass3)
  object NestedCaseClass2 {
    implicit val nestedCaseClass2: Schema.CaseClass2[Int, SimpleCaseClass3, NestedCaseClass2] =
      DeriveSchema.gen[NestedCaseClass2]
    val (id, nested)                                                                          = ProjectionExpression.accessors[NestedCaseClass2]
  }

  final case class SimpleCaseClass3(id: Int, name: String, flag: Boolean)
  implicit lazy val simpleCaseClass3: Schema[SimpleCaseClass3] = DeriveSchema.gen[SimpleCaseClass3]

  private val program = for {
    _         <-
      put("table1", NestedCaseClass2(id = 1, SimpleCaseClass3(2, "Avi", flag = true))).execute // Save case class to DB
    caseClass <- get("table1", NestedCaseClass2.id.partitionKey === 2).execute // read case class from DB
    _         <- printLine(s"get: found $caseClass")
    either    <- scanSome[NestedCaseClass2]("table1", 10).execute
    _         <- printLine(s"scanSome: found $either")
    stream    <- scanAll[NestedCaseClass2]("table1").execute
    xs        <- stream.runCollect
    _         <- printLine(s"scanAll: found stream $xs")

    either2 <- querySome[NestedCaseClass2]("table1", 10).execute
    _       <- printLine(s"querySome: found $either2")
    stream2 <- queryAll[NestedCaseClass2]("table1").execute
    xs2     <- stream2.runCollect
    _       <- printLine(s"queryAll: found stream $xs2")

  } yield ()

  override def run =
    program.provide(DynamoDBExecutor.test("table1" -> "id"))
}
