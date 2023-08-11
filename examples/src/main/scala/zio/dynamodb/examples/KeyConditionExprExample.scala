package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression

import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.dynamodb.DynamoDBQuery

object KeyConditionExprExample extends App {

  import zio.dynamodb.KeyConditionExpr._
  import zio.dynamodb.KeyConditionExpr.SortKeyEquals
  import zio.dynamodb.ProjectionExpression.$

  val x6 =
    $("foo.bar").partitionKey === 1 && $("foo.baz").sortKey === "y"
  val x7 = $("foo.bar").partitionKey === 1 && $("foo.baz").sortKey > 1
  val x8 =
    $("foo.bar").partitionKey === 1 && $("foo.baz").sortKey.between(1, 2)
  val x9 =
    $("foo.bar").partitionKey === 1 && $("foo.baz").sortKey.beginsWith(1L)

  final case class Elephant(email: String, subject: String, age: Int)
  object Elephant {
    implicit val schema: Schema.CaseClass3[String, String, Int, Elephant] = DeriveSchema.gen[Elephant]
    val (email, subject, age)                                             = ProjectionExpression.accessors[Elephant]
  }
  final case class Student(email: String, subject: String, age: Long, binary: List[Byte], binary2: Vector[Byte])
  object Student  {
    implicit val schema: Schema.CaseClass5[String, String, Long, List[Byte], Vector[Byte], Student] =
      DeriveSchema.gen[Student]
    val (email, subject, age, binary, binary2)                                                      = ProjectionExpression.accessors[Student]
  }

  val pk: PartitionKeyEquals[Student]           = Student.email.partitionKey === "x"
//  val pkX: PartitionKeyExpr[Student, String]     = Student.age.primaryKey === "x" // as expected does not compile
  val sk1: SortKeyEquals[Student]               = Student.subject.sortKey === "y"
  val sk2: ExtendedSortKeyExpr[Student, String]    = Student.subject.sortKey > "y"
  val pkAndSk: CompositePrimaryKeyExpr[Student] =
    Student.email.partitionKey === "x" && Student.subject.sortKey === "y"

  //val three = Student.email.primaryKey === "x" && Student.subject.sortKey === "y" && Student.subject.sortKey // 3 terms not allowed
  val pkAndSkExtended1 =
    Student.email.partitionKey === "x" && Student.subject.sortKey > "y"
  val pkAndSkExtended2 =
    Student.email.partitionKey === "x" && Student.subject.sortKey < "y"
  val pkAndSkExtended3 =
    Student.email.partitionKey === "x" && Student.subject.sortKey.between("1", "2")
  val pkAndSkExtended4 =
    Student.email.partitionKey === "x" && Student.subject.sortKey.beginsWith("1")
  val pkAndSkExtended5 =
    Student.email.partitionKey === "x" && Student.binary.sortKey.beginsWith(List(1.toByte))
  val pkAndSkExtended6 =
    Student.email.partitionKey === "x" && Student.binary2.sortKey.beginsWith(List(1.toByte))

  val (aliasMap, s) = pkAndSkExtended1.render.execute
  println(s"aliasMap=$aliasMap, s=$s")

  val get = DynamoDBQuery.queryAllItem("table").whereKey($("foo.bar").partitionKey === 1 && $("foo.baz").sortKey > 1)
}
