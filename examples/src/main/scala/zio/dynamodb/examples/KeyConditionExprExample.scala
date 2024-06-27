package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression

import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.schema.annotation.discriminatorName
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
  val sk2: ExtendedSortKeyExpr[Student, String] = Student.subject.sortKey > "y"
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
    Student.email.partitionKey === "x" && Student.binary2.sortKey.beginsWith(Vector(1.toByte))

  val (aliasMap, s) = pkAndSkExtended1.render.execute
  println(s"aliasMap=$aliasMap, s=$s")

  val get = DynamoDBQuery.queryAllItem("table").whereKey($("foo.bar").partitionKey === 1 && $("foo.baz").sortKey > 1)

  // more complex example
  @discriminatorName("invoiceType")
  sealed trait InvoiceWithDiscriminatorName {
    def id: String
  }
  object InvoiceWithDiscriminatorName       {
    final case class Unpaid(id: String) extends InvoiceWithDiscriminatorName
    object Unpaid {
      implicit val schema: Schema.CaseClass1[String, Unpaid] = DeriveSchema.gen[Unpaid]
      val id                                                 = ProjectionExpression.accessors[Unpaid]
    }
    final case class Paid(
      id: String,
      amount: Option[Int],
      accountId: Option[String] = None,
      email: Option[String] = None
    ) extends InvoiceWithDiscriminatorName
    object Paid   {
      implicit val schema: Schema.CaseClass4[String, Option[Int], Option[String], Option[String], Paid] =
        DeriveSchema.gen[Paid]
      val (id, amount, accountId, email)                                                                = ProjectionExpression.accessors[Paid]
    }
    implicit val schema: Schema.Enum2[Unpaid, Paid, InvoiceWithDiscriminatorName] =
      DeriveSchema.gen[InvoiceWithDiscriminatorName]
    val (unpaid, paid) = ProjectionExpression.accessors[InvoiceWithDiscriminatorName]
  }

  val peId: ProjectionExpression[InvoiceWithDiscriminatorName, String]                =
    InvoiceWithDiscriminatorName.paid >>> InvoiceWithDiscriminatorName.Paid.id
  val peAccountId: ProjectionExpression[InvoiceWithDiscriminatorName, Option[String]] =
    InvoiceWithDiscriminatorName.paid >>> InvoiceWithDiscriminatorName.Paid.accountId
  val peEmailId: ProjectionExpression[InvoiceWithDiscriminatorName, Option[String]]   =
    InvoiceWithDiscriminatorName.paid >>> InvoiceWithDiscriminatorName.Paid.email
  val peAmount: ProjectionExpression[InvoiceWithDiscriminatorName, Option[Int]]       =
    InvoiceWithDiscriminatorName.paid >>> InvoiceWithDiscriminatorName.Paid.amount
  val pk1: PartitionKeyEquals[InvoiceWithDiscriminatorName]                           = peId.partitionKey === "1"
  val pk2: PartitionKeyEquals[InvoiceWithDiscriminatorName]                           = peAccountId.partitionKey === Some("1")
  val pk3: CompositePrimaryKeyExpr[InvoiceWithDiscriminatorName]                      =
    peAccountId.partitionKey === Some("1") && peEmailId.sortKey === Some("1")
  val pk4: ExtendedCompositePrimaryKeyExpr[InvoiceWithDiscriminatorName]              =
    peAccountId.partitionKey === Some("1") && peEmailId.sortKey > Some("1")
  val pk5: ExtendedCompositePrimaryKeyExpr[InvoiceWithDiscriminatorName]              =
    peAccountId.partitionKey === Some("1") && peEmailId.sortKey < Some("1")
  val pk6: ExtendedCompositePrimaryKeyExpr[InvoiceWithDiscriminatorName]              =
    peAccountId.partitionKey === Some("1") && peEmailId.sortKey <> Some("1")
  val pk7: ExtendedCompositePrimaryKeyExpr[InvoiceWithDiscriminatorName]              =
    peAccountId.partitionKey === Some("1") && peEmailId.sortKey <= Some("1")
  val pk8: ExtendedCompositePrimaryKeyExpr[InvoiceWithDiscriminatorName]              =
    peAccountId.partitionKey === Some("1") && peEmailId.sortKey >= Some("1")
  val pk9: ExtendedCompositePrimaryKeyExpr[InvoiceWithDiscriminatorName]              =
    peAccountId.partitionKey === Some("1") && peEmailId.sortKey.between(Some("1"), Some("3"))
  val pk10: ExtendedCompositePrimaryKeyExpr[InvoiceWithDiscriminatorName]             =
    peAccountId.partitionKey === Some("1") && peEmailId.sortKey.beginsWith(Some("1"))
  // val pk11: ExtendedCompositePrimaryKeyExpr[InvoiceWithDiscriminatorName]             =
  //   peAccountId.partitionKey === Some("1") && peAmount.sortKey.beginsWith(Some(1)) // does not compile as expected

}
