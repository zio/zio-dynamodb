package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression

import zio.schema.Schema
import zio.schema.DeriveSchema

object KeyConditionExprExample extends App {

  import zio.dynamodb.KeyConditionExpr._
  import zio.dynamodb.KeyConditionExpr.SortKeyExpr

  // DynamoDbQuery's still use PrimaryKey
  // typesafe API constructors only expose PartitionKeyEprn


  // def whereKey[From](k: KeyConditionExpr[From]) =
  //   k match {
  //     // PartitionKeyExpr
  //     case PartitionKeyExpr(pk, value)             => println(s"pk=$pk, value=$value")
  //     // CompositePrimaryKeyExpr
  //     case CompositePrimaryKeyExpr(pk, sk)         => println(s"pk=$pk, sk=$sk")
  //     // ExtendedCompositePrimaryKeyExpr
  //     case ExtendedCompositePrimaryKeyExpr(pk, sk) => println(s"pk=$pk, sk=$sk")
  //   }

  // in low level - non type safe land
  //val x1: PartitionKeyExpr[Nothing]                = PartitionKey("email") === "x"
  // val x2: SortKeyEprn[Nothing]                     = SortKey("subject") === "y"
  // val x3: CompositePrimaryKeyExpr[Nothing]         = x1 && x2
  // val x4                                           = PartitionKey[Nothing]("email") === "x" && SortKey[Nothing]("subject") === "y"
  // val x5: ExtendedCompositePrimaryKeyExpr[Nothing] =
  //   PartitionKey[Nothing]("email") === "x" && SortKey[Nothing]("subject") > "y"

  // val y0: PartitionKeyExpr[Any]                = PartitionKey("email") === "x"
  // val y1: CompositePrimaryKeyExpr[Any]         = PartitionKey("email") === "x" && SortKey("subject") === "y"
  // val y2: ExtendedCompositePrimaryKeyExpr[Any] = PartitionKey("email") === "x" && SortKey("subject") > "y"

  import zio.dynamodb.ProjectionExpression.$
// TODO: Avi
//inferred type arguments [String] do not conform to method ==='s type parameter bounds [To2 <: zio.dynamodb.ProjectionExpression.Unknown]
  val x6: CompositePrimaryKeyExpr[Any, String]      = $("foo.bar").primaryKey === 1 && $("foo.baz").sortKey === "y"
  val x7                                            = $("foo.bar").primaryKey === 1 && $("foo.baz").sortKey > 1
  val x8: ExtendedCompositePrimaryKeyExpr[Any, Int] =
    $("foo.bar").primaryKey === 1 && $("foo.baz").sortKey.between(1, 2)
  val x9                                            =
    $("foo.bar").primaryKey === 1 && $("foo.baz").sortKey.beginsWith(1L)

  final case class Elephant(email: String, subject: String, age: Int)
  object Elephant {
    implicit val schema: Schema.CaseClass3[String, String, Int, Elephant] = DeriveSchema.gen[Elephant]
    val (email, subject, age)                                             = ProjectionExpression.accessors[Elephant]
  }
  final case class Student(email: String, subject: String, age: Long, binary: List[Byte])
  object Student  {
    implicit val schema: Schema.CaseClass4[String, String, Long, List[Byte], Student] = DeriveSchema.gen[Student]
    val (email, subject, age, binary)                                                 = ProjectionExpression.accessors[Student]
  }

  val pk: PartitionKeyExpr[Student, String]     = Student.email.primaryKey === "x"
//  val pkX: PartitionKeyExpr[Student, String]     = Student.age.primaryKey === "x" // as expected does not compile
  val sk1: SortKeyExpr[Student, String]         = Student.subject.sortKey === "y"
  val sk2: ExtendedSortKeyExpr[Student, String] = Student.subject.sortKey > "y"
  val pkAndSk                                   = Student.email.primaryKey === "x" && Student.subject.sortKey === "y"
  //val three = Student.email.primaryKey === "x" && Student.subject.sortKey === "y" && Student.subject.sortKey // 3 terms not allowed
  val pkAndSkExtended1                          =
    Student.email.primaryKey === "x" && Student.subject.sortKey > "y"
  val pkAndSkExtended2                          =
    Student.email.primaryKey === "x" && Student.subject.sortKey < "y"
  val pkAndSkExtended3                          =
    Student.email.primaryKey === "x" && Student.subject.sortKey.between("1", "2")
  val pkAndSkExtended4                          =
    Student.email.primaryKey === "x" && Student.subject.sortKey.beginsWith("1")
  val pkAndSkExtended5                          =
    Student.email.primaryKey === "x" && Student.binary.sortKey.beginsWith(List(1.toByte))
  // TODO: Avi - fix ToAttrubuteValue interop with Array[Byte]

  // GetItem Query will have three overridden versions
  // 1) takes AttrMap/PriamaryKey - for users of low level API
  // 2) takes PartitionKeyExpr - internally this can be converted to AttrMap/PriamaryKey
  // 3) takes CompositePrimaryKeyExpr - internally this can b converted to AttrMap/PriamaryKey

  // whereKey function (for Query)
  // 1) takes a KeyConditionExpression
  // 2) down side is that users of low level API will have to construct case class instances manually - but they have to do that now anyway
  // println(asPk(pkAndSk))
  // println(pkAndSkExtended)

  // Render requirements
  val (aliasMap, s) = pkAndSkExtended1.render.execute
  println(s"aliasMap=$aliasMap, s=$s")
}

object Foo {
  final case class Student(email: String, subject: String, age: Int)
  object Student {
    implicit val schema: Schema.CaseClass3[String, String, Int, Student] = DeriveSchema.gen[Student]
    val (email, subject, age)                                            = ProjectionExpression.accessors[Student]
  }

  val pkAndSk =
    Student.email.primaryKey === "x" && Student.subject.sortKey === "y"

}
