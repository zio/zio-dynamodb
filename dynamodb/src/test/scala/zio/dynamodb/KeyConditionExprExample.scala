package zio.dynamodb

import zio.schema.Schema
import zio.schema.DeriveSchema

object KeyConditionExprExample extends App {

  // DynamoDbQuery's still use PrimaryKey
  // typesafe API constructors only expose PartitionKeyEprn

  // TODO: move to PartitionKeyExpr
  def asPk[From](k: PartitionKeyExpr[From]): PrimaryKey        =
    k match {
      case PartitionKeyExpr.Equals(pk, value) => PrimaryKey(pk.keyName -> value)
    }
  def asPk[From](k: CompositePrimaryKeyExpr[From]): PrimaryKey =
    k match {
      case CompositePrimaryKeyExpr.And(pk, sk) =>
        (pk, sk) match {
          case (PartitionKeyExpr.Equals(pk, value), SortKeyExpr.Equals(sk, value2)) =>
            PrimaryKey(pk.keyName -> value, sk.keyName -> value2)
        }
    }

  def whereKey[From](k: KeyConditionExpr[From]) =
    k match {
      // PartitionKeyExpr
      case PartitionKeyExpr.Equals(pk, value)                 => println(s"pk=$pk, value=$value")
      // CompositePrimaryKeyExpr
      case CompositePrimaryKeyExpr.And(pk, sk)                => println(s"pk=$pk, sk=$sk")
      // ExtendedCompositePrimaryKeyExpr
      case ExtendedCompositePrimaryKeyExpr.ComplexAnd(pk, sk) => println(s"pk=$pk, sk=$sk")
    }

  // in low level - non type safe land
  // val x1: PartitionKeyExpr[Nothing]                = PartitionKey("email") === "x"
  // val x2: SortKeyEprn[Nothing]                     = SortKey("subject") === "y"
  // val x3: CompositePrimaryKeyExpr[Nothing]         = x1 && x2
  // val x4                                           = PartitionKey[Nothing]("email") === "x" && SortKey[Nothing]("subject") === "y"
  // val x5: ExtendedCompositePrimaryKeyExpr[Nothing] =
  //   PartitionKey[Nothing]("email") === "x" && SortKey[Nothing]("subject") > "y"

  // val y0: PartitionKeyExpr[Any]                = PartitionKey("email") === "x"
  // val y1: CompositePrimaryKeyExpr[Any]         = PartitionKey("email") === "x" && SortKey("subject") === "y"
  // val y2: ExtendedCompositePrimaryKeyExpr[Any] = PartitionKey("email") === "x" && SortKey("subject") > "y"

  import zio.dynamodb.ProjectionExpression.$
  val x6: CompositePrimaryKeyExpr[Any]         = $("foo.bar").primaryKey === "x" && $("foo.baz").sortKey === "y"
  val x7: ExtendedCompositePrimaryKeyExpr[Any] = $("foo.bar").primaryKey === "x" && $("foo.baz").sortKey > "y"

  final case class Student(email: String, subject: String, age: Int)
  object Student {
    implicit val schema: Schema.CaseClass3[String, String, Int, Student] = DeriveSchema.gen[Student]
    val (email, subject, age)                                            = ProjectionExpression.accessors[Student]
  }

  val pk: PartitionKeyExpr[Student]                             = Student.email.primaryKey === "x"
  val sk1: SortKeyEprn[Student]                                 = Student.subject.sortKey === "y"
  val sk2: ExtendedSortKeyExpr[Student]                         = Student.subject.sortKey > "y"
  val pkAndSk: CompositePrimaryKeyExpr[Student]                 = Student.email.primaryKey === "x" && Student.subject.sortKey === "y"
  //val three = Student.email.primaryKey === "x" && Student.subject.sortKey === "y" && Student.subject.sortKey // 3 terms not allowed
  val pkAndSkExtended: ExtendedCompositePrimaryKeyExpr[Student] =
    Student.email.primaryKey === "x" && Student.subject.sortKey > "y"

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
  val (aliasMap, s) = pkAndSkExtended.render.execute
  println(s"aliasMap=$aliasMap, s=$s")
}
