package zio.dynamodb

import zio.schema.Schema
import zio.schema.DeriveSchema
//import zio.dynamodb.PrimaryKey

/**
 * This API should only exists in type safe land ie only access is via a PE method (TODO confirm we can do this restriction)
 * Looks like we can
 * TODO: check how AWS uses filter vs where
 * TODO: propogate From/To type parameters
 */
object Foo {
  sealed trait PartitionKeyEprn { self =>
    def &&(other: SortKeyEprn): PartitionKeyEprn              = PartitionKeyEprn.And(self, other)
    def &&(other: ComplexSortKeyEprn): KeyConditionExpression = PartitionKeyEprn.ComplexAnd(self, other)
  }
  sealed trait KeyConditionExpression // extends PartitionKeyEprn

  object PartitionKeyEprn {
    final case class PartitionKey(keyName: String) {
      def ===(value: String): PartitionKeyEprn = Equals(this, value)
    }

    final case class And(pk: PartitionKeyEprn, sk: SortKeyEprn)               extends PartitionKeyEprn
    final case class ComplexAnd(pk: PartitionKeyEprn, sk: ComplexSortKeyEprn) extends KeyConditionExpression

    final case class Equals(pk: PartitionKey, value: String) extends PartitionKeyEprn
  }

  sealed trait SortKeyEprn
  sealed trait ComplexSortKeyEprn
  object SortKeyExprn {
    final case class SortKey(keyName: String) {
      def ===(value: String): SortKeyEprn      = Equals(this, value)
      def >(value: String): ComplexSortKeyEprn = GreaterThan(this, value)
      // ... and so on for all the other operators
    }
    final case class Equals(sortKey: SortKey, value: String) extends SortKeyEprn
    final case class GreaterThan(sortKey: SortKey, value: String) extends ComplexSortKeyEprn
  }

}

object FooExample extends App {
  import Foo._
  import Foo.PartitionKeyEprn._
  import Foo.SortKeyExprn._

//   def get(k: PartitionKeyEprn) = {
//     val pk = k match {
//       case PartitionKeyEprn.Equals(pk, value) => PrimaryKey(pk.keyName -> value)
//       case PartitionKeyEprn.And(pk, sk)       => sk match {
//         case SortKeyExprn.Equals(sk, value) => PrimaryKey(pk.keyName -> value)
//       }
//     }
//   } 

  def where(k: PartitionKeyEprn)       =
    k match {
      case PartitionKeyEprn.Equals(pk, value) => println(s"pk=$pk, value=$value")
      case PartitionKeyEprn.And(pk, sk)       => println(s"pk=$pk, sk=$sk")
    }
  def where(k: KeyConditionExpression) =
    k match {
      case PartitionKeyEprn.ComplexAnd(pk, sk) => println(s"pk=$pk, sk=$sk")
    }

  val x: PartitionKeyEprn       = PartitionKey("email") === "x"
  val y: PartitionKeyEprn       = PartitionKey("email") === "x" && SortKey("subject") === "y"
  val z: KeyConditionExpression = PartitionKey("email") === "x" && SortKey("subject") > "y"

  final case class Student(email: String, subject: String, age: Int)
  object Student {
    implicit val schema: Schema.CaseClass3[String, String, Int, Student] = DeriveSchema.gen[Student]
    val (email, subject, age)                                            = ProjectionExpression.accessors[Student]
  }

  val pk: PartitionKeyEprn                    = Student.email.primaryKey === "x"
  val pkAndSk: PartitionKeyEprn               = Student.email.primaryKey === "x" && Student.subject.sortKey === "y"
  //val three = Student.email.primaryKey === "x" && Student.subject.sortKey === "y" && Student.subject.sortKey // 3 terms not allowed
  val pkAndSkExtended: KeyConditionExpression = Student.email.primaryKey === "x" && Student.subject.sortKey > "y"
  println(pkAndSk)
  println(pkAndSkExtended)
}
