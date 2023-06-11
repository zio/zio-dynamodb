package zio.dynamodb

import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.dynamodb.PrimaryKey
import zio.dynamodb.proofs.IsPrimaryKey

/**
 * This API should only exists in type safe land ie only access is via a PE method (TODO confirm we can do this restriction)
 * Looks like we can
 * TODO:
 * copy render methods from KeyConditionExpression
 */
object Foo {
  sealed trait PartitionKeyEprn { self =>
    def &&(other: SortKeyEprn): PartitionKeyEprn              = PartitionKeyEprn.And(self, other)
    def &&(other: ComplexSortKeyEprn): KeyConditionExpression = PartitionKeyEprn.ComplexAnd(self, other)
  }
  sealed trait KeyConditionExpression // extends PartitionKeyEprn

  object PartitionKeyEprn {
    final case class PartitionKey(keyName: String) {
      def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): PartitionKeyEprn = {
        val _ = ev
        Equals(this, to.toAttributeValue(value))
      }
    }

    final case class And(pk: PartitionKeyEprn, sk: SortKeyEprn)               extends PartitionKeyEprn
    final case class ComplexAnd(pk: PartitionKeyEprn, sk: ComplexSortKeyEprn) extends KeyConditionExpression

    final case class Equals(pk: PartitionKey, value: AttributeValue) extends PartitionKeyEprn
  }

  sealed trait SortKeyEprn
  sealed trait ComplexSortKeyEprn
  object SortKeyExprn {
    final case class SortKey(keyName: String) {
      def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): SortKeyEprn = {
        val _ = ev
        Equals(this, to.toAttributeValue(value))
      }
      def >[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): ComplexSortKeyEprn = {
        val _ = ev
        GreaterThan(this, to.toAttributeValue(value))
      }
      // ... and so on for all the other operators
    }
    final case class Equals(sortKey: SortKey, value: AttributeValue) extends SortKeyEprn
    final case class GreaterThan(sortKey: SortKey, value: AttributeValue) extends ComplexSortKeyEprn
  }

}

object FooExample extends App {
  import Foo._
  import Foo.PartitionKeyEprn._
  import Foo.SortKeyExprn._

  // DynamoDbQuery's still use PrimaryKey
  // typesafe API constructors only expose PartitionKeyEprn
  def asPk(k: PartitionKeyEprn): PrimaryKey = {
    val pk = k match {
      case PartitionKeyEprn.Equals(pk, value) => PrimaryKey(pk.keyName -> value)
      case PartitionKeyEprn.And(pk, sk)       =>
        (pk, sk) match {
          case (PartitionKeyEprn.Equals(pk, value), SortKeyExprn.Equals(sk, value2)) =>
            PrimaryKey(pk.keyName -> value, sk.keyName -> value2)
          case _                                                                     =>
            throw new IllegalArgumentException("This should not happed?????")
        }
    }
    pk
  }

  def whereKey(k: PartitionKeyEprn)       =
    k match {
      case PartitionKeyEprn.Equals(pk, value) => println(s"pk=$pk, value=$value")
      case PartitionKeyEprn.And(pk, sk)       => println(s"pk=$pk, sk=$sk")
    }
  def whereKey(k: KeyConditionExpression) =
    k match {
      case PartitionKeyEprn.ComplexAnd(pk, sk) => println(s"pk=$pk, sk=$sk")
    }

  val x: PartitionKeyEprn       = PartitionKey("email") === 1
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
  println(asPk(pkAndSk))
  println(pkAndSkExtended)
}
