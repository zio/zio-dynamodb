package zio.dynamodb

import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.dynamodb.PrimaryKey
import zio.dynamodb.proofs.IsPrimaryKey
import zio.dynamodb.Foo.PartitionKeyExprn.ComplexAnd

/**
 * This API should only exists in type safe land ie only access is via a PE method (TODO confirm we can do this restriction)
 * Looks like we can
 * TODO:
 * rename PartitionKeyEprn to PrimaryKeyExprn
 * copy render methods from
 * make PartitionKeyEprn/KeyConditionExpression sum types so that they can be stored in the same type on the query class
 * DONE
 */
object Foo {
  sealed trait KeyConditionExpression extends Renderable { self =>
    def render: AliasMapRender[String] =
      self match {
        case and @ Foo.PartitionKeyExprn.And(_, _)       => and.render
        case and @ ComplexAnd(_, _)                      => and.render
        case equals @ Foo.PartitionKeyExprn.Equals(_, _) => equals.render
      }
  }
  object KeyConditionExpression {
  }
  // models primary key expressions
  // email.primaryKey === "x"
  // Student.email.primaryKey === "x" && Student.subject.sortKey === "y"
  sealed trait PartitionKeyExprn extends KeyConditionExpression { self =>
    def &&(other: SortKeyEprn): PartitionKeyExprn                 = PartitionKeyExprn.And(self, other)
    def &&(other: ExtendedSortKeyEprn): ExtendedPartitionKeyExprn = PartitionKeyExprn.ComplexAnd(self, other)

    override def render: AliasMapRender[String] =
      self match {
        case PartitionKeyExprn.And(pk, sk)       =>
          for {
            pkStr <- pk.render
            skStr <- sk.render
          } yield s"$pkStr AND $skStr"
        case PartitionKeyExprn.Equals(pk, value) =>
          for {
            pkStr <- AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
          } yield s"$pkStr = $value"
      }
  }

  // models "extended" primary key expressions
  // Student.email.primaryKey === "x" && Student.subject.sortKey > "y"
  sealed trait ExtendedPartitionKeyExprn extends KeyConditionExpression { self =>
    override def render: AliasMapRender[String] = ???
  }

  // no overlap between PartitionKeyExprn and ExtendedPartitionKeyExprn

  object PartitionKeyExprn {
    final case class PartitionKey(keyName: String) {
      def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): PartitionKeyExprn = {
        val _ = ev
        Equals(this, to.toAttributeValue(value))
      }
    }

    final case class And(pk: PartitionKeyExprn, sk: SortKeyEprn)                extends PartitionKeyExprn
    final case class ComplexAnd(pk: PartitionKeyExprn, sk: ExtendedSortKeyEprn) extends ExtendedPartitionKeyExprn

    final case class Equals(pk: PartitionKey, value: AttributeValue) extends PartitionKeyExprn
  }

  sealed trait SortKeyEprn {
    def render: AliasMapRender[String] = ???
  }
  sealed trait ExtendedSortKeyEprn
  object SortKeyExprn      {
    final case class SortKey(keyName: String) {
      def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): SortKeyEprn = {
        val _ = ev
        Equals(this, to.toAttributeValue(value))
      }
      def >[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): ExtendedSortKeyEprn = {
        val _ = ev
        GreaterThan(this, to.toAttributeValue(value))
      }
      // ... and so on for all the other operators
    }
    final case class Equals(sortKey: SortKey, value: AttributeValue) extends SortKeyEprn
    final case class GreaterThan(sortKey: SortKey, value: AttributeValue) extends ExtendedSortKeyEprn
  }

}

object FooExample extends App {
  import Foo._
  import Foo.PartitionKeyExprn._
  import Foo.SortKeyExprn._

  // DynamoDbQuery's still use PrimaryKey
  // typesafe API constructors only expose PartitionKeyEprn
  def asPk(k: PartitionKeyExprn): PrimaryKey = {
    val pk = k match {
      case PartitionKeyExprn.Equals(pk, value) => PrimaryKey(pk.keyName -> value)
      case PartitionKeyExprn.And(pk, sk)       =>
        (pk, sk) match {
          case (PartitionKeyExprn.Equals(pk, value), SortKeyExprn.Equals(sk, value2)) =>
            PrimaryKey(pk.keyName -> value, sk.keyName -> value2)
          case _                                                                      =>
            throw new IllegalArgumentException("This should not happed?????")
        }
    }
    pk
  }

  def whereKey(k: PartitionKeyExprn)         =
    k match {
      case PartitionKeyExprn.Equals(pk, value) => println(s"pk=$pk, value=$value")
      case PartitionKeyExprn.And(pk, sk)       => println(s"pk=$pk, sk=$sk")
    }
  def whereKey(k: ExtendedPartitionKeyExprn) =
    k match {
      case PartitionKeyExprn.ComplexAnd(pk, sk) => println(s"pk=$pk, sk=$sk")
    }

  val x: KeyConditionExpression    = PartitionKey("email") === 1
  val y: PartitionKeyExprn         = PartitionKey("email") === "x" && SortKey("subject") === "y"
  val z: ExtendedPartitionKeyExprn = PartitionKey("email") === "x" && SortKey("subject") > "y"

  final case class Student(email: String, subject: String, age: Int)
  object Student {
    implicit val schema: Schema.CaseClass3[String, String, Int, Student] = DeriveSchema.gen[Student]
    val (email, subject, age)                                            = ProjectionExpression.accessors[Student]
  }

  val pk: PartitionKeyExprn                      = Student.email.primaryKey === "x"
  val pkAndSk: PartitionKeyExprn                 = Student.email.primaryKey === "x" && Student.subject.sortKey === "y"
  //val three = Student.email.primaryKey === "x" && Student.subject.sortKey === "y" && Student.subject.sortKey // 3 terms not allowed
  val pkAndSkExtended: ExtendedPartitionKeyExprn = Student.email.primaryKey === "x" && Student.subject.sortKey > "y"
  // GetItem Query will have two overridden versions
  // 1) takes AttrMap/PriamaryKey - for users of low level API
  // 2) takes PartitionKeyExprn - internally this can be converted to AttrMap/PriamaryKey

  // whereKey function (for Query) can have 2 overridden versions
  // 1) takes PartitionKeyExprn
  // 2) takes ExtendedPartitionKeyExprn
  // 3) down side is that users of low level API will have to construct case class instances manually - but they have to do that now anyway
  println(asPk(pkAndSk))
  println(pkAndSkExtended)
}
