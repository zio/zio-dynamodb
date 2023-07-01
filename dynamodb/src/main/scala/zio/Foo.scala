package zio.dynamodb

import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.dynamodb.PrimaryKey
import zio.dynamodb.proofs.IsPrimaryKey

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
  sealed trait KeyConditionExpression[From] extends Renderable                   { self =>
    def render: AliasMapRender[String] = ??? // do render in concrete classes
  }
  object KeyConditionExpression {}
  // models primary key expressions
  // email.primaryKey === "x"
  // Student.email.primaryKey === "x" && Student.subject.sortKey === "y"
  sealed trait PartitionKeyExprn[From]      extends KeyConditionExpression[From] { self =>
    def &&(other: SortKeyEprn[From]): PartitionKeyExprn[From]                 = PartitionKeyExprn.And[From](self, other)
    def &&(other: ExtendedSortKeyEprn[From]): ExtendedPartitionKeyExprn[From] =
      PartitionKeyExprn.ComplexAnd[From](self, other)

    // def render2: AliasMapRender[String] =
    //   self match {
    //     case PartitionKeyExprn.And(pk, sk)       =>
    //       for {
    //         pkStr <- pk.render
    //         skStr <- sk.render
    //       } yield s"$pkStr AND $skStr"
    //     case PartitionKeyExprn.Equals(pk, value) =>
    //       for {
    //         pkStr <- AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
    //       } yield s"$pkStr = $value"
    //   }
  }

  // models "extended" primary key expressions
  // Student.email.primaryKey === "x" && Student.subject.sortKey > "y"
  sealed trait ExtendedPartitionKeyExprn[From] extends KeyConditionExpression[From] { self =>
//    override def render: AliasMapRender[String] = ???
  }

  // no overlap between PartitionKeyExprn and ExtendedPartitionKeyExprn

  object PartitionKeyExprn {
    final case class PartitionKey[From](keyName: String) {
      def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): PartitionKeyExprn[From] = {
        val _ = ev
        Equals(this, to.toAttributeValue(value))
      }
    }

    final case class And[From](pk: PartitionKeyExprn[From], sk: SortKeyEprn[From]) extends PartitionKeyExprn[From] {
      self =>
      // do render in concrete classes

    }
    final case class ComplexAnd[From](pk: PartitionKeyExprn[From], sk: ExtendedSortKeyEprn[From])
        extends ExtendedPartitionKeyExprn[From]

    final case class Equals[From](pk: PartitionKey[From], value: AttributeValue) extends PartitionKeyExprn[From]
  }

  sealed trait SortKeyEprn[From] {
//    def render: AliasMapRender[String] = ???
  }
  sealed trait ExtendedSortKeyEprn[From]
  object SortKeyExprn            {
    final case class SortKey[From](keyName: String) {
      def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): SortKeyEprn[From] = {
        val _ = ev
        Equals[From](this, to.toAttributeValue(value))
      }
      def >[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): ExtendedSortKeyEprn[From] = {
        val _ = ev
        GreaterThan(this, to.toAttributeValue(value))
      }
      // ... and so on for all the other operators
    }
    final case class Equals[From](sortKey: SortKey[From], value: AttributeValue) extends SortKeyEprn[From]
    final case class GreaterThan[From](sortKey: SortKey[From], value: AttributeValue) extends ExtendedSortKeyEprn[From]
  }

}

object FooExample extends App {
  import Foo._

  // DynamoDbQuery's still use PrimaryKey
  // typesafe API constructors only expose PartitionKeyEprn
  def asPk[From](k: PartitionKeyExprn[From]): PrimaryKey = {
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

  def whereKey[From](k: PartitionKeyExprn[From])         =
    k match {
      case PartitionKeyExprn.Equals(pk, value) => println(s"pk=$pk, value=$value")
      case PartitionKeyExprn.And(pk, sk)       => println(s"pk=$pk, sk=$sk")
    }
  def whereKey[From](k: ExtendedPartitionKeyExprn[From]) =
    k match {
      case PartitionKeyExprn.ComplexAnd(pk, sk) => println(s"pk=$pk, sk=$sk")
    }

  // in low level - non type safe land
  // TODO: Avi - fix "Nothing <: From, but trait SortKeyEprn is invariant in type From"
  // otherwise users of the low level API will have to type all eprns with Nothing for From
  import Foo.PartitionKeyExprn._
  import Foo.SortKeyExprn._
  val x: PartitionKeyExprn[Nothing]         = PartitionKey("email") === 1
  /*
	"type mismatch;\n found   : zio.dynamodb.Foo.SortKeyEprn[Nothing]\n required: zio.dynamodb.Foo.SortKeyEprn[From]\
  nNote: Nothing <: From, but trait SortKeyEprn is invariant in type From.\nYou may wish to define From as +From instead. (SLS 4.5)",
   */
  val yyyy: PartitionKeyExprn[Nothing]      = PartitionKey("email") === "x"
  val xxxx: SortKeyEprn[Nothing]            = SortKey("subject") === "y"
  val sdfsdf: PartitionKeyExprn[Nothing]    = yyyy && xxxx
  val y                                     = PartitionKey[Nothing]("email") === "x" && SortKey[Nothing]("subject") === "y"
  val z: ExtendedPartitionKeyExprn[Nothing] =
    PartitionKey[Nothing]("email") === "x" && SortKey[Nothing]("subject") > "y"

  final case class Student(email: String, subject: String, age: Int)
  object Student {
    implicit val schema: Schema.CaseClass3[String, String, Int, Student] = DeriveSchema.gen[Student]
    val (email, subject, age)                                            = ProjectionExpression.accessors[Student]
  }

  val pk: PartitionKeyExprn[Student]                      = Student.email.primaryKey === "x"
  val sk1: SortKeyEprn[Student]                           = Student.subject.sortKey === "y"
  val sk2: ExtendedSortKeyEprn[Student]                   = Student.subject.sortKey > "y"
  val pkAndSk: PartitionKeyExprn[Student]                 = Student.email.primaryKey === "x" && Student.subject.sortKey === "y"
  //val three = Student.email.primaryKey === "x" && Student.subject.sortKey === "y" && Student.subject.sortKey // 3 terms not allowed
  val pkAndSkExtended: ExtendedPartitionKeyExprn[Student] =
    Student.email.primaryKey === "x" && Student.subject.sortKey > "y"

  // GetItem Query will have two overridden versions
  // 1) takes AttrMap/PriamaryKey - for users of low level API
  // 2) takes PartitionKeyExprn - internally this can be converted to AttrMap/PriamaryKey

  // whereKey function (for Query) can have 2 overridden versions
  // 1) takes PartitionKeyExprn
  // 2) takes ExtendedPartitionKeyExprn
  // 3) down side is that users of low level API will have to construct case class instances manually - but they have to do that now anyway
  println(asPk(pkAndSk))
  println(pkAndSkExtended)

  // Render requirements
  pkAndSk.render.map(s => println(s"rendered: $s"))
}
