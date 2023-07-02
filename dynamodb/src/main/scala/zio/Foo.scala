package zio.dynamodb

import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.dynamodb.PrimaryKey
import zio.dynamodb.proofs.IsPrimaryKey

/**
 * Typesafe KeyConditionExpression/primary key experiment
 */
object Foo {
  sealed trait KeyConditionExpression[From] extends Renderable                   { self =>
    def render: AliasMapRender[String] // do render in concrete classes
  }
  object KeyConditionExpression {}
  // models primary key expressions
  // email.primaryKey === "x"
  // Student.email.primaryKey === "x" && Student.subject.sortKey === "y"
  sealed trait PartitionKeyExprn[From]      extends KeyConditionExpression[From] { self =>
    def &&(other: SortKeyEprn[From]): CompositePrimaryKeyExprn[From]                 = PartitionKeyExprn.And[From](self, other)
    def &&(other: ExtendedSortKeyEprn[From]): ExtendedCompositePrimaryKeyExprn[From] =
      PartitionKeyExprn.ComplexAnd[From](self, other)

    def render2: AliasMapRender[String] =
      self match {
        case PartitionKeyExprn.Equals(pk, value) =>
          AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
      }

    def render: AliasMapRender[String] =
      self match {
        case PartitionKeyExprn.Equals(pk, value) =>
          AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
      }
  }

  sealed trait CompositePrimaryKeyExprn[From] extends KeyConditionExpression[From] { self => }

  // models "extended" primary key expressions
  // Student.email.primaryKey === "x" && Student.subject.sortKey > "y"
  sealed trait ExtendedCompositePrimaryKeyExprn[From] extends KeyConditionExpression[From] { self => }

  // no overlap between PartitionKeyExprn and ExtendedPartitionKeyExprn

  object PartitionKeyExprn {
    final case class PartitionKey[From](keyName: String) {
      def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): PartitionKeyExprn[From] = {
        val _ = ev
        Equals(this, to.toAttributeValue(value))
      }
    }

    final case class And[From](pk: PartitionKeyExprn[From], sk: SortKeyEprn[From])
        extends CompositePrimaryKeyExprn[From] {
      self =>

      override def render: AliasMapRender[String] =
        self match {
          case PartitionKeyExprn.And(pk, sk) =>
            for {
              pkStr <- pk.render2
              skStr <- sk.render2
            } yield s"$pkStr AND $skStr"
        }

      // do render in concrete classes

    }
    final case class ComplexAnd[From](pk: PartitionKeyExprn[From], sk: ExtendedSortKeyEprn[From])
        extends ExtendedCompositePrimaryKeyExprn[From] { self =>

      override def render: AliasMapRender[String] =
        self match {
          case PartitionKeyExprn.ComplexAnd(pk, sk) =>
            for {
              pkStr <- pk.render2
              skStr <- sk.render2
            } yield s"$pkStr AND $skStr"
        }

    }

    final case class Equals[From](pk: PartitionKey[From], value: AttributeValue) extends PartitionKeyExprn[From] {
      self =>

      override def render: AliasMapRender[String] =
        self match {
          case PartitionKeyExprn.Equals(pk, value) =>
            AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
        }

    }
  }

  sealed trait SortKeyEprn[From]         { self =>
    def render2: AliasMapRender[String] =
      self match {
        case SortKeyExprn.Equals(sk, value) =>
          AliasMapRender
            .getOrInsert(value)
            .map(v => s"${sk.keyName} = $v")
      }
  }
  sealed trait ExtendedSortKeyEprn[From] { self =>
    def render2: AliasMapRender[String] =
      self match {
        case SortKeyExprn.GreaterThan(sk, value) =>
          AliasMapRender
            .getOrInsert(value)
            .map(v => s"${sk.keyName} > $v")
      }

  }
  object SortKeyExprn {
    final case class SortKey[From](keyName: String) {
      def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): SortKeyEprn[From] = {
        val _ = ev
        Equals[From](this, to.toAttributeValue(value))
      }
      def >[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): ExtendedSortKeyEprn[From] = {
        val _ = ev
        GreaterThan(this, to.toAttributeValue(value))
      }
      // ... and so on for all the other extended operators
    }
    final case class Equals[From](sortKey: SortKey[From], value: AttributeValue) extends SortKeyEprn[From]
    final case class GreaterThan[From](sortKey: SortKey[From], value: AttributeValue) extends ExtendedSortKeyEprn[From]
  }

}

object FooExample extends App {
  import Foo._

  // DynamoDbQuery's still use PrimaryKey
  // typesafe API constructors only expose PartitionKeyEprn
  def asPk[From](k: PartitionKeyExprn[From]): PrimaryKey        =
    k match {
      case PartitionKeyExprn.Equals(pk, value) => PrimaryKey(pk.keyName -> value)
    }
  def asPk[From](k: CompositePrimaryKeyExprn[From]): PrimaryKey =
    k match {
      case PartitionKeyExprn.And(pk, sk) =>
        (pk, sk) match {
          case (PartitionKeyExprn.Equals(pk, value), SortKeyExprn.Equals(sk, value2)) =>
            PrimaryKey(pk.keyName -> value, sk.keyName -> value2)
        }
    }

  def whereKey[From](k: KeyConditionExpression[From]) =
    k match {
      // PartitionKeyExprn
      case PartitionKeyExprn.Equals(pk, value)  => println(s"pk=$pk, value=$value")
      // CompositePrimaryKeyExprn
      case PartitionKeyExprn.And(pk, sk)        => println(s"pk=$pk, sk=$sk")
      // ExtendedCompositePrimaryKeyExprn
      case PartitionKeyExprn.ComplexAnd(pk, sk) => println(s"pk=$pk, sk=$sk")
    }

  // in low level - non type safe land
  import Foo.PartitionKeyExprn._
  import Foo.SortKeyExprn._
  val x1: PartitionKeyExprn[Nothing]                = PartitionKey("email") === "x"
  val x2: SortKeyEprn[Nothing]                      = SortKey("subject") === "y"
  val x3: CompositePrimaryKeyExprn[Nothing]         = x1 && x2
  val x4                                            = PartitionKey[Nothing]("email") === "x" && SortKey[Nothing]("subject") === "y"
  val x5: ExtendedCompositePrimaryKeyExprn[Nothing] =
    PartitionKey[Nothing]("email") === "x" && SortKey[Nothing]("subject") > "y"
//	"type mismatch;\n found   : zio.dynamodb.Foo.SortKeyEprn[Nothing]\n required: zio.dynamodb.Foo.SortKeyEprn[From]\
//  nNote: Nothing <: From, but trait SortKeyEprn is invariant in type From.\nYou may wish to define From as +From instead. (SLS 4.5)",
//  val yy                                           = PartitionKey("email") === "x" && SortKey("subject") === "y"
import zio.dynamodb.ProjectionExpression.$
val x6: CompositePrimaryKeyExprn[Any] = $("foo.bar").primaryKey === "x" && $("foo.baz").sortKey === "y"
val x7: ExtendedCompositePrimaryKeyExprn[Any] = $("foo.bar").primaryKey === "x" && $("foo.baz").sortKey > "y"

  final case class Student(email: String, subject: String, age: Int)
  object Student {
    implicit val schema: Schema.CaseClass3[String, String, Int, Student] = DeriveSchema.gen[Student]
    val (email, subject, age)                                            = ProjectionExpression.accessors[Student]
  }

  val pk: PartitionKeyExprn[Student]                             = Student.email.primaryKey === "x"
  val sk1: SortKeyEprn[Student]                                  = Student.subject.sortKey === "y"
  val sk2: ExtendedSortKeyEprn[Student]                          = Student.subject.sortKey > "y"
  val pkAndSk: CompositePrimaryKeyExprn[Student]                 = Student.email.primaryKey === "x" && Student.subject.sortKey === "y"
  //val three = Student.email.primaryKey === "x" && Student.subject.sortKey === "y" && Student.subject.sortKey // 3 terms not allowed
  val pkAndSkExtended: ExtendedCompositePrimaryKeyExprn[Student] =
    Student.email.primaryKey === "x" && Student.subject.sortKey > "y"

  // GetItem Query will have three overridden versions
  // 1) takes AttrMap/PriamaryKey - for users of low level API
  // 2) takes PartitionKeyExprn - internally this can be converted to AttrMap/PriamaryKey
  // 3) takes CompositePrimaryKeyExprn - internally this can b converted to AttrMap/PriamaryKey

  // whereKey function (for Query)
  // 1) takes a KeyConditionExpression
  // 2) down side is that users of low level API will have to construct case class instances manually - but they have to do that now anyway
  // println(asPk(pkAndSk))
  // println(pkAndSkExtended)

  // Render requirements
  val (aliasMap, s) = pkAndSkExtended.render.execute
  println(s"aliasMap=$aliasMap, s=$s")
}
