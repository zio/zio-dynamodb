package zio.dynamodb

import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.dynamodb.PrimaryKey
import zio.dynamodb.proofs.IsPrimaryKey

/**
 * Typesafe KeyConditionExpression/primary key experiment
 */
/*
TODO
- consider collapsing 1 member sealed traits
- make raw constrctors that contain Phamtom types private
  - expose helper methods for them
 */
object Foo {
  sealed trait KeyConditionExpr[-From] extends Renderable { self =>
    def render: AliasMapRender[String]
  }
  object KeyConditionExpr {}
  // models primary key expressions
  // email.primaryKey === "x"
  // Student.email.primaryKey === "x" && Student.subject.sortKey === "y"
  sealed trait PartitionKeyExpr[-From] extends KeyConditionExpr[From] { self =>
    def &&[From1 <: From](other: SortKeyEprn[From1]): CompositePrimaryKeyExpr[From1]                 =
      PartitionKeyExpr.And[From1](self, other)
    def &&[From1 <: From](other: ExtendedSortKeyEprn[From1]): ExtendedCompositePrimaryKeyExpr[From1] =
      PartitionKeyExpr.ComplexAnd[From1](self, other)

    def render2: AliasMapRender[String] =
      self match {
        case PartitionKeyExpr.Equals(pk, value) =>
          AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
      }

    def render: AliasMapRender[String] =
      self match {
        case PartitionKeyExpr.Equals(pk, value) =>
          AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
      }
  }

  // sealed trait has only one member but it is usefull as a type alias to guide the user
  // for instance And extends it - but having a type called And would make no sense
  sealed trait CompositePrimaryKeyExpr[-From] extends KeyConditionExpr[From] { self => }

  // sealed trait has only one member but it is usefull as a type alias to guide the user
  // models "extended" primary key expressions
  // Student.email.primaryKey === "x" && Student.subject.sortKey > "y"
  sealed trait ExtendedCompositePrimaryKeyExpr[-From] extends KeyConditionExpr[From] { self => }

  object ExtendedCompositePrimaryKeyExpr {
    // TODO move ComplexAnd here
  }

  // no overlap between PartitionKeyExpr and ExtendedPartitionKeyExpr

  object PartitionKeyExpr {
    // belongs to the package top level
    final case class PartitionKey[From](keyName: String) {
      def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): PartitionKeyExpr[From] = {
        val _ = ev
        Equals(this, to.toAttributeValue(value))
      }
    }

    // TODO move to companion object
    final case class And[From](pk: PartitionKeyExpr[From], sk: SortKeyEprn[From])
        extends CompositePrimaryKeyExpr[From] {
      self =>

      override def render: AliasMapRender[String] =
        self match {
          case PartitionKeyExpr.And(pk, sk) =>
            for {
              pkStr <- pk.render2
              skStr <- sk.render2
            } yield s"$pkStr AND $skStr"
        }

      // do render in concrete classes

    }

    // TODO move to companion object
    final case class ComplexAnd[-From](pk: PartitionKeyExpr[From], sk: ExtendedSortKeyEprn[From])
        extends ExtendedCompositePrimaryKeyExpr[From] { self =>

      override def render: AliasMapRender[String] =
        self match {
          case PartitionKeyExpr.ComplexAnd(pk, sk) =>
            for {
              pkStr <- pk.render2
              skStr <- sk.render2
            } yield s"$pkStr AND $skStr"
        }

    }

    final case class Equals[From](pk: PartitionKey[From], value: AttributeValue) extends PartitionKeyExpr[From] {
      self =>

      override def render: AliasMapRender[String] =
        self match {
          case PartitionKeyExpr.Equals(pk, value) =>
            AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
        }

    }
  }

  sealed trait SortKeyEprn[-From]         { self =>
    def render2: AliasMapRender[String] =
      self match {
        case SortKeyExpr.Equals(sk, value) =>
          AliasMapRender
            .getOrInsert(value)
            .map(v => s"${sk.keyName} = $v")
      }
  }
  sealed trait ExtendedSortKeyEprn[-From] { self =>
    def render2: AliasMapRender[String] =
      self match {
        case SortKeyExpr.GreaterThan(sk, value) =>
          AliasMapRender
            .getOrInsert(value)
            .map(v => s"${sk.keyName} > $v")
      }

  }
  object SortKeyExpr {
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

  // TODO: move to PartitionKeyExpr
  def asPk[From](k: PartitionKeyExpr[From]): PrimaryKey        =
    k match {
      case PartitionKeyExpr.Equals(pk, value) => PrimaryKey(pk.keyName -> value)
    }
  def asPk[From](k: CompositePrimaryKeyExpr[From]): PrimaryKey =
    k match {
      case PartitionKeyExpr.And(pk, sk) =>
        (pk, sk) match {
          case (PartitionKeyExpr.Equals(pk, value), SortKeyExpr.Equals(sk, value2)) =>
            PrimaryKey(pk.keyName -> value, sk.keyName -> value2)
        }
    }

  def whereKey[From](k: KeyConditionExpr[From]) =
    k match {
      // PartitionKeyExpr
      case PartitionKeyExpr.Equals(pk, value)  => println(s"pk=$pk, value=$value")
      // CompositePrimaryKeyExpr
      case PartitionKeyExpr.And(pk, sk)        => println(s"pk=$pk, sk=$sk")
      // ExtendedCompositePrimaryKeyExpr
      case PartitionKeyExpr.ComplexAnd(pk, sk) => println(s"pk=$pk, sk=$sk")
    }

  // in low level - non type safe land
  import Foo.PartitionKeyExpr._
  import Foo.SortKeyExpr._
  val x1: PartitionKeyExpr[Nothing]                = PartitionKey("email") === "x"
  val x2: SortKeyEprn[Nothing]                     = SortKey("subject") === "y"
  val x3: CompositePrimaryKeyExpr[Nothing]         = x1 && x2
  val x4                                           = PartitionKey[Nothing]("email") === "x" && SortKey[Nothing]("subject") === "y"
  val x5: ExtendedCompositePrimaryKeyExpr[Nothing] =
    PartitionKey[Nothing]("email") === "x" && SortKey[Nothing]("subject") > "y"

  val y0: PartitionKeyExpr[Any]                = PartitionKey("email") === "x"
  val y1: CompositePrimaryKeyExpr[Any]         = PartitionKey("email") === "x" && SortKey("subject") === "y"
  val y2: ExtendedCompositePrimaryKeyExpr[Any] = PartitionKey("email") === "x" && SortKey("subject") > "y"

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
  val sk2: ExtendedSortKeyEprn[Student]                         = Student.subject.sortKey > "y"
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
