package zio.dynamodb

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

Files
- KeyConditionExpr.scala
- PartitionKey2.scala
- SortKey2.scala
- KeyConditionExpr.scala
- PartitionKeyExpr.scala
- CompositePrimaryKeyExpr.scala
- ExtendedCompositePrimaryKeyExpr.scala
- SortKeyExpr.scala
- ExtendedSortKeyExpr.scala
 */

import zio.dynamodb.KeyConditionExpr.PartitionKeyExpr

// belongs to the package top level
final case class PartitionKey2[From](keyName: String) {
  def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): PartitionKeyExpr[From] = {
    val _ = ev
    PartitionKeyExpr.Equals(this, to.toAttributeValue(value))
  }
}

import zio.dynamodb.KeyConditionExpr.SortKeyExpr
import zio.dynamodb.KeyConditionExpr.ExtendedSortKeyExpr

final case class SortKey2[From](keyName: String) {
  def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): SortKeyExpr[From] = {
    val _ = ev
    SortKeyExpr[From](this, to.toAttributeValue(value))
  }
  def >[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): ExtendedSortKeyExpr[From] = {
    val _ = ev
    ExtendedSortKeyExpr.GreaterThan(this, to.toAttributeValue(value))
  }
  // ... and so on for all the other extended operators
}

sealed trait KeyConditionExpr[-From] extends Renderable { self =>
  def render: AliasMapRender[String]
}

object KeyConditionExpr {
  // models primary key expressions
  // email.primaryKey === "x"
  // Student.email.primaryKey === "x" && Student.subject.sortKey === "y"
  sealed trait PartitionKeyExpr[-From] extends KeyConditionExpr[From] { self =>
    def &&[From1 <: From](other: SortKeyExpr[From1]): CompositePrimaryKeyExpr[From1]                 =
      CompositePrimaryKeyExpr[From1](self, other)
    def &&[From1 <: From](other: ExtendedSortKeyExpr[From1]): ExtendedCompositePrimaryKeyExpr[From1] =
      ExtendedCompositePrimaryKeyExpr[From1](self, other)

    def render2: AliasMapRender[String] =
      self match {
        case PartitionKeyExpr.Equals(pk, value) =>
          AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
      }

    def asAttrMap: AttrMap =
      self match {
        case PartitionKeyExpr.Equals(pk, value) => AttrMap(pk.keyName -> value)
      }

    def render: AliasMapRender[String] =
      self match {
        case PartitionKeyExpr.Equals(pk, value) =>
          AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
      }
  }
  object PartitionKeyExpr {

    final case class Equals[From](pk: PartitionKey2[From], value: AttributeValue) extends PartitionKeyExpr[From] {
      self =>

      override def render: AliasMapRender[String] =
        self match {
          case PartitionKeyExpr.Equals(pk, value) =>
            AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
        }

    }
  }

  final case class SortKeyExpr[From](sortKey: SortKey2[From], value: AttributeValue) { self =>
    def render2: AliasMapRender[String] =
      AliasMapRender
        .getOrInsert(value)
        .map(v => s"${sortKey.keyName} = $v")
  }

  final case class CompositePrimaryKeyExpr[From](pk: PartitionKeyExpr[From], sk: SortKeyExpr[From])
      extends KeyConditionExpr[From] {
    self =>

    def asAttrVal: PrimaryKey =
      self match { // TODO: delete match
        case CompositePrimaryKeyExpr(pk, sk) =>
          (pk, sk) match {
            case (PartitionKeyExpr.Equals(pk, value), SortKeyExpr(sk, value2)) =>
              PrimaryKey(pk.keyName -> value, sk.keyName -> value2)
          }
      }

    override def render: AliasMapRender[String] =
      for {
        pkStr <- pk.render2
        skStr <- sk.render2
      } yield s"$pkStr AND $skStr"

  }
  final case class ExtendedCompositePrimaryKeyExpr[-From](pk: PartitionKeyExpr[From], sk: ExtendedSortKeyExpr[From])
      extends KeyConditionExpr[From] {
    self =>

    def render: AliasMapRender[String] =
      for {
        pkStr <- pk.render2
        skStr <- sk.render2
      } yield s"$pkStr AND $skStr"

  }
  // no overlap between PartitionKeyExpr and ExtendedPartitionKeyExpr

  // single member sealed trait ATM but will have more members
  sealed trait ExtendedSortKeyExpr[-From] { self =>
    def render2: AliasMapRender[String] =
      self match {
        case ExtendedSortKeyExpr.GreaterThan(sk, value) =>
          AliasMapRender
            .getOrInsert(value)
            .map(v => s"${sk.keyName} > $v")
      }

  }
  object ExtendedSortKeyExpr {
    final case class GreaterThan[From](sortKey: SortKey2[From], value: AttributeValue) extends ExtendedSortKeyExpr[From]
    // TODO add other extended operators
  }

}
