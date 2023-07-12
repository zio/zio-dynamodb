package zio.dynamodb

import zio.dynamodb.PrimaryKey
import zio.dynamodb.proofs.IsPrimaryKey

/**
 * Typesafe KeyConditionExpr/primary key experiment
 *
 * TODO break out files
 * - PartitionKey2.scala
 * - SortKey2.scala
 * - KeyConditionExpr.scala
 */
import zio.dynamodb.KeyConditionExpr.PartitionKeyExpr

// belongs to the package top level
private[dynamodb] final case class PartitionKey2[-From](keyName: String) { self =>
  def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): PartitionKeyExpr[From] = {
    val _ = ev
    PartitionKeyExpr(self, to.toAttributeValue(value))
  }
}

import zio.dynamodb.KeyConditionExpr.SortKeyExpr
import zio.dynamodb.KeyConditionExpr.ExtendedSortKeyExpr

private[dynamodb] final case class SortKey2[From](keyName: String) { self =>
  def ===[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): SortKeyExpr[From] = {
    val _ = ev
    SortKeyExpr[From](self, to.toAttributeValue(value))
  }
  def >[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): ExtendedSortKeyExpr[From] = {
    val _ = ev
    ExtendedSortKeyExpr.GreaterThan(self, to.toAttributeValue(value))
  }
  // def <[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): ExtendedSortKeyExpr[From] = {
  //   val _ = ev
  //   ExtendedSortKeyExpr.LessThan(self, to.toAttributeValue(value))
  // }
  // def <>[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): ExtendedSortKeyExpr[From] = {
  //   val _ = ev
  //   ExtendedSortKeyExpr.NotEqual(self, to.toAttributeValue(value))
  // }
  // def <=[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): ExtendedSortKeyExpr[From] = {
  //   val _ = ev
  //   ExtendedSortKeyExpr.LessThanOrEqual(self, to.toAttributeValue(value))
  // }
  // def >=[To](value: To)(implicit to: ToAttributeValue[To], ev: IsPrimaryKey[To]): ExtendedSortKeyExpr[From] = {
  //   val _ = ev
  //   ExtendedSortKeyExpr.GreaterThanOrEqual(self, to.toAttributeValue(value))
  // }
  // def between[To](min: To, max: To)(implicit t: ToAttributeValue[To]): ExtendedSortKeyExpr[From] =
  //   ExtendedSortKeyExpr.Between[From](self, t.toAttributeValue(min), t.toAttributeValue(max))
}

sealed trait KeyConditionExpr[-From] extends Renderable { self =>
  def render: AliasMapRender[String]
}

object KeyConditionExpr {
  // models primary key expressions
  // email.primaryKey === "x"
  // Student.email.primaryKey === "x" && Student.subject.sortKey === "y"

  private[dynamodb] final case class PartitionKeyExpr[-From](pk: PartitionKey2[From], value: AttributeValue)
      extends KeyConditionExpr[From] { self =>

    def &&[From1 <: From](other: SortKeyExpr[From1]): CompositePrimaryKeyExpr[From1]                 =
      CompositePrimaryKeyExpr[From1](self, other)
    def &&[From1 <: From](other: ExtendedSortKeyExpr[From1]): ExtendedCompositePrimaryKeyExpr[From1] =
      ExtendedCompositePrimaryKeyExpr[From1](self, other)

    def asAttrMap: AttrMap = AttrMap(pk.keyName -> value)

    override def render: AliasMapRender[String] =
      AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
  }

  private[dynamodb] final case class SortKeyExpr[From](sortKey: SortKey2[From], value: AttributeValue) { self =>
    def render2: AliasMapRender[String] =
      AliasMapRender
        .getOrInsert(value)
        .map(v => s"${sortKey.keyName} = $v")
  }

  private[dynamodb] final case class CompositePrimaryKeyExpr[From](pk: PartitionKeyExpr[From], sk: SortKeyExpr[From])
      extends KeyConditionExpr[From] {
    self =>

    def asAttrMap: AttrMap = PrimaryKey(pk.pk.keyName -> pk.value, sk.sortKey.keyName -> sk.value)

    override def render: AliasMapRender[String] =
      for {
        pkStr <- pk.render
        skStr <- sk.render2
      } yield s"$pkStr AND $skStr"

  }
  private[dynamodb] final case class ExtendedCompositePrimaryKeyExpr[-From](
    pk: PartitionKeyExpr[From],
    sk: ExtendedSortKeyExpr[From]
  ) extends KeyConditionExpr[From] {
    self =>

    def render: AliasMapRender[String] =
      for {
        pkStr <- pk.render
        skStr <- sk.render2
      } yield s"$pkStr AND $skStr"

  }

  sealed trait ExtendedSortKeyExpr[-From] { self =>
    def render2: AliasMapRender[String] =
      self match {
        case ExtendedSortKeyExpr.GreaterThan(sk, value) =>
          AliasMapRender
            .getOrInsert(value)
            .map(v => s"${sk.keyName} > $v")
        // case ExtendedSortKeyExpr.LessThan(sk, value)           =>
        //   AliasMapRender
        //     .getOrInsert(value)
        //     .map(v => s"${sk.keyName} < $v")
        // case ExtendedSortKeyExpr.NotEqual(sk, value)           =>
        //   AliasMapRender
        //     .getOrInsert(value)
        //     .map(v => s"${sk.keyName} <> $v")
        // case ExtendedSortKeyExpr.LessThanOrEqual(sk, value)    =>
        //   AliasMapRender
        //     .getOrInsert(value)
        //     .map(v => s"${sk.keyName} <= $v")
        // case ExtendedSortKeyExpr.GreaterThanOrEqual(sk, value) =>
        //   AliasMapRender
        //     .getOrInsert(value)
        //     .map(v => s"${sk.keyName} >= $v")
        // case ExtendedSortKeyExpr.Between(left, min, max)       =>
        //   AliasMapRender
        //     .getOrInsert(min)
        //     .flatMap(min =>
        //       AliasMapRender.getOrInsert(max).map { max =>
        //         s"${left.keyName} BETWEEN $min AND $max"
        //       }
        //     )
        // case ExtendedSortKeyExpr.BeginsWith(left, value)       =>
        //   AliasMapRender
        //     .getOrInsert(value)
        //     .map { v =>
        //       s"begins_with(${left.keyName}, $v)"
        //     }
      }

  }
  object ExtendedSortKeyExpr {
    private[dynamodb] final case class GreaterThan[From](sortKey: SortKey2[From], value: AttributeValue)
        extends ExtendedSortKeyExpr[From]
    // private[dynamodb] final case class LessThan[From](sortKey: SortKey2[From], value: AttributeValue)
    //     extends ExtendedSortKeyExpr[From]
    // private[dynamodb] final case class NotEqual[From](sortKey: SortKey2[From], value: AttributeValue)
    //     extends ExtendedSortKeyExpr[From]
    // private[dynamodb] final case class LessThanOrEqual[From](sortKey: SortKey2[From], value: AttributeValue)
    //     extends ExtendedSortKeyExpr[From]
    // private[dynamodb] final case class GreaterThanOrEqual[From](sortKey: SortKey2[From], value: AttributeValue)
    //     extends ExtendedSortKeyExpr[From]
    // private[dynamodb] final case class Between[From](left: SortKey2[From], min: AttributeValue, max: AttributeValue)
    //     extends ExtendedSortKeyExpr[From]
    // private[dynamodb] final case class BeginsWith[From](left: SortKey2[From], value: AttributeValue)
    //     extends ExtendedSortKeyExpr[From]
  }

}
