package zio.dynamodb

import zio.dynamodb.PrimaryKey

/**
 * Typesafe KeyConditionExpr/primary key experiment
 *
 * TODO break out files
 * - PartitionKey2.scala
 * - SortKey2.scala
 * - KeyConditionExpr.scala
 */
import zio.dynamodb.KeyConditionExpr.PartitionKeyExpr
import zio.dynamodb.proofs.RefersTo
import zio.dynamodb.proofs.CanSortKeyBeginsWith

// belongs to the package top level
private[dynamodb] final case class PartitionKey2[-From, To](keyName: String) { self =>
  def ===[To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To, To2]): PartitionKeyExpr[From, To] = {
    val _ = ev
    PartitionKeyExpr(self, implicitly[ToAttributeValue[To2]].toAttributeValue(value))
  }
}

import zio.dynamodb.KeyConditionExpr.SortKeyExpr
import zio.dynamodb.KeyConditionExpr.ExtendedSortKeyExpr

private[dynamodb] final case class SortKey2[-From, To](keyName: String) { self =>
  // all comparion ops apply to: Strings, Numbers, Binary values
  def ===[To2: ToAttributeValue, IsPrimaryKey](value: To2)(implicit ev: RefersTo[To, To2]): SortKeyExpr[From, To2] = {
    val _ = ev
    SortKeyExpr[From, To2](
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly[ToAttributeValue[To2]].toAttributeValue(value)
    )
  }
  def >[To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To, To2]): ExtendedSortKeyExpr[From, To2] = {
    val _ = ev
    ExtendedSortKeyExpr.GreaterThan(
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly(ToAttributeValue[To2]).toAttributeValue(value)
    )
  }
  def <[To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To, To2]): ExtendedSortKeyExpr[From, To2] = {
    val _ = ev
    ExtendedSortKeyExpr.LessThan(
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly[ToAttributeValue[To2]].toAttributeValue(value)
    )
  }
  def <>[To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To, To2]): ExtendedSortKeyExpr[From, To2] = {
    val _ = ev
    ExtendedSortKeyExpr.NotEqual(
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly(ToAttributeValue[To2]).toAttributeValue(value)
    )
  }
  def <=[To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To, To2]): ExtendedSortKeyExpr[From, To2] = {
    val _ = ev
    ExtendedSortKeyExpr.LessThanOrEqual(
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly[ToAttributeValue[To2]].toAttributeValue(value)
    )
  }
  def >=[To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To, To2]): ExtendedSortKeyExpr[From, To2] = {
    val _ = ev
    ExtendedSortKeyExpr.GreaterThanOrEqual(
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly[ToAttributeValue[To2]].toAttributeValue(value)
    )
  }
  // applies to all PK types
  def between[To: ToAttributeValue, IsPrimaryKey](min: To, max: To): ExtendedSortKeyExpr[From, To] =
    ExtendedSortKeyExpr.Between[From, To](
      self.asInstanceOf[SortKey2[From, To]],
      implicitly[ToAttributeValue[To]].toAttributeValue(min),
      implicitly[ToAttributeValue[To]].toAttributeValue(max)
    )

/*
    /**
     * Applies to a String or Set
     */
    def contains[A](av: A)(implicit ev: Containable[To, A], to: ToAttributeValue[A]): ConditionExpression[From] = {
      val _ = ev
      ConditionExpression.Contains(self, to.toAttributeValue(av))
    }

*/
  // beginsWith applies to: Strings, Binary values
  def beginsWith[To2: ToAttributeValue, IsPrimaryKey](prefix: To2)(implicit ev: CanSortKeyBeginsWith[To, To2]): ExtendedSortKeyExpr[From, To2]    = {
    val _ = ev
    ExtendedSortKeyExpr.BeginsWith[From, To2](
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly[ToAttributeValue[To2]].toAttributeValue(prefix)
    )
  }

  // // beginsWith applies to: Strings, Binary values
  // def beginsWith[To: ToAttributeValue, IsPrimaryKey](prefix: To)(implicit ev: RefersTo[String, To]): ExtendedSortKeyExpr[From, To]    = {
  //   val _ = ev
  //   ExtendedSortKeyExpr.BeginsWith[From, To](
  //     self.asInstanceOf[SortKey2[From, To]],
  //     implicitly[ToAttributeValue[To]].toAttributeValue(prefix)
  //   )
  // }

}

sealed trait KeyConditionExpr[-From] extends Renderable { self =>
  def render: AliasMapRender[String]
}

object KeyConditionExpr {
  // models primary key expressions
  // email.primaryKey === "x"
  // Student.email.primaryKey === "x" && Student.subject.sortKey === "y"

  private[dynamodb] final case class PartitionKeyExpr[-From, To](pk: PartitionKey2[From, To], value: AttributeValue)
      extends KeyConditionExpr[From] { self =>

    def &&[From1 <: From, To2](other: SortKeyExpr[From1, To2]): CompositePrimaryKeyExpr[From1, To2]                 =
      CompositePrimaryKeyExpr[From1, To2](self.asInstanceOf[PartitionKeyExpr[From1, To2]], other)
    def &&[From1 <: From, To2](other: ExtendedSortKeyExpr[From1, To2]): ExtendedCompositePrimaryKeyExpr[From1, To2] =
      ExtendedCompositePrimaryKeyExpr[From1, To2](self.asInstanceOf[PartitionKeyExpr[From1, To2]], other)

    def asAttrMap: AttrMap = AttrMap(pk.keyName -> value)

    override def render: AliasMapRender[String] =
      AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
  }

  private[dynamodb] final case class SortKeyExpr[-From, To](sortKey: SortKey2[From, To], value: AttributeValue) {
    self =>
    def render2: AliasMapRender[String] =
      AliasMapRender
        .getOrInsert(value)
        .map(v => s"${sortKey.keyName} = $v")
  }

  private[dynamodb] final case class CompositePrimaryKeyExpr[-From, To](
    pk: PartitionKeyExpr[From, To],
    sk: SortKeyExpr[From, To]
  ) extends KeyConditionExpr[From] {
    self =>

    def asAttrMap: AttrMap = PrimaryKey(pk.pk.keyName -> pk.value, sk.sortKey.keyName -> sk.value)

    override def render: AliasMapRender[String] =
      for {
        pkStr <- pk.render
        skStr <- sk.render2
      } yield s"$pkStr AND $skStr"

  }
  private[dynamodb] final case class ExtendedCompositePrimaryKeyExpr[-From, To](
    pk: PartitionKeyExpr[From, To],
    sk: ExtendedSortKeyExpr[From, To]
  ) extends KeyConditionExpr[From] {
    self =>

    def render: AliasMapRender[String] =
      for {
        pkStr <- pk.render
        skStr <- sk.render2
      } yield s"$pkStr AND $skStr"

  }

  sealed trait ExtendedSortKeyExpr[-From, To] { self =>
    def render2: AliasMapRender[String] =
      self match {
        case ExtendedSortKeyExpr.GreaterThan(sk, value)        =>
          AliasMapRender
            .getOrInsert(value)
            .map(v => s"${sk.keyName} > $v")
        case ExtendedSortKeyExpr.LessThan(sk, value)           =>
          AliasMapRender
            .getOrInsert(value)
            .map(v => s"${sk.keyName} < $v")
        case ExtendedSortKeyExpr.NotEqual(sk, value)           =>
          AliasMapRender
            .getOrInsert(value)
            .map(v => s"${sk.keyName} <> $v")
        case ExtendedSortKeyExpr.LessThanOrEqual(sk, value)    =>
          AliasMapRender
            .getOrInsert(value)
            .map(v => s"${sk.keyName} <= $v")
        case ExtendedSortKeyExpr.GreaterThanOrEqual(sk, value) =>
          AliasMapRender
            .getOrInsert(value)
            .map(v => s"${sk.keyName} >= $v")
        case ExtendedSortKeyExpr.Between(left, min, max)       =>
          AliasMapRender
            .getOrInsert(min)
            .flatMap(min =>
              AliasMapRender.getOrInsert(max).map { max =>
                s"${left.keyName} BETWEEN $min AND $max"
              }
            )
        case ExtendedSortKeyExpr.BeginsWith(left, value)       =>
          AliasMapRender
            .getOrInsert(value)
            .map { v =>
              s"begins_with(${left.keyName}, $v)"
            }
      }

  }
  object ExtendedSortKeyExpr {
    private[dynamodb] final case class GreaterThan[From, To](sortKey: SortKey2[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class LessThan[From, To](sortKey: SortKey2[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class NotEqual[From, To](sortKey: SortKey2[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class LessThanOrEqual[From, To](sortKey: SortKey2[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class GreaterThanOrEqual[From, To](sortKey: SortKey2[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class Between[From, To](
      left: SortKey2[From, To],
      min: AttributeValue,
      max: AttributeValue
    ) extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class BeginsWith[From, To](left: SortKey2[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
  }

}
