package zio.dynamodb

import zio.dynamodb.PrimaryKey

/**
 * Typesafe KeyConditionExpr/primary key experiment
 */
sealed trait KeyConditionExpr[-From, +To] extends Renderable { self =>
  def render: AliasMapRender[String]
}

object KeyConditionExpr {
  // models primary key expressions
  // email.primaryKey === "x"
  // Student.email.primaryKey === "x" && Student.subject.sortKey === "y"

  private[dynamodb] final case class PartitionKeyEquals[-From, +To](pk: PartitionKey2[From, To], value: AttributeValue)
      extends KeyConditionExpr[From, To] { self =>

    def &&[From1 <: From, To2](other: SortKeyEquals[From1, To2]): CompositePrimaryKeyExpr[From1, To2]               =
      CompositePrimaryKeyExpr[From1, To2](self.asInstanceOf[PartitionKeyEquals[From1, To2]], other)
    def &&[From1 <: From, To2](other: ExtendedSortKeyExpr[From1, To2]): ExtendedCompositePrimaryKeyExpr[From1, To2] =
      ExtendedCompositePrimaryKeyExpr[From1, To2](self.asInstanceOf[PartitionKeyEquals[From1, To2]], other)

    def asAttrMap: AttrMap = AttrMap(pk.keyName -> value)

    override def render: AliasMapRender[String] =
      AliasMapRender.getOrInsert(value).map(v => s"${pk.keyName} = $v")
  }

  private[dynamodb] final case class SortKeyEquals[-From, +To](sortKey: SortKey2[From, To], value: AttributeValue) {
    self =>
    def render2: AliasMapRender[String] =
      AliasMapRender
        .getOrInsert(value)
        .map(v => s"${sortKey.keyName} = $v")
  }

  private[dynamodb] final case class CompositePrimaryKeyExpr[-From, +To](
    pk: PartitionKeyEquals[From, To],
    sk: SortKeyEquals[From, To]
  ) extends KeyConditionExpr[From, To] {
    self =>

    def asAttrMap: AttrMap = PrimaryKey(pk.pk.keyName -> pk.value, sk.sortKey.keyName -> sk.value)

    override def render: AliasMapRender[String] =
      for {
        pkStr <- pk.render
        skStr <- sk.render2
      } yield s"$pkStr AND $skStr"

  }
  private[dynamodb] final case class ExtendedCompositePrimaryKeyExpr[-From, +To](
    pk: PartitionKeyEquals[From, To],
    sk: ExtendedSortKeyExpr[From, To]
  ) extends KeyConditionExpr[From, To] {
    self =>

    def render: AliasMapRender[String] =
      for {
        pkStr <- pk.render
        skStr <- sk.render2
      } yield s"$pkStr AND $skStr"

  }

  sealed trait ExtendedSortKeyExpr[-From, +To] { self =>
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
    private[dynamodb] final case class GreaterThan[From, +To](sortKey: SortKey2[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class LessThan[From, +To](sortKey: SortKey2[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class NotEqual[From, +To](sortKey: SortKey2[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class LessThanOrEqual[From, +To](sortKey: SortKey2[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class GreaterThanOrEqual[From, +To](sortKey: SortKey2[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class Between[From, +To](
      left: SortKey2[From, To],
      min: AttributeValue,
      max: AttributeValue
    ) extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class BeginsWith[From, +To](left: SortKey2[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
  }

}
