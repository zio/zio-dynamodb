package zio.dynamodb

/**
 * This sum type models:
 * 1) partition key equality expressions
 * 2) composite primary key expressions where sort key expression is equality
 * 3) extended composite primary key expressions where sort key is not equality eg >, <, >=, <=, between, begins_with
 *
 * Note 1), 2) and 3) are all valid key condition expressions used in Query DynamoDB queries
 * BUT only 1) and 2) are valid primary key expressions that can be used in GetItem, UpdateItem and DeleteItem DynamoDB queries
 */
sealed trait KeyConditionExpr[-From] extends Renderable { self =>
  def render: AliasMapRender[String]
}

object KeyConditionExpr {
  type SortKeyNotUsed

  sealed trait PrimaryKeyExpr[-From] extends KeyConditionExpr[From] {
    def asAttrMap: AttrMap
  }

  def getOrInsert[From, To](primaryKeyName: String): AliasMapRender[String] =
    // note primary keys must be scalar values, they can't be nested
    AliasMapRender.getOrInsert(ProjectionExpression.MapElement[From, To](ProjectionExpression.Root, primaryKeyName))

  private[dynamodb] final case class PartitionKeyEquals[-From](pk: PartitionKey[From, _], value: AttributeValue)
      extends PrimaryKeyExpr[From] { self =>

    def &&[From1 <: From](other: SortKeyEquals[From1]): CompositePrimaryKeyExpr[From1] =
      CompositePrimaryKeyExpr[From1](self.asInstanceOf[PartitionKeyEquals[From1]], other)
    def &&[From1 <: From, Sk](
      other: ExtendedSortKeyExpr[From1, Sk]
    ): ExtendedCompositePrimaryKeyExpr[From1]                                          =
      ExtendedCompositePrimaryKeyExpr[From1](self.asInstanceOf[PartitionKeyEquals[From1]], other)

    def asAttrMap: AttrMap = AttrMap(pk.keyName -> value)

    override def render: AliasMapRender[String] =
      for {
        v        <- AliasMapRender.getOrInsert(value)
        keyAlias <- KeyConditionExpr.getOrInsert(pk.keyName)
      } yield s"${keyAlias} = $v"

  }

  private[dynamodb] final case class SortKeyEquals[-From](sortKey: SortKey[From, _], value: AttributeValue) {
    self =>
    def miniRender: AliasMapRender[String] =
      for {
        v        <- AliasMapRender.getOrInsert(value)
        keyAlias <- KeyConditionExpr.getOrInsert(sortKey.keyName)
      } yield s"${keyAlias} = $v"
  }

  private[dynamodb] final case class CompositePrimaryKeyExpr[-From](
    pk: PartitionKeyEquals[From],
    sk: SortKeyEquals[From]
  ) extends PrimaryKeyExpr[From] {
    self =>

    def asAttrMap: AttrMap = PrimaryKey(pk.pk.keyName -> pk.value, sk.sortKey.keyName -> sk.value)

    override def render: AliasMapRender[String] =
      for {
        pkStr <- pk.render
        skStr <- sk.miniRender
      } yield s"$pkStr AND $skStr"

  }
  private[dynamodb] final case class ExtendedCompositePrimaryKeyExpr[-From](
    pk: PartitionKeyEquals[From],
    sk: ExtendedSortKeyExpr[From, _]
  ) extends KeyConditionExpr[From] {
    self =>

    def render: AliasMapRender[String] =
      for {
        pkStr <- pk.render
        skStr <- sk.miniRender
      } yield s"$pkStr AND $skStr"

  }

  sealed trait ExtendedSortKeyExpr[-From, +Sk] { self =>
    def miniRender: AliasMapRender[String] =
      self match {
        case ExtendedSortKeyExpr.GreaterThan(sk, value)        =>
          for {
            v        <- AliasMapRender.getOrInsert(value)
            keyAlias <- KeyConditionExpr.getOrInsert(sk.keyName)
          } yield s"${keyAlias} > $v"
        case ExtendedSortKeyExpr.LessThan(sk, value)           =>
          for {
            v        <- AliasMapRender.getOrInsert(value)
            keyAlias <- KeyConditionExpr.getOrInsert(sk.keyName)
          } yield s"${keyAlias} < $v"
        case ExtendedSortKeyExpr.NotEqual(sk, value)           =>
          for {
            v        <- AliasMapRender.getOrInsert(value)
            keyAlias <- KeyConditionExpr.getOrInsert(sk.keyName)
          } yield s"${keyAlias} <> $v"
        case ExtendedSortKeyExpr.LessThanOrEqual(sk, value)    =>
          for {
            v        <- AliasMapRender.getOrInsert(value)
            keyAlias <- KeyConditionExpr.getOrInsert(sk.keyName)
          } yield s"${keyAlias} <= $v"
        case ExtendedSortKeyExpr.GreaterThanOrEqual(sk, value) =>
          for {
            v        <- AliasMapRender.getOrInsert(value)
            keyAlias <- KeyConditionExpr.getOrInsert(sk.keyName)
          } yield s"${keyAlias} >= $v"
        case ExtendedSortKeyExpr.Between(left, min, max)       =>
          for {
            min2     <- AliasMapRender.getOrInsert(min)
            max2     <- AliasMapRender.getOrInsert(max)
            keyAlias <- KeyConditionExpr.getOrInsert(left.keyName)
          } yield s"${keyAlias} BETWEEN $min2 AND $max2"
        case ExtendedSortKeyExpr.BeginsWith(left, value)       =>
          for {
            v        <- AliasMapRender.getOrInsert(value)
            keyAlias <- KeyConditionExpr.getOrInsert(left.keyName)
          } yield s"begins_with(${keyAlias}, $v)"
      }

  }
  object ExtendedSortKeyExpr {
    private[dynamodb] final case class GreaterThan[From, +To](sortKey: SortKey[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class LessThan[From, +To](sortKey: SortKey[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class NotEqual[From, +To](sortKey: SortKey[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class LessThanOrEqual[From, +To](sortKey: SortKey[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class GreaterThanOrEqual[From, +To](sortKey: SortKey[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class Between[From, +To](
      left: SortKey[From, To],
      min: AttributeValue,
      max: AttributeValue
    ) extends ExtendedSortKeyExpr[From, To]
    private[dynamodb] final case class BeginsWith[From, +To](left: SortKey[From, To], value: AttributeValue)
        extends ExtendedSortKeyExpr[From, To]
  }

}
