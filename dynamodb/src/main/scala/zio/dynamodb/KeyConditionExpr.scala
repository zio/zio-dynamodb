package zio.dynamodb

/**
 * Models:
 * 1) partition key equality expressions
 * 2) composite primary key expressions where sort key expression is equality
 * 3) extended composite primary key expressions where sort key is not equality eg >, <, >=, <=, between, begins_with
 *
 * Note 1), 2) and 3) are all valid key condition expressions
 * BUT only 1) and 2) are valid primary key expressions that can be used in GetItem, UpdateItem and DeleteItem DynamoDB queries
 */
sealed trait KeyConditionExpr[-From, +To] extends Renderable { self =>
  def render: AliasMapRender[String]
}

object KeyConditionExpr {
  def getOrInsert[From, To](primaryKeyName: String): AliasMapRender[String] =
    // note primary keys must be scalar values, they can't be nested
    AliasMapRender.getOrInsert(ProjectionExpression.MapElement[From, To](ProjectionExpression.Root, primaryKeyName))

  private[dynamodb] final case class PartitionKeyEquals[-From, +To](pk: PartitionKey[From, To], value: AttributeValue)
      extends KeyConditionExpr[From, To] { self =>

    def &&[From1 <: From, To2](other: SortKeyEquals[From1, To2]): CompositePrimaryKeyExpr[From1]                    =
      CompositePrimaryKeyExpr[From1](self.asInstanceOf[PartitionKeyEquals[From1, To2]], other)
    def &&[From1 <: From, To2](other: ExtendedSortKeyExpr[From1, To2]): ExtendedCompositePrimaryKeyExpr[From1, To2] =
      ExtendedCompositePrimaryKeyExpr[From1, To2](self.asInstanceOf[PartitionKeyEquals[From1, To2]], other)

    def asAttrMap: AttrMap = AttrMap(pk.keyName -> value)

    override def render: AliasMapRender[String] =
      for {
        v        <- AliasMapRender.getOrInsert(value)
        keyAlias <- KeyConditionExpr.getOrInsert(pk.keyName)
      } yield s"${keyAlias} = $v"

  }

  private[dynamodb] final case class SortKeyEquals[-From, +To](sortKey: SortKey[From, To], value: AttributeValue) {
    self =>
    def miniRender: AliasMapRender[String] =
      for {
        v        <- AliasMapRender.getOrInsert(value)
        keyAlias <- KeyConditionExpr.getOrInsert(sortKey.keyName)
      } yield s"${keyAlias} = $v"
  }

  private[dynamodb] final case class CompositePrimaryKeyExpr[-From](
    pk: PartitionKeyEquals[From, Any],
    sk: SortKeyEquals[From, Any]
  ) extends KeyConditionExpr[From, Any] {
    self =>

    def asAttrMap: AttrMap = PrimaryKey(pk.pk.keyName -> pk.value, sk.sortKey.keyName -> sk.value)

    override def render: AliasMapRender[String] =
      for {
        pkStr <- pk.render
        skStr <- sk.miniRender
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
        skStr <- sk.miniRender
      } yield s"$pkStr AND $skStr"

  }

  sealed trait ExtendedSortKeyExpr[-From, +To] { self =>
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
