package zio.dynamodb

/*
KeyCondition expression is a restricted version of ConditionExpression where by
- partition exprn is required and can only use "=" equals comparison
- optionally AND can be used to add a sort key expression

eg partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval
comparisons operators are the same as for Condition

 */

// introduce a case class called variable aliases
final case class AliasMap private (map: Map[AttributeValue, String], index: Int = 0) { self =>
  // REVIEW: Is this a reasonable interface?
  def +(entry: AttributeValue): (AliasMap, String) = {
    // AWS expects variables to all start with `:`, and have their keys in the expressionAttributesValues map start with it as well
    val nextVariable = s":v${self.index}"
    (AliasMap(self.map + ((entry, nextVariable)), self.index + 1), nextVariable)
  }

}

object AliasMap {
  def empty: AliasMap = AliasMap(Map.empty, 0)

}

final case class AliasMapRender[+A](
  render: AliasMap => (AliasMap, A)
) // add map, flatmap, succeed and necessary monads
// All renders can just return an AliasMapRender of string

sealed trait KeyConditionExpression { self =>
  /*
  render will make new variables if it doesn't see an alias for a variable
  v0, v1, v2, v3, ....
  starting off with AliasMap.empty
  we're going to return the String for the KeyConditionExpression

  ExpressionAttributeMap will be generated based on the final AliasMap that render returns

   */
  def render(aliasMap: AliasMap): (AliasMap, String) =
    self match {
      case KeyConditionExpression.And(_, _)   => ???
      case expression: PartitionKeyExpression => expression.render(aliasMap)
    }
}

object KeyConditionExpression {
  private[dynamodb] final case class And(left: PartitionKeyExpression, right: SortKeyExpression)
      extends KeyConditionExpression
}

sealed trait PartitionKeyExpression extends KeyConditionExpression { self =>
  import KeyConditionExpression.And

  def &&(that: SortKeyExpression): KeyConditionExpression = And(self, that)

  override def render(aliasMap: AliasMap): (AliasMap, String) =
    self match {
      case PartitionKeyExpression.Equals(left, right) =>
        aliasMap.map
          .get(right)
          .map(value => (aliasMap, s"${left.keyName} = $value"))
          .getOrElse({
            val (nextMap, variableName) = aliasMap + right
            (nextMap, s"${left.keyName} = $variableName")
          })
    }
}
object PartitionKeyExpression {
  final case class PartitionKey(keyName: String) { self =>
    def ===[A](that: A)(implicit t: ToAttributeValue[A]): PartitionKeyExpression =
      Equals(self, t.toAttributeValue(that))
  }
  final case class Equals(left: PartitionKey, right: AttributeValue) extends PartitionKeyExpression
}

sealed trait SortKeyExpression { self =>
  def render(aliasMap: AliasMap): (AliasMap, String) =
    self match {
      case SortKeyExpression.Equals(left, right) =>
        aliasMap.map
          .get(right)
          .map(value => (aliasMap, s"${left.keyName} = $value"))
          .getOrElse {
            val (nextMap, variableName) = aliasMap + right
            (nextMap, s"${left.keyName} = $variableName")
          }
      case _                                     => ???
//      case SortKeyExpression.NotEqual(left, right)           => ??? //s"${left.keyName} != :${right.render()}"
//      case SortKeyExpression.LessThan(left, right)           => ??? //s"${left.keyName} < :${right.render()}"
//      case SortKeyExpression.GreaterThan(left, right)        => ??? //s"${left.keyName} > :${right.render()}"
//      case SortKeyExpression.LessThanOrEqual(left, right)    => ??? //s"${left.keyName} <= :${right.render()}"
//      case SortKeyExpression.GreaterThanOrEqual(left, right) => ??? //s"${left.keyName} >= :${right.render()}"
//      case SortKeyExpression.Between(left, min, max)         =>
//        ??? //s"${left.keyName} BETWEEN :${min.render()} AND :${max.render()}"
//      case SortKeyExpression.BeginsWith(left, value)         => ??? //s"begins_with ( ${left.keyName}, :${value.render()})"
    }
}

object SortKeyExpression {

  final case class SortKey(keyName: String) { self =>
    def ===[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression            = Equals(self, t.toAttributeValue(that))
    def <>[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression             = NotEqual(self, t.toAttributeValue(that))
    def <[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression              = LessThan(self, t.toAttributeValue(that))
    def <=[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression             =
      LessThanOrEqual(self, t.toAttributeValue(that))
    def >[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression              =
      GreaterThanOrEqual(self, t.toAttributeValue(that))
    def >=[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression             =
      GreaterThanOrEqual(self, t.toAttributeValue(that))
    def between[A](min: A, max: A)(implicit t: ToAttributeValue[A]): SortKeyExpression =
      Between(self, t.toAttributeValue(min), t.toAttributeValue(max))
    def beginsWith[A](value: A)(implicit t: ToAttributeValue[A]): SortKeyExpression    =
      BeginsWith(self, t.toAttributeValue(value))
  }

  private[dynamodb] final case class Equals(left: SortKey, right: AttributeValue)             extends SortKeyExpression
  private[dynamodb] final case class NotEqual(left: SortKey, right: AttributeValue)           extends SortKeyExpression
  private[dynamodb] final case class LessThan(left: SortKey, right: AttributeValue)           extends SortKeyExpression
  private[dynamodb] final case class GreaterThan(left: SortKey, right: AttributeValue)        extends SortKeyExpression
  private[dynamodb] final case class LessThanOrEqual(left: SortKey, right: AttributeValue)    extends SortKeyExpression
  private[dynamodb] final case class GreaterThanOrEqual(left: SortKey, right: AttributeValue) extends SortKeyExpression
  private[dynamodb] final case class Between(left: SortKey, min: AttributeValue, max: AttributeValue)
      extends SortKeyExpression
  private[dynamodb] final case class BeginsWith(left: SortKey, value: AttributeValue)         extends SortKeyExpression
}
