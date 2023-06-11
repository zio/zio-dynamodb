package zio.dynamodb

import zio.dynamodb.PartitionKeyExpression2.PartitionKey
import zio.dynamodb.SortKeyExpression2.SortKey

/*
KeyCondition expression is a restricted version of ConditionExpression where by
- partition exprn is required and can only use "=" equals comparison
- optionally AND can be used to add a sort key expression

eg partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval
comparisons operators are the same as for Condition

 */

sealed trait KeyConditionExpression2 extends Renderable { self =>
  def render: AliasMapRender[String] =
    self match {
      case KeyConditionExpression2.And(left, right) =>
        left.render
          .zipWith(
            right.render
          ) { case (l, r) => s"$l AND $r" } // TODO: Avi - use alias map if DDB requires it
      case expression: PartitionKeyExpression2      => expression.render
    }

}

object KeyConditionExpression2 {
  private[dynamodb] final case class And(left: PartitionKeyExpression2, right: SortKeyExpression2)
      extends KeyConditionExpression2
  def partitionKey(key: String): PartitionKey = PartitionKey(key)

  /**
   * Create a KeyConditionExpression from a ConditionExpression
   * Must be in the form of `<Condition1> && <Condition2>` where format of `<Condition1>` is:
   * {{{<ProjectionExpressionForPartitionKey> === <value>}}}
   * and the format of `<Condition2>` is:
   * {{{<ProjectionExpressionForSortKey> <op> <value>}}} where op can be one of `===`, `>`, `>=`, `<`, `<=`, `between`, `beginsWith`
   *
   * Example using type API:
   * {{{
   * val (email, subject, enrollmentDate, payment) = ProjectionExpression.accessors[Student]
   * // ...
   * val keyConditionExprn = filterKey(email === "avi@gmail.com" && subject === "maths")
   * }}}
   */
  private[dynamodb] def fromConditionExpressionUnsafe(c: ConditionExpression[_]): KeyConditionExpression =
    KeyConditionExpression(c).getOrElse(
      throw new IllegalStateException(s"Error: invalid key condition expression $c")
    )

  // // TODO: Avi - revists
  // private[dynamodb] def apply(c: ConditionExpression[_]): Either[String, KeyConditionExpression2] =
  //   c match {
  //     case ConditionExpression.Equals(
  //           ProjectionExpressionOperand(MapElement(Root, partitionKey)),
  //           ConditionExpression.Operand.ValueOperand(av)
  //         ) =>
  //       Right(PartitionKeyExpression2.Equals(PartitionKey(partitionKey), av))
  //     case ConditionExpression.And(
  //           ConditionExpression.Equals(
  //             ProjectionExpressionOperand(MapElement(Root, partitionKey)),
  //             ConditionExpression.Operand.ValueOperand(avL)
  //           ),
  //           rhs
  //         ) =>
  //       rhs match {
  //         case ConditionExpression.Equals(
  //               ProjectionExpressionOperand(MapElement(Root, sortKey)),
  //               ConditionExpression.Operand.ValueOperand(avR)
  //             ) =>
  //           Right(
  //             PartitionKeyExpression2
  //               .Equals(PartitionKey(partitionKey), avL)
  //               .&&(SimSortKeyExpression2.Equals(SortKey(sortKey), avR))
  //           )
  //         case ConditionExpression.NotEqual(
  //               ProjectionExpressionOperand(MapElement(Root, sortKey)),
  //               ConditionExpression.Operand.ValueOperand(avR)
  //             ) =>
  //           Right(
  //             PartitionKeyExpression
  //               .Equals(PartitionKey(partitionKey), avL)
  //               .&&(SortKeyExpression.NotEqual(SortKey(sortKey), avR))
  //           )
  //         case ConditionExpression.GreaterThan(
  //               ProjectionExpressionOperand(MapElement(Root, sortKey)),
  //               ConditionExpression.Operand.ValueOperand(avR)
  //             ) =>
  //           Right(
  //             PartitionKeyExpression
  //               .Equals(PartitionKey(partitionKey), avL)
  //               .&&(SortKeyExpression.GreaterThan(SortKey(sortKey), avR))
  //           )
  //         case ConditionExpression.LessThan(
  //               ProjectionExpressionOperand(MapElement(Root, sortKey)),
  //               ConditionExpression.Operand.ValueOperand(avR)
  //             ) =>
  //           Right(
  //             PartitionKeyExpression
  //               .Equals(PartitionKey(partitionKey), avL)
  //               .&&(SortKeyExpression.LessThan(SortKey(sortKey), avR))
  //           )
  //         case ConditionExpression.GreaterThanOrEqual(
  //               ProjectionExpressionOperand(MapElement(Root, sortKey)),
  //               ConditionExpression.Operand.ValueOperand(avR)
  //             ) =>
  //           Right(
  //             PartitionKeyExpression
  //               .Equals(PartitionKey(partitionKey), avL)
  //               .&&(SortKeyExpression.GreaterThanOrEqual(SortKey(sortKey), avR))
  //           )
  //         case ConditionExpression.LessThanOrEqual(
  //               ProjectionExpressionOperand(MapElement(Root, sortKey)),
  //               ConditionExpression.Operand.ValueOperand(avR)
  //             ) =>
  //           Right(
  //             PartitionKeyExpression
  //               .Equals(PartitionKey(partitionKey), avL)
  //               .&&(SortKeyExpression.LessThanOrEqual(SortKey(sortKey), avR))
  //           )
  //         case ConditionExpression.Between(
  //               ProjectionExpressionOperand(MapElement(Root, sortKey)),
  //               avMin,
  //               avMax
  //             ) =>
  //           Right(
  //             PartitionKeyExpression
  //               .Equals(PartitionKey(partitionKey), avL)
  //               .&&(SortKeyExpression.Between(SortKey(sortKey), avMin, avMax))
  //           )
  //         case ConditionExpression.BeginsWith(
  //               MapElement(Root, sortKey),
  //               av
  //             ) =>
  //           Right(
  //             PartitionKeyExpression
  //               .Equals(PartitionKey(partitionKey), avL)
  //               .&&(SortKeyExpression.BeginsWith(SortKey(sortKey), av))
  //           )
  //         case c => Left(s"condition '$c' is not a valid sort condition expression")
  //       }

  //     case c => Left(s"condition $c is not a valid key condition expression")
  //   }

}

sealed trait PartitionKeyExpression2 extends KeyConditionExpression2 { self =>
  import KeyConditionExpression2.And

  def &&(that: SortKeyExpression2): KeyConditionExpression2 = And(self, that)

  override def render: AliasMapRender[String] =
    self match {
      case PartitionKeyExpression2.Equals(left, right) =>
        AliasMapRender.getOrInsert(right).map(v => s"${left.keyName} = $v")
    }
}
object PartitionKeyExpression2 {
  final case class PartitionKey(keyName: String) { self =>
    def ===[A](that: A)(implicit t: ToAttributeValue[A]): PartitionKeyExpression2 =
      Equals(self, t.toAttributeValue(that))
  }
  final case class Equals(left: PartitionKey, right: AttributeValue) extends PartitionKeyExpression2
}

sealed trait SimpleSortKeyExpression2 { self =>
  def render: AliasMapRender[String] =
    self match {
      case SimpleSortKeyExpression2.Equals(left, right)       =>
        AliasMapRender
          .getOrInsert(right)
          .map { v =>
            s"${left.keyName} = $v"
          }
      case SortKeyExpression2.LessThan(left, right)           =>
        AliasMapRender
          .getOrInsert(right)
          .map { v =>
            s"${left.keyName} < $v"
          }
      case SortKeyExpression2.NotEqual(left, right)           =>
        AliasMapRender
          .getOrInsert(right)
          .map { v =>
            s"${left.keyName} <> $v"
          }
      case SortKeyExpression2.GreaterThan(left, right)        =>
        AliasMapRender
          .getOrInsert(right)
          .map { v =>
            s"${left.keyName} > $v"
          }
      case SortKeyExpression2.LessThanOrEqual(left, right)    =>
        AliasMapRender
          .getOrInsert(right)
          .map { v =>
            s"${left.keyName} <= $v"
          }
      case SortKeyExpression2.GreaterThanOrEqual(left, right) =>
        AliasMapRender
          .getOrInsert(right)
          .map { v =>
            s"${left.keyName} >= $v"
          }
      case SortKeyExpression2.Between(left, min, max)         =>
        AliasMapRender
          .getOrInsert(min)
          .flatMap(min =>
            AliasMapRender.getOrInsert(max).map { max =>
              s"${left.keyName} BETWEEN $min AND $max"
            }
          )
      case SortKeyExpression2.BeginsWith(left, value)         =>
        AliasMapRender
          .getOrInsert(value)
          .map { v =>
            s"begins_with(${left.keyName}, $v)"
          }
    }
}
object SimpleSortKeyExpression2       {
  private[dynamodb] final case class Equals(left: SortKey, right: AttributeValue) extends SimpleSortKeyExpression2
}

sealed trait SortKeyExpression2 extends SimpleSortKeyExpression2 { self =>
  // def render: AliasMapRender[String] =
  //   self match {
  //     case SimpleSortKeyExpression2.Equals(left, right)             =>
  //       AliasMapRender
  //         .getOrInsert(right)
  //         .map { v =>
  //           s"${left.keyName} = $v"
  //         }
  //     case SortKeyExpression2.LessThan(left, right)           =>
  //       AliasMapRender
  //         .getOrInsert(right)
  //         .map { v =>
  //           s"${left.keyName} < $v"
  //         }
  //     case SortKeyExpression2.NotEqual(left, right)           =>
  //       AliasMapRender
  //         .getOrInsert(right)
  //         .map { v =>
  //           s"${left.keyName} <> $v"
  //         }
  //     case SortKeyExpression2.GreaterThan(left, right)        =>
  //       AliasMapRender
  //         .getOrInsert(right)
  //         .map { v =>
  //           s"${left.keyName} > $v"
  //         }
  //     case SortKeyExpression2.LessThanOrEqual(left, right)    =>
  //       AliasMapRender
  //         .getOrInsert(right)
  //         .map { v =>
  //           s"${left.keyName} <= $v"
  //         }
  //     case SortKeyExpression2.GreaterThanOrEqual(left, right) =>
  //       AliasMapRender
  //         .getOrInsert(right)
  //         .map { v =>
  //           s"${left.keyName} >= $v"
  //         }
  //     case SortKeyExpression2.Between(left, min, max)         =>
  //       AliasMapRender
  //         .getOrInsert(min)
  //         .flatMap(min =>
  //           AliasMapRender.getOrInsert(max).map { max =>
  //             s"${left.keyName} BETWEEN $min AND $max"
  //           }
  //         )
  //     case SortKeyExpression2.BeginsWith(left, value)         =>
  //       AliasMapRender
  //         .getOrInsert(value)
  //         .map { v =>
  //           s"begins_with(${left.keyName}, $v)"
  //         }
  //   }

}

object SortKeyExpression2 {

  final case class SortKey(keyName: String) { self =>
    def ===[A](that: A)(implicit t: ToAttributeValue[A]): SimpleSortKeyExpression2      =
      SimpleSortKeyExpression2.Equals(self, t.toAttributeValue(that))
    def <>[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression2             = NotEqual(self, t.toAttributeValue(that))
    def <[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression2              = LessThan(self, t.toAttributeValue(that))
    def <=[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression2             =
      LessThanOrEqual(self, t.toAttributeValue(that))
    def >[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression2              =
      GreaterThanOrEqual(self, t.toAttributeValue(that))
    def >=[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression2             =
      GreaterThanOrEqual(self, t.toAttributeValue(that))
    def between[A](min: A, max: A)(implicit t: ToAttributeValue[A]): SortKeyExpression2 =
      Between(self, t.toAttributeValue(min), t.toAttributeValue(max))
    def beginsWith[A](value: A)(implicit t: ToAttributeValue[A]): SortKeyExpression2    =
      BeginsWith(self, t.toAttributeValue(value))
  }

//  private[dynamodb] final case class Equals(left: SortKey, right: AttributeValue)             extends SimpleSortKeyExpression2
  private[dynamodb] final case class NotEqual(left: SortKey, right: AttributeValue)           extends SortKeyExpression2
  private[dynamodb] final case class LessThan(left: SortKey, right: AttributeValue)           extends SortKeyExpression2
  private[dynamodb] final case class GreaterThan(left: SortKey, right: AttributeValue)        extends SortKeyExpression2
  private[dynamodb] final case class LessThanOrEqual(left: SortKey, right: AttributeValue)    extends SortKeyExpression2
  private[dynamodb] final case class GreaterThanOrEqual(left: SortKey, right: AttributeValue) extends SortKeyExpression2
  private[dynamodb] final case class Between(left: SortKey, min: AttributeValue, max: AttributeValue)
      extends SortKeyExpression2
  private[dynamodb] final case class BeginsWith(left: SortKey, value: AttributeValue)         extends SortKeyExpression2
}
