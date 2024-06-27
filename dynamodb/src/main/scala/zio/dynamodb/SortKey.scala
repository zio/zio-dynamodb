package zio.dynamodb

import zio.dynamodb.proofs.CanSortKeyBeginsWith

import zio.dynamodb.KeyConditionExpr.SortKeyEquals
import zio.dynamodb.KeyConditionExpr.ExtendedSortKeyExpr
import zio.dynamodb.ProjectionExpression.Unknown

private[dynamodb] final case class SortKey[-From, +To](keyName: String)

private[dynamodb] object SortKey {
  // all comparison ops apply to: Strings, Numbers, Binary values

  implicit class SortKeyUnknownToOps[-From](val sk: SortKey[From, Unknown]) {
    def ===[To: ToAttributeValue](
      value: To
    ): SortKeyEquals[From]                                                             =
      SortKeyEquals(sk, implicitly[ToAttributeValue[To]].toAttributeValue(value))
    def >[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To]                                                   =
      ExtendedSortKeyExpr.GreaterThan(
        sk.asInstanceOf[SortKey[From, To]],
        implicitly(ToAttributeValue[To]).toAttributeValue(value)
      )
    def <[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To]                                                   =
      ExtendedSortKeyExpr.LessThan(
        sk.asInstanceOf[SortKey[From, To]],
        implicitly[ToAttributeValue[To]].toAttributeValue(value)
      )
    def <>[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To]                                                   =
      ExtendedSortKeyExpr.NotEqual(
        sk.asInstanceOf[SortKey[From, To]],
        implicitly(ToAttributeValue[To]).toAttributeValue(value)
      )
    def <=[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To]                                                   =
      ExtendedSortKeyExpr.LessThanOrEqual(
        sk.asInstanceOf[SortKey[From, To]],
        implicitly[ToAttributeValue[To]].toAttributeValue(value)
      )
    def >=[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To]                                                   =
      ExtendedSortKeyExpr.GreaterThanOrEqual(
        sk.asInstanceOf[SortKey[From, To]],
        implicitly[ToAttributeValue[To]].toAttributeValue(value)
      )
    def between[To: ToAttributeValue](min: To, max: To): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.Between[From, To](
        sk.asInstanceOf[SortKey[From, To]],
        implicitly[ToAttributeValue[To]].toAttributeValue(min),
        implicitly[ToAttributeValue[To]].toAttributeValue(max)
      )
    def beginsWith[To: ToAttributeValue](
      prefix: To
    )(implicit ev: CanSortKeyBeginsWith[_, To]): ExtendedSortKeyExpr[From, To] = {
      val _ = ev
      ExtendedSortKeyExpr.BeginsWith[From, To](
        sk.asInstanceOf[SortKey[From, To]],
        implicitly[ToAttributeValue[To]].toAttributeValue(prefix)
      )
    }
  }

  implicit class SortKeyOps[-From, To: ToAttributeValue](val sk: SortKey[From, To]) {
    def ===(
      value: To
    ): SortKeyEquals[From]                                       =
      SortKeyEquals(sk, implicitly[ToAttributeValue[To]].toAttributeValue(value))
    def >(
      value: To
    ): ExtendedSortKeyExpr[From, To]                             =
      ExtendedSortKeyExpr.GreaterThan(
        sk.asInstanceOf[SortKey[From, To]],
        implicitly(ToAttributeValue[To]).toAttributeValue(value)
      )
    def <(
      value: To
    ): ExtendedSortKeyExpr[From, To]                             =
      ExtendedSortKeyExpr.LessThan(
        sk.asInstanceOf[SortKey[From, To]],
        implicitly[ToAttributeValue[To]].toAttributeValue(value)
      )
    def <>(
      value: To
    ): ExtendedSortKeyExpr[From, To]                             =
      ExtendedSortKeyExpr.NotEqual(
        sk.asInstanceOf[SortKey[From, To]],
        implicitly(ToAttributeValue[To]).toAttributeValue(value)
      )
    def <=(
      value: To
    ): ExtendedSortKeyExpr[From, To]                             =
      ExtendedSortKeyExpr.LessThanOrEqual(
        sk.asInstanceOf[SortKey[From, To]],
        implicitly[ToAttributeValue[To]].toAttributeValue(value)
      )
    def >=(
      value: To
    ): ExtendedSortKeyExpr[From, To]                             =
      ExtendedSortKeyExpr.GreaterThanOrEqual(
        sk.asInstanceOf[SortKey[From, To]],
        implicitly[ToAttributeValue[To]].toAttributeValue(value)
      )
    def between(min: To, max: To): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.Between[From, To](
        sk.asInstanceOf[SortKey[From, To]],
        implicitly[ToAttributeValue[To]].toAttributeValue(min),
        implicitly[ToAttributeValue[To]].toAttributeValue(max)
      )
    def beginsWith(
      prefix: To
    )(implicit ev: CanSortKeyBeginsWith[To, To]): ExtendedSortKeyExpr[From, To] = {
      val _ = ev
      ExtendedSortKeyExpr.BeginsWith[From, To](
        sk.asInstanceOf[SortKey[From, To]],
        implicitly[ToAttributeValue[To]].toAttributeValue(prefix)
      )
    }

  }

}
