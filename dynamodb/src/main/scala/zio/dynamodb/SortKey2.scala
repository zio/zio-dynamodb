package zio.dynamodb

import zio.dynamodb.proofs.RefersTo
import zio.dynamodb.proofs.CanSortKeyBeginsWith

import zio.dynamodb.KeyConditionExpr.SortKeyExpr
import zio.dynamodb.KeyConditionExpr.ExtendedSortKeyExpr

private[dynamodb] final case class SortKey2[-From, +To](keyName: String) { self =>
  // all comparison ops apply to: Strings, Numbers, Binary values
  def ===[To1 >: To, To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To1, To2]): SortKeyExpr[From, To2] = {
    val _ = ev
    SortKeyExpr[From, To2](
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly[ToAttributeValue[To2]].toAttributeValue(value)
    )
  }
  def >[To1 >: To, To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To1, To2]): ExtendedSortKeyExpr[From, To2] = {
    val _ = ev
    ExtendedSortKeyExpr.GreaterThan(
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly(ToAttributeValue[To2]).toAttributeValue(value)
    )
  }
  def <[To1 >: To, To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To1, To2]): ExtendedSortKeyExpr[From, To2] = {
    val _ = ev
    ExtendedSortKeyExpr.LessThan(
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly[ToAttributeValue[To2]].toAttributeValue(value)
    )
  }
  def <>[To1 >: To, To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To1, To2]): ExtendedSortKeyExpr[From, To2] = {
    val _ = ev
    ExtendedSortKeyExpr.NotEqual(
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly(ToAttributeValue[To2]).toAttributeValue(value)
    )
  }
  def <=[To1 >: To, To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To1, To2]): ExtendedSortKeyExpr[From, To2] = {
    val _ = ev
    ExtendedSortKeyExpr.LessThanOrEqual(
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly[ToAttributeValue[To2]].toAttributeValue(value)
    )
  }
  def >=[To1 >: To, To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To1, To2]): ExtendedSortKeyExpr[From, To2] = {
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

  // beginsWith applies to: Strings, Binary values
  def beginsWith[To1 >: To, To2: ToAttributeValue, IsPrimaryKey](
    prefix: To2
  )(implicit ev: CanSortKeyBeginsWith[To1, To2]): ExtendedSortKeyExpr[From, To2] = {
    val _ = ev
    ExtendedSortKeyExpr.BeginsWith[From, To2](
      self.asInstanceOf[SortKey2[From, To2]],
      implicitly[ToAttributeValue[To2]].toAttributeValue(prefix)
    )
  }

}
