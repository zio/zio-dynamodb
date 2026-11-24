package zio.dynamodb

import zio.dynamodb.proofs.CanSortKeyBeginsWith

import zio.dynamodb.KeyConditionExpr.SortKeyEquals
import zio.dynamodb.KeyConditionExpr.ExtendedSortKeyExpr
import zio.dynamodb.ProjectionExpression.Unknown

private[dynamodb] final case class SortKey[-From, +To](keyName: String)

/**
 * Operators that build key condition expressions from the sort key of a table or index, which a `sortKey` call on a
 * [[ProjectionExpression]] returns. Each operator returns a condition that is combined with a partition key condition
 * using `&&`.
 */
object SortKey {
  // all comparison ops apply to: Strings, Numbers, Binary values

  /**
   * Operators for a sort key whose value type is not known, such as `\$("ts").sortKey`. The type of the value is taken
   * from the argument.
   */
  implicit class SortKeyUnknownToOps[-From](val sk: SortKey[From, Unknown]) {

    /** Builds the key condition `key = value`. */
    def ===[To: ToAttributeValue](
      value: To
    ): SortKeyEquals[From] =
      SortKeyEquals(sk, ToAttributeValue[To].toAttributeValue(value))

    /** Builds the key condition `key > value`. */
    def >[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.GreaterThan(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )

    /** Builds the key condition `key < value`. */
    def <[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.LessThan(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )

    /**
     * Builds the key condition `key <> value`. The library builds this condition, but the DynamoDB documentation does
     * not list `<>` among the sort key operators of a key condition.
     */
    def <>[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.NotEqual(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )

    /** Builds the key condition `key <= value`. */
    def <=[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.LessThanOrEqual(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )

    /** Builds the key condition `key >= value`. */
    def >=[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.GreaterThanOrEqual(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )

    /** Builds the key condition `key BETWEEN min AND max`. */
    def between[To: ToAttributeValue](min: To, max: To): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.Between[From, To](
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(min),
        ToAttributeValue[To].toAttributeValue(max)
      )

    /**
     * Builds the key condition `begins_with(key, prefix)`. The type of the prefix is not checked for a sort key of
     * unknown type.
     */
    def beginsWith[To: ToAttributeValue](
      prefix: To
    )(implicit ev: CanSortKeyBeginsWith[_, To]): ExtendedSortKeyExpr[From, To] = {
      val _ = ev
      ExtendedSortKeyExpr.BeginsWith[From, To](
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(prefix)
      )
    }
  }

  /**
   * Operators for a sort key whose value type `To` is known, such as one taken from the accessors of a case class. The
   * value must be of type `To`.
   */
  implicit class SortKeyOps[-From, To: ToAttributeValue](val sk: SortKey[From, To]) {

    /** Builds the key condition `key = value`. */
    def ===(
      value: To
    ): SortKeyEquals[From] =
      SortKeyEquals(sk, ToAttributeValue[To].toAttributeValue(value))

    /** Builds the key condition `key > value`. */
    def >(
      value: To
    ): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.GreaterThan(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )

    /** Builds the key condition `key < value`. */
    def <(
      value: To
    ): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.LessThan(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )

    /**
     * Builds the key condition `key <> value`. The library builds this condition, but the DynamoDB documentation does
     * not list `<>` among the sort key operators of a key condition.
     */
    def <>(
      value: To
    ): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.NotEqual(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )

    /** Builds the key condition `key <= value`. */
    def <=(
      value: To
    ): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.LessThanOrEqual(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )

    /** Builds the key condition `key >= value`. */
    def >=(
      value: To
    ): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.GreaterThanOrEqual(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )

    /** Builds the key condition `key BETWEEN min AND max`. */
    def between(min: To, max: To): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.Between[From, To](
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(min),
        ToAttributeValue[To].toAttributeValue(max)
      )

    /**
     * Builds the key condition `begins_with(key, prefix)`. The code does not compile unless the key type is a `String`,
     * an `Option[String]`, a byte collection such as `Chunk[Byte]`, or an `Option` of a byte collection.
     */
    def beginsWith(
      prefix: To
    )(implicit ev: CanSortKeyBeginsWith[To, To]): ExtendedSortKeyExpr[From, To] = {
      val _ = ev
      ExtendedSortKeyExpr.BeginsWith[From, To](
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(prefix)
      )
    }

  }

}
