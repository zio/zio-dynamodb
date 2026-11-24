package zio.dynamodb.proofs

import scala.annotation.implicitNotFound

/**
 * Evidence that `A` can be the type of a partition key or sort key. Instances exist for `String`, `Short`, `Int`,
 * `Long`, `Float`, `Double`, `BigDecimal` and `Iterable[Byte]`, and for `Option` of each of those types.
 * `ProjectionExpression#partitionKey` and `sortKey` ask for it.
 */
@implicitNotFound(
  "DynamoDB does not support primary key type ${A} - allowed types are: String, Number, Binary or an Option of those types"
)
sealed trait IsPrimaryKey[-A]

/** Instances of [[IsPrimaryKey]]. */
object IsPrimaryKey {

  /** Proves that `String` can be a key type. */
  implicit val stringIsPrimaryKey: IsPrimaryKey[String] = new IsPrimaryKey[String] {}

  /** Proves that `Short` can be a key type. */
  implicit val shortIsPrimaryKey: IsPrimaryKey[Short] = new IsPrimaryKey[Short] {}

  /** Proves that `Int` can be a key type. */
  implicit val intIsPrimaryKey: IsPrimaryKey[Int] = new IsPrimaryKey[Int] {}

  /** Proves that `Long` can be a key type. */
  implicit val longIsPrimaryKey: IsPrimaryKey[Long] = new IsPrimaryKey[Long] {}

  /** Proves that `Float` can be a key type. */
  implicit val floatIsPrimaryKey: IsPrimaryKey[Float] = new IsPrimaryKey[Float] {}

  /** Proves that `Double` can be a key type. */
  implicit val doubleIsPrimaryKey: IsPrimaryKey[Double] = new IsPrimaryKey[Double] {}

  /** Proves that `BigDecimal` can be a key type. */
  implicit val bigDecimalIsPrimaryKey: IsPrimaryKey[BigDecimal] = new IsPrimaryKey[BigDecimal] {}

  /** Proves that `Iterable[Byte]` can be a key type. */
  implicit val binaryIsPrimaryKey: IsPrimaryKey[Iterable[Byte]] = new IsPrimaryKey[Iterable[Byte]] {}

  /** Proves that `Option[String]` can be a key type. */
  implicit val stringIsPrimaryKeyOpt: IsPrimaryKey[Option[String]] = new IsPrimaryKey[Option[String]] {}

  /** Proves that `Option[Short]` can be a key type. */
  implicit val shortIsPrimaryKeyOpt: IsPrimaryKey[Option[Short]] = new IsPrimaryKey[Option[Short]] {}

  /** Proves that `Option[Int]` can be a key type. */
  implicit val intIsPrimaryKeyOpt: IsPrimaryKey[Option[Int]] = new IsPrimaryKey[Option[Int]] {}

  /** Proves that `Option[Long]` can be a key type. */
  implicit val longIsPrimaryKeyOpt: IsPrimaryKey[Option[Long]] = new IsPrimaryKey[Option[Long]] {}

  /** Proves that `Option[Float]` can be a key type. */
  implicit val floatIsPrimaryKeyOpt: IsPrimaryKey[Option[Float]] = new IsPrimaryKey[Option[Float]] {}

  /** Proves that `Option[Double]` can be a key type. */
  implicit val doubleIsPrimaryKeyOpt: IsPrimaryKey[Option[Double]] = new IsPrimaryKey[Option[Double]] {}

  /** Proves that `Option[BigDecimal]` can be a key type. */
  implicit val bigDecimalIsPrimaryKeyOpt: IsPrimaryKey[Option[BigDecimal]] =
    new IsPrimaryKey[Option[BigDecimal]] {}

  /** Proves that `Option[Iterable[Byte]]` can be a key type. */
  implicit val binaryIsPrimaryKeyOpt: IsPrimaryKey[Option[Iterable[Byte]]] =
    new IsPrimaryKey[Option[Iterable[Byte]]] {}

}
