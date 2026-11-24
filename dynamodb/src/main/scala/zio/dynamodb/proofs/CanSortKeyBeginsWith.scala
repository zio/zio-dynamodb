package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression
import scala.annotation.implicitNotFound

/**
 * Evidence that `beginsWith` can be applied to a sort key of type `X` with a prefix of type `A`. Instances exist when
 * both types are `String`, `Option[String]`, a subtype of `Iterable[Byte]` or a subtype of `Option[Iterable[Byte]]`,
 * and when either type is `ProjectionExpression.Unknown`.
 */
@implicitNotFound(
  "Field of type ${X} has 'beginsWith' argument of type ${A} - they must be the same type and be a string or a byte array"
)
sealed trait CanSortKeyBeginsWith[-X, -A]

/** Lower-priority instances of [[CanSortKeyBeginsWith]]. */
trait CanSortKeyBeginsWith0 extends CanSortKeyBeginsWith1 {

  /** Proves `beginsWith` for any sort key type `X` when the prefix type is `ProjectionExpression.Unknown`. */
  implicit def unknownRight[X]: CanSortKeyBeginsWith[X, ProjectionExpression.Unknown] =
    new CanSortKeyBeginsWith[X, ProjectionExpression.Unknown] {}
}

/** Lowest-priority instances of [[CanSortKeyBeginsWith]], for strings and bytes. */
trait CanSortKeyBeginsWith1 {
  // begins_with with only applies to keys of type string or bytes
  /** Proves `beginsWith` for a sort key and prefix of the same `Iterable[Byte]` subtype. */
  implicit def bytes[A <: Iterable[Byte]]: CanSortKeyBeginsWith[A, A]               =
    new CanSortKeyBeginsWith[A, A] {}

  /** Proves `beginsWith` for a sort key and prefix of the same optional byte-collection type. */
  implicit def optionBytes[A <: Option[Iterable[Byte]]]: CanSortKeyBeginsWith[A, A] =
    new CanSortKeyBeginsWith[A, A] {}

  /** Proves `beginsWith` for a `String` sort key with a `String` prefix. */
  implicit def string: CanSortKeyBeginsWith[String, String]                         = new CanSortKeyBeginsWith[String, String] {}

  /** Proves `beginsWith` for an `Option[String]` sort key with an `Option[String]` prefix. */
  implicit def optionString: CanSortKeyBeginsWith[Option[String], Option[String]] =
    new CanSortKeyBeginsWith[Option[String], Option[String]] {}
}

/** Instances of [[CanSortKeyBeginsWith]]. */
object CanSortKeyBeginsWith extends CanSortKeyBeginsWith0 {

  /** Proves `beginsWith` for any prefix type `X` when the sort key type is `ProjectionExpression.Unknown`. */
  implicit def unknownLeft[X]: CanSortKeyBeginsWith[ProjectionExpression.Unknown, X] =
    new CanSortKeyBeginsWith[ProjectionExpression.Unknown, X] {}
}
