package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

import scala.annotation.implicitNotFound

/**
 * Evidence that an element can be removed by index from an attribute of type `X`. An instance exists for `Seq[A]` (and,
 * because the type parameter is contravariant, for subtypes such as `List[A]` and `Vector[A]`) and for
 * `ProjectionExpression.Unknown`. `ProjectionExpression#remove(index)` asks for it.
 */
@implicitNotFound(
  "DynamoDB does not support remove operations on the type ${X}. To use this operation work with any collection type that extends 'Seq'"
)
sealed trait ListRemoveable[-X]

/** Lower-priority instances of [[ListRemoveable]]. */
trait ListRemoveable0 extends ListRemoveableLowPriorityImplicits {

  /** Proves the removal when the attribute type is `ProjectionExpression.Unknown`. */
  implicit def unknownRight[X]: ListRemoveable[ProjectionExpression.Unknown] =
    new ListRemoveable[ProjectionExpression.Unknown] {}
}

/** Lowest-priority instances of [[ListRemoveable]], for sequences. */
trait ListRemoveableLowPriorityImplicits {

  /** Proves the removal for a `Seq[A]` attribute. */
  implicit def list[A]: ListRemoveable[Seq[A]] = new ListRemoveable[Seq[A]] {}
}

/** Instances of [[ListRemoveable]]. */
object ListRemoveable extends ListRemoveable0
