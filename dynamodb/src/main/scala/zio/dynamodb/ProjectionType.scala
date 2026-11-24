package zio.dynamodb

/** The attributes that a secondary index copies from its table: `KeysOnly`, `Include` or `All`. */
sealed trait ProjectionType
object ProjectionType {

  /** Projects only the key attributes of the table and the index. */
  case object KeysOnly extends ProjectionType

  /**
   * Projects the key attributes and the listed non-key attributes. The number of attributes is not checked here;
   * DynamoDB applies its own limit.
   */
  final case class Include private (nonKeyAttributes: NonEmptySet[String]) extends ProjectionType

  /** Creates an `Include` projection. */
  object Include {

    /** Returns an `Include` projection for the given non-key attribute names, which are kept as a set. */
    def apply(nonKeyAttribute: String, nonKeyAttributes: String*): Include =
      new Include(NonEmptySet(nonKeyAttribute, nonKeyAttributes: _*))
  }

  /** Projects all the attributes of the table. */
  case object All extends ProjectionType
}
