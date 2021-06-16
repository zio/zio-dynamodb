package zio.dynamodb

sealed trait ProjectionType
object ProjectionType {

  case object KeysOnly                                                                       extends ProjectionType
  // count must not exceed 20
  private[dynamodb] final case class Include private (nonKeyAttributes: NonEmptySet[String]) extends ProjectionType
  object Include {
    def apply(nonKeyAttribute: String, nonKeyAttributes: String*): Include =
      new Include(NonEmptySet(nonKeyAttribute, nonKeyAttributes: _*))
  }
  case object All extends ProjectionType
}
