package zio.dynamodb

sealed trait ProjectionType
object Projection {

  case object KeysOnly                                                     extends ProjectionType
  // count must not exceed 20
  final case class Include private (nonKeyAttributes: NonEmptySet[String]) extends ProjectionType { self =>
    def +(attributeName: String): Include = Include(self.nonKeyAttributes + attributeName)
  }
  object Include {
    def apply(nonKeyAttribute: String): Include = Include(NonEmptySet(nonKeyAttribute))
  }
  case object All                                                          extends ProjectionType
}
