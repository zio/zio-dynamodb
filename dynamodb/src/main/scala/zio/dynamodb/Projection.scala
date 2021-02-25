package zio.dynamodb

sealed trait ProjectionType
object Projection {

  case object KeysOnly                                                  extends ProjectionType
  // count must not exceed 20
  final case class Include(head: String, nonKeyAttributes: Set[String]) extends ProjectionType { self =>
    def +(attributeName: String): Include = Include(attributeName, self.nonKeyAttributes + self.head)
  }
  case object All                                                       extends ProjectionType
}
