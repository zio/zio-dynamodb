package zio.dynamodb

// The maximum depth for a document path is 32
sealed trait ProjectionExpression { self =>
  def apply(index: Int): ProjectionExpression = ProjectionExpression.ListElement(self, index)

  def apply(key: String): ProjectionExpression = ProjectionExpression.MapElement(self, key)
}

object ProjectionExpression {
  def apply(name: String): ProjectionExpression = TopLevel(name)

  final case class TopLevel(name: String)                                extends ProjectionExpression
  final case class MapElement(parent: ProjectionExpression, key: String) extends ProjectionExpression
  final case class ListElement(parent: ProjectionExpression, index: Int) extends ProjectionExpression
}
