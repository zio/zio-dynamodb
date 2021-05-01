package zio.dynamodb

// The maximum depth for a document path is 32
sealed trait ProjectionExpression { self =>
  def apply(index: Int): ProjectionExpression = ProjectionExpression.ListElement(self, index)

  def apply(key: String): ProjectionExpression = ProjectionExpression.MapElement(self, key)

  def exists: ConditionExpression                                    = ConditionExpression.AttributeExists(self)
  def notExists: ConditionExpression                                 = ConditionExpression.AttributeNotExists(self)
  def contains(av: AttributeValue): ConditionExpression              = ConditionExpression.Contains(self, av)
  def beginsWith(av: AttributeValue): ConditionExpression            = ConditionExpression.BeginsWith(self, av)
  def isType(attributeType: AttributeValueType): ConditionExpression =
    ConditionExpression.AttributeType(self, attributeType)

  def size: ConditionExpression.Operand.Size = ConditionExpression.Operand.Size(self)
}

object ProjectionExpression {
  // Note that you can only use a ProjectionExpression if the first character is a-z or A-Z and the second character
  // (if present) is a-z, A-Z, or 0-9. Also key words are not allowed
  // If this is not the case then you must use the Expression Attribute Names facility to create an alias.
  // Attribute names containing a dot "." must also use the Expression Attribute Names
  def apply(name: String): ProjectionExpression = TopLevel(name)

  final case class TopLevel(name: String)                                extends ProjectionExpression
  final case class MapElement(parent: ProjectionExpression, key: String) extends ProjectionExpression
  // index must be non negative - we could use a new type here?
  final case class ListElement(parent: ProjectionExpression, index: Int) extends ProjectionExpression
}
