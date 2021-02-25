package zio.dynamodb

// models a non empty set
final case class AttributeDefinitions(
  head: AttributeDefinition,
  tail: Set[AttributeDefinition] = Set.empty
) { self =>
  def +(that: AttributeDefinition): AttributeDefinitions = AttributeDefinitions(that, self.tail + self.head)
}
