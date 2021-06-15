package zio.dynamodb

private[dynamodb] final case class AttributeDefinition(
  name: String,
  attributeType: PrimitiveValueType
)
object AttributeDefinition {
  def attrDefnBinary(name: String): AttributeDefinition = AttributeDefinition(name, AttributeValueType.Binary)
  def attrDefnNumber(name: String): AttributeDefinition = AttributeDefinition(name, AttributeValueType.Number)
  def attrDefnString(name: String): AttributeDefinition = AttributeDefinition(name, AttributeValueType.String)
}
