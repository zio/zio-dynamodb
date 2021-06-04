package zio.dynamodb

private[dynamodb] final case class AttributeDefinition(
  name: String,
  attributeType: PrimitiveValueType
)
object AttributeDefinition {
  def binaryAttrDefn(name: String): AttributeDefinition = AttributeDefinition(name, AttributeValueType.Binary)
  def numberAttrDefn(name: String): AttributeDefinition = AttributeDefinition(name, AttributeValueType.Number)
  def stringAttrDefn(name: String): AttributeDefinition = AttributeDefinition(name, AttributeValueType.String)
}
