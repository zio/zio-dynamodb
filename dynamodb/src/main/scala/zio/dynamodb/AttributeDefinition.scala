package zio.dynamodb

import scala.annotation.nowarn

@nowarn
final case class AttributeDefinition private (
  name: String,
  attributeType: PrimitiveValueType
)
object AttributeDefinition {
  def attrDefnBinary(name: String): AttributeDefinition = AttributeDefinition(name, AttributeValueType.Binary)
  def attrDefnNumber(name: String): AttributeDefinition = AttributeDefinition(name, AttributeValueType.Number)
  def attrDefnString(name: String): AttributeDefinition = AttributeDefinition(name, AttributeValueType.String)
}
