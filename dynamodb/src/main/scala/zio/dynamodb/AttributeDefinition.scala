package zio.dynamodb

final case class AttributeDefinition(
  name: String,
  attributeType: PrimitiveValueType
)
