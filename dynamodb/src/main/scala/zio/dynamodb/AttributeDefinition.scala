package zio.dynamodb

/**
 * The name and the type of an attribute that is a key of a table or of an index. The type is `Binary`, `Number` or
 * `String`.
 */
final case class AttributeDefinition private (
  name: String,
  attributeType: PrimitiveValueType
)

/** Creates an [[AttributeDefinition]] for a key attribute. */
object AttributeDefinition {

  /** Returns a definition for the attribute `name` of type `AttributeValueType.Binary`. */
  def attrDefnBinary(name: String): AttributeDefinition = AttributeDefinition(name, AttributeValueType.Binary)

  /** Returns a definition for the attribute `name` of type `AttributeValueType.Number`. */
  def attrDefnNumber(name: String): AttributeDefinition = AttributeDefinition(name, AttributeValueType.Number)

  /** Returns a definition for the attribute `name` of type `AttributeValueType.String`. */
  def attrDefnString(name: String): AttributeDefinition = AttributeDefinition(name, AttributeValueType.String)
}
