package zio.dynamodb

/**
 * The type of a DynamoDB attribute value. It is the type argument of the `attribute_type` condition function (for
 * example `isNumber` on a [[ProjectionExpression]]) and, for the primitive types, the key type of an
 * [[AttributeDefinition]].
 */
sealed trait AttributeValueType { self =>

  /**
   * Adds the DynamoDB type code of this type to the alias map as a value and returns its placeholder, for example `:v0`
   * for the code `N`. The codes are `B`, `N`, `S`, `BOOL`, `BS`, `L`, `M`, `NS`, `NULL` and `SS`.
   */
  def render: AliasMapRender[String] =
    self match {
      case valueType: PrimitiveValueType => valueType.render
      case AttributeValueType.Bool       => AliasMapRender.getOrInsert(AttributeValue.String("BOOL"))
      case AttributeValueType.BinarySet  => AliasMapRender.getOrInsert(AttributeValue.String("BS"))
      case AttributeValueType.List       => AliasMapRender.getOrInsert(AttributeValue.String("L"))
      case AttributeValueType.Map        => AliasMapRender.getOrInsert(AttributeValue.String("M"))
      case AttributeValueType.NumberSet  => AliasMapRender.getOrInsert(AttributeValue.String("NS"))
      case AttributeValueType.Null       => AliasMapRender.getOrInsert(AttributeValue.String("NULL"))
      case AttributeValueType.StringSet  => AliasMapRender.getOrInsert(AttributeValue.String("SS"))
    }
}

/**
 * The types that can be the type of a key attribute: `Binary`, `Number` and `String`. An [[AttributeDefinition]] takes
 * one of these.
 */
sealed trait PrimitiveValueType extends AttributeValueType { self =>
  override def render: AliasMapRender[String] =
    self match {
      case AttributeValueType.Binary => AliasMapRender.getOrInsert(AttributeValue.String("B"))
      case AttributeValueType.Number => AliasMapRender.getOrInsert(AttributeValue.String("N"))
      case AttributeValueType.String => AliasMapRender.getOrInsert(AttributeValue.String("S"))
    }
}

/** The attribute value types of DynamoDB. */
object AttributeValueType {
  // primitive types
  /** Binary data, with the type code `B`. */
  case object Binary extends PrimitiveValueType

  /** A number, with the type code `N`. */
  case object Number extends PrimitiveValueType

  /** A string, with the type code `S`. */
  case object String extends PrimitiveValueType
  // non primitive types
  /** A boolean, with the type code `BOOL`. */
  case object Bool   extends AttributeValueType

  /** A set of binary values, with the type code `BS`. */
  case object BinarySet extends AttributeValueType

  /** A list of values, with the type code `L`. */
  case object List extends AttributeValueType

  /** A map of attribute names to values, with the type code `M`. */
  case object Map extends AttributeValueType

  /** A set of numbers, with the type code `NS`. */
  case object NumberSet extends AttributeValueType

  /** The null value, with the type code `NULL`. */
  case object Null extends AttributeValueType

  /** A set of strings, with the type code `SS`. */
  case object StringSet extends AttributeValueType
}
