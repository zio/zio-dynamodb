package zio.dynamodb

// TODO: is there a better way to do this?
sealed trait AttributeValueType
sealed trait PrimitiveValueType extends AttributeValueType

object AttributeValueType {
  // primitive types
  final case object Binary    extends PrimitiveValueType
  final case object Number    extends PrimitiveValueType
  final case object String    extends PrimitiveValueType
  // non primitive types
  final case object Bool      extends AttributeValueType
  final case object BinarySet extends AttributeValueType
  final case object List      extends AttributeValueType
  final case object Map       extends AttributeValueType
  final case object NumberSet extends AttributeValueType
  final case object Null      extends AttributeValueType
  final case object StringSet extends AttributeValueType
}
