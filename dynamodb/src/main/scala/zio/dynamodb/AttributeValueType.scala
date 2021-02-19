package zio.dynamodb

// TODO: is there a better way to do this?
sealed trait AttributeValueType

object AttributeValueType {
  final case object Binary    extends AttributeValueType
  final case object Bool      extends AttributeValueType
  final case object BinarySet extends AttributeValueType
  final case object List      extends AttributeValueType
  final case object Map       extends AttributeValueType
  final case object Number    extends AttributeValueType
  final case object NumberSet extends AttributeValueType
  final case object Null      extends AttributeValueType
  final case object String    extends AttributeValueType
  final case object StringSet extends AttributeValueType
}
