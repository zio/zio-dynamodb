package zio.dynamodb

import zio.Chunk

sealed trait AttributeValue

object AttributeValue {
  import Predef.{ Map => ScalaMap, String => ScalaString }
  final case class Binary(value: Chunk[Byte])                   extends AttributeValue
  final case class Bool(value: Boolean)                         extends AttributeValue
  final case class BinarySet(value: Chunk[Chunk[Byte]])         extends AttributeValue
  final case class List(value: Chunk[AttributeValue])           extends AttributeValue
  final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue
  final case class Number(value: BigDecimal)                    extends AttributeValue
  final case class NumberSet(value: Set[BigDecimal])            extends AttributeValue
  object Null                                                   extends AttributeValue
  final case class String(value: ScalaString)                   extends AttributeValue
  final case class StringSet(value: Set[ScalaString])           extends AttributeValue
}

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
