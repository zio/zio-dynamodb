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
