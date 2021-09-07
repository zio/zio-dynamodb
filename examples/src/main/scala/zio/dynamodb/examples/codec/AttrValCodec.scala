package zio.dynamodb.examples.codec

import zio.dynamodb.AttributeValue

trait AttrValCodec {
  def encode[A]: A => AttributeValue
  def decode[A]: AttributeValue => A
}
