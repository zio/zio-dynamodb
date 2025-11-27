package zio.dynamodb.blocks

import zio.dynamodb.{ Decoder, Encoder }

trait DynamoDbCodec[A] {

  def encoder: Encoder[A]
  def decoder: Decoder[A]
}
