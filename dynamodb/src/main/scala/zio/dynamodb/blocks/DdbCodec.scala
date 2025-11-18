package zio.dynamodb.blocks

import zio.dynamodb.{ Decoder, Encoder }

trait DdbCodec[A] {

  def encoder: Encoder[A]
  def decoder: Decoder[A]
}
