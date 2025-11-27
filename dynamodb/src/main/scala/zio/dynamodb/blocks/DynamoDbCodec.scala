package zio.dynamodb.blocks

import zio.blocks.schema.binding.RegisterOffset
import zio.dynamodb.{ Decoder, Encoder }

abstract class DynamoDbCodec[A](val valueType: Int = DynamoDbCodec.objectType) {

  val valueOffset: RegisterOffset.RegisterOffset = valueType match {
    case DynamoDbCodec.objectType => RegisterOffset(objects = 1)
    case _                        => RegisterOffset.Zero
  }

  def encoder: Encoder[A]
  def decoder: Decoder[A]

}
object DynamoDbCodec {
  val objectType = 0
}
