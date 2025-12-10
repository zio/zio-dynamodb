package zio.dynamodb.blocks

import zio.blocks.schema.binding.RegisterOffset
import zio.dynamodb.{ Decoder, Encoder }

abstract class DynamoDBCodec[A](val valueType: Int = DynamoDBCodec.objectType) {

  val valueOffset: RegisterOffset.RegisterOffset = valueType match {
    case DynamoDBCodec.objectType => RegisterOffset(objects = 1)
    case DynamoDBCodec.intType    => RegisterOffset(ints = 1)
    case DynamoDBCodec.longType   => RegisterOffset(longs = 1)
    case _                        => RegisterOffset.Zero
  }

  def encoder: Encoder[A]
  def decoder: Decoder[A]

}
object DynamoDBCodec {
  val objectType = 0
  val intType    = 1
  val longType   = 3
}
