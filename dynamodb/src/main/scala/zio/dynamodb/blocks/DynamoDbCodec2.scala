package zio.dynamodb.blocks

import zio.blocks.schema.binding.RegisterOffset
import zio.dynamodb.blocks.DummyCodec2.{ Decoder2, Encoder2 }

abstract class DynamoDbCodec2[A](val valueType: Int = DynamoDbCodec2.objectType) {

  val valueOffset: RegisterOffset.RegisterOffset = valueType match {
    case DynamoDbCodec.objectType => RegisterOffset(objects = 1)
    case DynamoDbCodec.intType    => RegisterOffset(ints = 1)
    case DynamoDbCodec.longType   => RegisterOffset(longs = 1)
    case _                        => RegisterOffset.Zero
  }

  def encoder: Encoder2[A]
  def decoder: Decoder2[A]

}
object DynamoDbCodec2 {
  val objectType = 0
  val intType    = 1
  val longType   = 3
}
