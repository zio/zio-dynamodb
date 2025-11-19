package zio.dynamodb.blocks

import zio.blocks.schema.binding.RegisterOffset
import zio.dynamodb.{Decoder, Encoder}

abstract class DdbCodec2[A](val valueType: Int) {

  def encoder: Encoder[A]
  def decoder: Decoder[A]

  val valueOffset: RegisterOffset.RegisterOffset = valueType match {
    case DdbCodec2.objectType  => RegisterOffset(objects = 1)
    case DdbCodec2.booleanType => RegisterOffset(booleans = 1)
    case DdbCodec2.byteType    => RegisterOffset(bytes = 1)
    case DdbCodec2.charType    => RegisterOffset(chars = 1)
    case DdbCodec2.shortType   => RegisterOffset(shorts = 1)
    case DdbCodec2.floatType   => RegisterOffset(floats = 1)
    case DdbCodec2.intType     => RegisterOffset(ints = 1)
    case DdbCodec2.doubleType  => RegisterOffset(doubles = 1)
    case DdbCodec2.longType    => RegisterOffset(longs = 1)
    case _                           => RegisterOffset.Zero
  }
}
object DdbCodec2 {
  val objectType  = 0
  val booleanType = 1
  val byteType    = 2
  val charType    = 3
  val shortType   = 4
  val floatType   = 5
  val intType     = 6
  val doubleType  = 7
  val longType    = 8
  val unitType    = 9
}
