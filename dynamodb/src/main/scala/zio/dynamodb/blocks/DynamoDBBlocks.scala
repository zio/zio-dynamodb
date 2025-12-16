package zio.dynamodb.blocks

import zio.blocks.schema.Reflect.Bound
import zio.blocks.schema._
import zio.blocks.schema.binding.BindingType.{ Primitive, Variant, Wrapper }
import zio.blocks.schema.binding.RegisterOffset.RegisterOffset
import zio.blocks.schema.binding._
import zio.blocks.schema.derive.{ BindingInstance, Deriver }
import zio.dynamodb.AttributeValue.Map.JMapView
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.{ AttributeValue, Decoder, Encoder }

import scala.collection.mutable.ArrayBuffer

object DynamoDBCodecDeriver extends DynamoDBBlocks.DynamoDBCodecDeriver()

/**
 * borrows heavily from Andriy Plokhotnyuk's zio-blocks JSON codec https://github.com/zio/zio-blocks
 */
object DynamoDBBlocks {

  class DynamoDBCodecDeriver() extends Deriver[DynamoDBCodec] {
    override def derivePrimitive[F[_, _], A](
      primitiveType: PrimitiveType[A],
      typeName: TypeName[A],
      binding: Binding[Primitive, A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    ): Lazy[DynamoDBCodec[A]] =
      Lazy(
        deriveCodec(
          Reflect.Primitive(
            primitiveType = primitiveType,
            typeName = typeName,
            primitiveBinding = binding,
            doc = doc,
            modifiers = modifiers
          )
        )
      )

    override def deriveRecord[F[_, _], A](
      fields: IndexedSeq[Term[F, A, _]],
      typeName: TypeName[A],
      binding: Binding[BindingType.Record, A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[A]] =
      Lazy(
        deriveCodec(
          Reflect.Record(
            fields = fields.asInstanceOf[IndexedSeq[Term[Binding, A, _]]],
            typeName = typeName,
            recordBinding = binding,
            doc = doc,
            modifiers = modifiers
          )
        )
      )

    override def deriveVariant[F[_, _], A](
      cases: IndexedSeq[Term[F, A, _]],
      typeName: TypeName[A],
      binding: Binding[Variant, A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[A]] = ???

    override def deriveSequence[F[_, _], C[_], A](
      element: Reflect[F, A],
      typeName: TypeName[C[A]],
      binding: Binding[BindingType.Seq[C], C[A]],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[C[A]]] = ???

    override def deriveMap[F[_, _], M[_, _], K, V](
      key: Reflect[F, K],
      value: Reflect[F, V],
      typeName: TypeName[M[K, V]],
      binding: Binding[BindingType.Map[M], M[K, V]],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[M[K, V]]] = ???

    override def deriveDynamic[F[_, _]](
      binding: Binding[BindingType.Dynamic, DynamicValue],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[DynamicValue]] = ???

    override def deriveWrapper[F[_, _], A, B](
      wrapped: Reflect[F, B],
      typeName: TypeName[A],
      wrapperPrimitiveType: Option[PrimitiveType[A]],
      binding: Binding[Wrapper[A, B], A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[A]] = ???
  }

  private[this] val stringCodec: DynamoDBCodec[String] =
    new DynamoDBCodec[String](valueType = DynamoDBCodec.objectType) {
      override def encoder: Encoder[String] =
        a => AttributeValue.String(a.toString)

      override def decoder: Decoder[String] = {
        case AttributeValue.String(s) => Right(s)
        case other                    => Left(DecodingError(s"Expected String attribute value but got: $other"))
      }
    }

  private[this] val intCodec: DynamoDBCodec[Int] = new DynamoDBCodec[Int](valueType = DynamoDBCodec.intType) {
    override def encoder: Encoder[Int] =
      (a: Int) => AttributeValue.Number(BigDecimal.valueOf(a.toLong))

    override def decoder: Decoder[Int] = {
      case AttributeValue.Number(bd) => Right(bd.intValue)
      case av                        =>
        Left(DecodingError(s"Error getting int value. Expected AttributeValue.Number but found ${av.showType}"))
    }
  }

  private[this] val longCodec: DynamoDBCodec[Long] = new DynamoDBCodec[Long](valueType = DynamoDBCodec.longType) {
    override def encoder: Encoder[Long] = { (a: Long) =>
      AttributeValue.Number(BigDecimal.valueOf(a))
    }

    override def decoder: Decoder[Long] = {
      case AttributeValue.Number(bd) => Right(bd.longValue)
      case av                        =>
        Left(DecodingError(s"Error getting long value. Expected AttributeValue.Number but found ${av.showType}"))
    }
  }

  def deriveCodec[A](
    reflect: Bound[A]
  ): DynamoDBCodec[A] =
    if (reflect.isPrimitive) {
      val primitive = reflect.asPrimitive.get
      if (primitive.primitiveBinding.isInstanceOf[Binding[?, ?]])
        (primitive.primitiveType match {
          case _: PrimitiveType.String => stringCodec
          case _: PrimitiveType.Int    => intCodec
          case _: PrimitiveType.Long   => longCodec
          case x                       =>
            println(s"XXXXX primitive type $x not handled yet")
            ???
        }).asInstanceOf[DynamoDBCodec[A]]
      else primitive.primitiveBinding.asInstanceOf[BindingInstance[DynamoDBCodec, ?, A]].instance.force
    } else if (reflect.isRecord) {
      val record = reflect.asRecord.get
      if (record.recordBinding.isInstanceOf[Binding[?, ?]]) {
        val binding = record.recordBinding.asInstanceOf[Binding.Record[A]]
        var offset  = 0
        val fields  = record.fields

        var fieldInfos: Array[FieldInfo] = null // TODO: investigate recursive cache
        val len                          = fields.length
        if (fieldInfos eq null) {
          fieldInfos = new Array[FieldInfo](len)
          var idx = 0
          while (idx < len) {
            val field        = fields(idx)
            val fieldReflect = field.value
            val codec        = deriveCodec(fieldReflect)
            fieldInfos(idx) = new FieldInfo(field.name, offset, codec)
            offset += codec.valueOffset
            idx += 1
          }
        }

        new DynamoDBCodec[A] {
          private[this] val constructor   = binding.constructor
          private[this] val deconstructor = binding.deconstructor
          private[this] val usedRegisters = offset
          private[this] val fields        = fieldInfos

          override def encoder: Encoder[A] = { value =>
            val regs                         = Registers(usedRegisters)
            var idx                          = 0
            val mapBuilder: JMapView.Builder = AttributeValue.Map.JMapView.hash.builder
            deconstructor.deconstruct(regs, 0, value)
            val len                          = fields.length
            while (idx < len) {
              val field  = fields(idx)
              val name   = field.name
              val offset = field.offset
              val codec  = field.codec
              field.valueType match {
                case DynamoDBCodec.intType    =>
                  val value = regs.getInt(offset, 0)
                  val av    = codec.asInstanceOf[DynamoDBCodec[Int]].encoder(value)
                  mapBuilder.addOne(name, av)
                case DynamoDBCodec.longType   =>
                  val value = regs.getLong(offset, 0)
                  val av    = codec.asInstanceOf[DynamoDBCodec[Long]].encoder(value)
                  mapBuilder.addOne(name, av)
                case DynamoDBCodec.objectType =>
                  val value = regs.getObject(offset, 0)
                  val av    = codec.asInstanceOf[DynamoDBCodec[AnyRef]].encoder(value)
                  mapBuilder.addOne(name, av)
                case _                        =>
                  // TODO: think about what we do here
                  val value = regs.getObject(offset, 0)
                  val av    = codec.asInstanceOf[DynamoDBCodec[AnyRef]].encoder(value)
                  mapBuilder.addOne(name, av)
              }
              idx += 1
            }
            AttributeValue.Map(mapBuilder.result)
          }

          override def decoder: Decoder[A] = {
            val len                         = fields.length
            var idx                         = 0
            val regs                        = Registers(usedRegisters)
            val errors: ArrayBuffer[String] = new ArrayBuffer[String]()

            (av: AttributeValue) =>
              av match {
                case avMap: AttributeValue.Map =>
                  while (idx < len) {
                    val field  = fields(idx)
                    val offset = field.offset
                    val name   = field.name

                    val av: AttributeValue = avMap.value.get(AttributeValue.String(name)).getOrElse(null)
                    if (av eq null) // TODO: Avi - should we fast fail on this?
                      errors.addOne(s"Missing attribute value for field: $name  len: $len")

                    field.valueType match {
                      case DynamoDBCodec.intType    =>
                        field.codec.asInstanceOf[DynamoDBCodec[Int]].decoder(av) match {
                          case Right(value) => regs.setInt(offset, 0, value)
                          case Left(err)    => errors.addOne(err.message)
                        }
                      case DynamoDBCodec.longType   =>
                        field.codec.asInstanceOf[DynamoDBCodec[Long]].decoder(av) match {
                          case Right(value) => regs.setLong(offset, 0, value)
                          case Left(err)    => errors.addOne(err.message)
                        }
                      case DynamoDBCodec.objectType =>
                        field.codec.asInstanceOf[DynamoDBCodec[AnyRef]].decoder(av) match {
                          case Right(value) => regs.setObject(offset, 0, value)
                          case Left(err)    => errors.addOne(err.message)
                        }
                      case _                        => throw new Exception("TODO: decide what to do here")
                    }
                    idx += 1
                  }                                                          // end while
                  if (errors.isEmpty) {
                    val a = constructor.construct(regs, RegisterOffset.Zero)
                    Right(a)
                  } else Left(ItemError.DecodingError(errors.mkString(","))) // TODO: Avi - Make ItemError a composite

                case av: AttributeValue        =>
                  Left(DecodingError(s"Expected Map attribute value but got: ${av.showType}"))
              }
          }
        }
      } else
        record.recordBinding.asInstanceOf[BindingInstance[DynamoDBCodec, ?, A]].instance.force
    } else
      ???

  final case class FieldInfo(
    name: String,
    offset: RegisterOffset,
    codec: DynamoDBCodec[?],
    isOptional: Boolean = false
  ) {
    val valueType: Int = codec.valueType
  }

//  private[this] def isOptional[F[_, _], A](reflect: Reflect[F, A]): Boolean =
//    !requireOptionFields && reflect.isVariant && {
//      val variant  = reflect.asVariant.get
//      val typeName = reflect.typeName
//      val cases    = variant.cases
//      typeName.namespace == Namespace.scala && typeName.name == "Option" &&
//      cases.length == 2 && cases(1).name == "Some"
//    }

}
