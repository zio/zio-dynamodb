package zio.dynamodb.blocks

import zio.blocks.schema.Reflect.Bound
import zio.blocks.schema.binding.BindingType.{ Primitive, Variant, Wrapper }
import zio.blocks.schema.binding.RegisterOffset.RegisterOffset
import zio.blocks.schema.binding._
import zio.blocks.schema.derive.{ BindingInstance, Deriver }
import zio.blocks.schema._
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.{ AttributeValue, Decoder, Encoder }

import scala.collection.mutable.ArrayBuffer

object DummyCodec {
  val stringSchema = new Schema(
    Reflect.Primitive(
      primitiveType = PrimitiveType.String(Validation.None),
      typeName = TypeName(Namespace("scala" :: Nil, Nil), "String"),
      primitiveBinding = Binding.Primitive.string,
      doc = Doc.Empty,
      modifiers = Seq.empty
    )
  )

//  private[this] val cache: ThreadLocal[java.util.HashMap[TypeName[?], CacheEntry2]] =
//    new ThreadLocal[util.HashMap[TypeName[_], CacheEntry2]] {
//      override def initialValue(): java.util.HashMap[TypeName[?], CacheEntry2] = new java.util.HashMap
//    }

  private[this] val stringCodec: DynamoDbCodec[String] =
    new DynamoDbCodec[String](valueType = DynamoDbCodec.objectType) {
      override def encoder: Encoder[String] =
        a => AttributeValue.String(a.toString)

      override def decoder: Decoder[String] = {
        case AttributeValue.String(s) => Right(s)
        case other                    => Left(DecodingError(s"Expected String attribute value but got: $other"))
      }
    }

  private[this] val intCodec: DynamoDbCodec[Int] = new DynamoDbCodec[Int](valueType = DynamoDbCodec.intType) {
    override def encoder: Encoder[Int] =
      (a: Int) => AttributeValue.Number(BigDecimal.valueOf(a.toLong))

    override def decoder: Decoder[Int] = {
      case AttributeValue.Number(bd) => Right(bd.intValue)
      case av                        =>
        Left(DecodingError(s"Error getting int value. Expected AttributeValue.Number but found ${av.showType}"))
    }
  }

  private[this] val longCodec: DynamoDbCodec[Long] = new DynamoDbCodec[Long](valueType = DynamoDbCodec.longType) {
    override def encoder: Encoder[Long] = { (a: Long) =>
      AttributeValue.Number(BigDecimal.valueOf(a))
    }

    override def decoder: Decoder[Long] = {
      case AttributeValue.Number(bd) => Right(bd.longValue)
      case av                        =>
        Left(DecodingError(s"Error getting long value. Expected AttributeValue.Number but found ${av.showType}"))
    }
  }

  object DummyDeriver extends Deriver[DynamoDbCodec] {
    override def derivePrimitive[F[_, _], A](
      primitiveType: PrimitiveType[A],
      typeName: TypeName[A],
      binding: Binding[Primitive, A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    ): Lazy[DynamoDbCodec[A]] =
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
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec[A]] =
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
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec[A]] = ???

    override def deriveSequence[F[_, _], C[_], A](
      element: Reflect[F, A],
      typeName: TypeName[C[A]],
      binding: Binding[BindingType.Seq[C], C[A]],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec[C[A]]] = ???

    override def deriveMap[F[_, _], M[_, _], K, V](
      key: Reflect[F, K],
      value: Reflect[F, V],
      typeName: TypeName[M[K, V]],
      binding: Binding[BindingType.Map[M], M[K, V]],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec[M[K, V]]] = ???

    override def deriveDynamic[F[_, _]](
      binding: Binding[BindingType.Dynamic, DynamicValue],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec[DynamicValue]] = ???

    override def deriveWrapper[F[_, _], A, B](
      wrapped: Reflect[F, B],
      typeName: TypeName[A],
      wrapperPrimitiveType: Option[PrimitiveType[A]],
      binding: Binding[Wrapper[A, B], A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec[A]] = ???
  }

  def deriveCodec[A](
    reflect: Bound[A]
  ): DynamoDbCodec[A] =
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
        }).asInstanceOf[DynamoDbCodec[A]]
      else primitive.primitiveBinding.asInstanceOf[BindingInstance[DynamoDbCodec, ?, A]].instance.force
    } else if (reflect.isRecord) {
      val record = reflect.asRecord.get
      if (record.recordBinding.isInstanceOf[Binding[?, ?]]) {
        val binding = record.recordBinding.asInstanceOf[Binding.Record[A]]
        var offset  = 0
        val fields  = record.fields

        var fieldInfos: Array[FieldInfo] = null // TODO: investigate recursive cache
        val len                          = fields.length
        if (fieldInfos == null) {
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

        new DynamoDbCodec[A] {
          private[this] val constructor   = binding.constructor
          private[this] val deconstructor = binding.deconstructor
          private[this] val usedRegisters = offset
          private[this] val fields        = fieldInfos

          override def encoder: Encoder[A] = { value =>
            val regs       = Registers(usedRegisters)
            var idx        = 0
            deconstructor.deconstruct(regs, 0, value)
            val mapBuilder = Map.newBuilder[AttributeValue.String, AttributeValue]
            val len        = fields.length
            while (idx < len) {
              val field              = fields(idx)
              val name               = field.name
              val offset             = field.offset
              val codec              = field.codec
              var av: AttributeValue = null
              field.valueType match {
                case DynamoDbCodec.intType    =>
                  val value = regs.getInt(offset, 0)
                  av = codec.asInstanceOf[DynamoDbCodec[Int]].encoder(value)
                case DynamoDbCodec.longType   =>
                  val value = regs.getLong(offset, 0)
                  av = codec.asInstanceOf[DynamoDbCodec[Long]].encoder(value)
                case DynamoDbCodec.objectType =>
                  val value = regs.getObject(offset, 0)
                  av = codec.asInstanceOf[DynamoDbCodec[AnyRef]].encoder(value)
                case _                        =>
                  // TODO: think about what we do here
                  val value = regs.getObject(offset, 0)
                  av = codec.asInstanceOf[DynamoDbCodec[AnyRef]].encoder(value)
              }
              mapBuilder.addOne(AttributeValue.String(name) -> av)
              idx += 1
            }
            AttributeValue.Map(mapBuilder.result())
          }

          override def decoder: Decoder[A] = {
            val len                         = fields.length
            var idx                         = 0
            val regs                        = Registers(usedRegisters)
            val avMapBuilder                = Map.newBuilder[AttributeValue.String, AttributeValue]
            avMapBuilder.sizeHint(len)
            val errors: ArrayBuffer[String] = new ArrayBuffer[String]()

            (av: AttributeValue) =>
              av match {
                case avMap: AttributeValue.Map =>
                  while (idx < len) {
                    val field  = fields(idx)
                    val offset = field.offset
                    val name   = field.name

                    val av: AttributeValue = avMap.value.get(AttributeValue.String(name)).getOrElse(null)
                    if (av eq null) // TODO: obvs !!!
                      throw new Exception(s"Missing attribute value for field: $name")

                    field.valueType match {
                      case DynamoDbCodec.intType    =>
                        field.codec.asInstanceOf[DynamoDbCodec[Int]].decoder(av) match {
                          case Right(value) => regs.setInt(offset, 0, value)
                          case Left(err)    => errors.addOne(err.message)
                        }
                      case DynamoDbCodec.longType   =>
                        field.codec.asInstanceOf[DynamoDbCodec[Long]].decoder(av) match {
                          case Right(value) => regs.setLong(offset, 0, value)
                          case Left(err)    => errors.addOne(err.message)
                        }
                      case DynamoDbCodec.objectType =>
                        field.codec.asInstanceOf[DynamoDbCodec[AnyRef]].decoder(av) match {
                          case Right(value) => regs.setObject(offset, 0, value)
                          case Left(err)    => errors.addOne(err.message)
                        }
                      case _                        => throw new Exception("TODO: decide what to do here")
                    }
                    idx += 1
                  }                                                          // end while
                  if (errors.isEmpty) {
                    val a = constructor.construct(regs, RegisterOffset.Zero)
                    Right(a.asInstanceOf[A])
                  } else Left(ItemError.DecodingError(errors.mkString(","))) // TODO: Avi - Make ItemError a composite

                case av: AttributeValue        =>
                  Left(DecodingError(s"Expected Map attribute value but got: ${av.showType}"))
              }
          }
        }
      } else {
        println(s"XXXXX record is NOT Binding: $reflect")
        record.recordBinding.asInstanceOf[BindingInstance[DynamoDbCodec, ?, A]].instance.force
      }
    } else {
      println(s"XXXXX reflect: $reflect not handled yet")
      ???
    }

  final class CacheEntry2 private (
    val fieldCodecs: Array[DynamoDbCodec[?]],
    names: Array[String]
  )                  {
    def size: Int                 = fieldCodecs.length // TODO: Avi - for debugging - remove
    override def toString: String = s"CacheEntry2(${fieldCodecs.toSeq}, ${names.toSeq})"

    private[this] var _nameToIndex: Map[String, Int] = null // TODO: Avi - investigate savings in getting rid of Map
    private[this] val hasNames                       = names.nonEmpty

    private def nameToIndex: Map[String, Int] = {
      var local = _nameToIndex
      if (local eq null) {
        if (hasNames)
          local = names.zipWithIndex.toMap
        else
          local = Map.empty
        _nameToIndex = local
      }
      local
    }

    def addEntry(codec: DynamoDbCodec[?], name: String, index: Int): Unit = {
      fieldCodecs(index) = codec
      if (hasNames)
        names(index) = name
    }

    def byIndex(i: Int): DynamoDbCodec[?] = fieldCodecs(i)

    def byName(name: String): Option[DynamoDbCodec[?]] =
      if (!hasNames) None
      else nameToIndex.get(name).map(fieldCodecs)
  }
  object CacheEntry2 {
    def makeWithNames(size: Int) =
      new CacheEntry2(new Array[DynamoDbCodec[?]](size), new Array[String](size))
  }

  final case class FieldInfo(
    name: String,
    offset: RegisterOffset,
    codec: DynamoDbCodec[?],
    isOptional: Boolean = false
  ) {
    val valueType: Int = codec.valueType
  }
}
