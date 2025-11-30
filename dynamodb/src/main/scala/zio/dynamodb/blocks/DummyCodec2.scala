package zio.dynamodb.blocks

import zio.blocks.schema.Reflect.Bound
import zio.blocks.schema._
import zio.blocks.schema.binding.BindingType.{ Primitive, Variant, Wrapper }
import zio.blocks.schema.binding.RegisterOffset.RegisterOffset
import zio.blocks.schema.binding._
import zio.blocks.schema.derive.{ BindingInstance, Deriver }
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBError.ItemError.DecodingError

import scala.collection.mutable.ArrayBuffer

object DummyCodec2 {

//  private[this] val cache: ThreadLocal[java.util.HashMap[TypeName[?], CacheEntry2]] =
//    new ThreadLocal[util.HashMap[TypeName[_], CacheEntry2]] {
//      override def initialValue(): java.util.HashMap[TypeName[?], CacheEntry2] = new java.util.HashMap
//    }

  private[this] val stringCodec: DynamoDbCodec2[String] =
    new DynamoDbCodec2[String](valueType = DynamoDbCodec2.objectType) {
      override def encoder: Encoder2[String] =
        a => AttributeValue2.String(a.toString)

      override def decoder: Decoder2[String] = {
        case AttributeValue2.String(s) => Right(s)
        case other                     => Left(DecodingError(s"Expected String attribute value but got: $other"))
      }
    }

  private[this] val intCodec: DynamoDbCodec2[Int] = new DynamoDbCodec2[Int](valueType = DynamoDbCodec2.intType) {
    override def encoder: Encoder2[Int] =
      (a: Int) => AttributeValue2.Number(BigDecimal.valueOf(a.toLong))

    override def decoder: Decoder2[Int] = {
      case AttributeValue2.Number(bd) => Right(bd.intValue)
      case av                         =>
        Left(DecodingError(s"Error getting int value. Expected AttributeValue2.Number but found ${av.showType}"))
    }
  }

  private[this] val longCodec: DynamoDbCodec2[Long] = new DynamoDbCodec2[Long](valueType = DynamoDbCodec2.longType) {
    override def encoder: Encoder2[Long] = { (a: Long) =>
      AttributeValue2.Number(BigDecimal.valueOf(a))
    }

    override def decoder: Decoder2[Long] = {
      case AttributeValue2.Number(bd) => Right(bd.longValue)
      case av                         =>
        Left(DecodingError(s"Error getting long value. Expected AttributeValue2.Number but found ${av.showType}"))
    }
  }

  object DummyDeriver extends Deriver[DynamoDbCodec2] {
    override def derivePrimitive[F[_, _], A](
      primitiveType: PrimitiveType[A],
      typeName: TypeName[A],
      binding: Binding[Primitive, A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    ): Lazy[DynamoDbCodec2[A]] =
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
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec2[A]] =
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
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec2[A]] = ???

    override def deriveSequence[F[_, _], C[_], A](
      element: Reflect[F, A],
      typeName: TypeName[C[A]],
      binding: Binding[BindingType.Seq[C], C[A]],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec2[C[A]]] = ???

    override def deriveMap[F[_, _], M[_, _], K, V](
      key: Reflect[F, K],
      value: Reflect[F, V],
      typeName: TypeName[M[K, V]],
      binding: Binding[BindingType.Map[M], M[K, V]],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec2[M[K, V]]] = ???

    override def deriveDynamic[F[_, _]](
      binding: Binding[BindingType.Dynamic, DynamicValue],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec2[DynamicValue]] = ???

    override def deriveWrapper[F[_, _], A, B](
      wrapped: Reflect[F, B],
      typeName: TypeName[A],
      wrapperPrimitiveType: Option[PrimitiveType[A]],
      binding: Binding[Wrapper[A, B], A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec2[A]] = ???
  }

  def deriveCodec[A](
    reflect: Bound[A]
  ): DynamoDbCodec2[A] =
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
        }).asInstanceOf[DynamoDbCodec2[A]]
      else primitive.primitiveBinding.asInstanceOf[BindingInstance[DynamoDbCodec2, ?, A]].instance.force
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

        new DynamoDbCodec2[A] {
          private[this] val constructor   = binding.constructor
          private[this] val deconstructor = binding.deconstructor
          private[this] val usedRegisters = offset
          private[this] val fields        = fieldInfos

          override def encoder: Encoder2[A] = { value =>
            val regs    = Registers(usedRegisters)
            var idx     = 0
            deconstructor.deconstruct(regs, 0, value)
            val len     = fields.length
            val hashMap = new java.util.HashMap[String, AttributeValue2](len)
            while (idx < len) {
              val field  = fields(idx)
              val name   = field.name
              val offset = field.offset
              val codec  = field.codec
              field.valueType match {
                case DynamoDbCodec2.intType    =>
                  val value = regs.getInt(offset, 0)
                  val av    = codec.asInstanceOf[DynamoDbCodec2[Int]].encoder(value)
                  hashMap.put(name, av)
                case DynamoDbCodec2.longType   =>
                  val value = regs.getLong(offset, 0)
                  val av    = codec.asInstanceOf[DynamoDbCodec2[Long]].encoder(value)
                  hashMap.put(name, av)
                case DynamoDbCodec2.objectType =>
                  val value = regs.getObject(offset, 0)
                  val av    = codec.asInstanceOf[DynamoDbCodec2[AnyRef]].encoder(value)
                  hashMap.put(name, av)
                case _                         =>
                  // TODO: think about what we do here
                  val value = regs.getObject(offset, 0)
                  val av    = codec.asInstanceOf[DynamoDbCodec2[AnyRef]].encoder(value)
                  hashMap.put(name, av)
              }
              idx += 1
            }
            AttributeValue2.Map(hashMap)
          }

          override def decoder: Decoder2[A] = {
            val len                         = fields.length
            var idx                         = 0
            val regs                        = Registers(usedRegisters)
            val avMapBuilder                = Map.newBuilder[AttributeValue2.String, AttributeValue2]
            avMapBuilder.sizeHint(len)
            val errors: ArrayBuffer[String] = new ArrayBuffer[String]()

            (av: AttributeValue2) =>
              av match {
                case avMap: AttributeValue2.Map =>
                  while (idx < len) {
                    val field  = fields(idx)
                    val offset = field.offset
                    val name   = field.name

                    val av: AttributeValue2 = avMap.underlying.getNullable(name)
                    if (av eq null) // TODO: obvs !!!
                      throw new Exception(s"Missing attribute value for field: $name")

                    field.valueType match {
                      case DynamoDbCodec2.intType    =>
                        field.codec.asInstanceOf[DynamoDbCodec2[Int]].decoder(av) match {
                          case Right(value) => regs.setInt(offset, 0, value)
                          case Left(err)    => errors.addOne(err.message)
                        }
                      case DynamoDbCodec2.longType   =>
                        field.codec.asInstanceOf[DynamoDbCodec2[Long]].decoder(av) match {
                          case Right(value) => regs.setLong(offset, 0, value)
                          case Left(err)    => errors.addOne(err.message)
                        }
                      case DynamoDbCodec2.objectType =>
                        field.codec.asInstanceOf[DynamoDbCodec2[AnyRef]].decoder(av) match {
                          case Right(value) => regs.setObject(offset, 0, value)
                          case Left(err)    => errors.addOne(err.message)
                        }
                      case _                         => throw new Exception("TODO: decide what to do here")
                    }
                    idx += 1
                  }                                                          // end while
                  if (errors.isEmpty) {
                    val a = constructor.construct(regs, RegisterOffset.Zero)
                    Right(a)
                  } else Left(ItemError.DecodingError(errors.mkString(","))) // TODO: Avi - Make ItemError a composite

                case av: AttributeValue2        =>
                  Left(DecodingError(s"Expected Map attribute value but got: ${av.showType}"))
              }
          }
        }
      } else {
        println(s"XXXXX record is NOT Binding: $reflect")
        record.recordBinding.asInstanceOf[BindingInstance[DynamoDbCodec2, ?, A]].instance.force
      }
    } else {
      println(s"XXXXX reflect: $reflect not handled yet")
      ???
    }

//  private[this] def isTuple[F[_, _], A](reflect: Reflect[F, A]): Boolean =
//    reflect.isRecord && {
//      val typeName = reflect.typeName
//      typeName.namespace == Namespace.scala && typeName.name.startsWith("Tuple")
//    }

  final case class FieldInfo(
    name: String,
    offset: RegisterOffset,
    codec: DynamoDbCodec2[?],
    isOptional: Boolean = false
  ) {
    val valueType: Int = codec.valueType
  }

  type Encoder2[A]  = A => AttributeValue2
  type Decoder2[+A] = AttributeValue2 => Either[ItemError, A]

  sealed trait AttributeValue2 { self =>
    private[dynamodb] final val showType: String =
      self match {
        case _: AttributeValue2.String => "AttributeValue2.String"
        case _: AttributeValue2.Number => "AttributeValue2.Number"
        case _: AttributeValue2.Map    => "AttributeValue2.Map"
      }
  }
  object AttributeValue2       {
    final case class String(value: scala.Predef.String) extends AttributeValue2
    final case class Number(value: BigDecimal)          extends AttributeValue2
    final case class Map(underlying: MyMap)             extends AttributeValue2 {
      def value: scala.collection.immutable.Map[scala.Predef.String, AttributeValue2] =
        underlying
    }
    object Map {
      def apply(value: java.util.HashMap[scala.Predef.String, AttributeValue2]): AttributeValue2.Map =
        AttributeValue2.Map(new MyMap(value))
    }

  }

  final class MyMap(private val underlying: java.util.HashMap[String, AttributeValue2])
      extends scala.collection.immutable.AbstractMap[String, AttributeValue2] {

    override def get(key: String): Option[AttributeValue2] = {
      val v = underlying.get(key)
      if (v eq null) None else Some(v)
    }

    def getNullable(key: String): AttributeValue2 =
      underlying.get(key)

    override def iterator: Iterator[(String, AttributeValue2)] = {
      import scala.jdk.CollectionConverters._
      underlying.entrySet().asScala.iterator.map(e => (e.getKey, e.getValue))
    }

    override def removed(key: String): MyMap = {
      val copy = new java.util.HashMap[String, AttributeValue2](underlying)
      copy.remove(key)
      new MyMap(copy)
    }

    override def updated[V1 >: AttributeValue2](key: String, value: V1): MyMap = {
      val copy = new java.util.HashMap[String, AttributeValue2](underlying)
      copy.put(key, value.asInstanceOf[AttributeValue2])
      new MyMap(copy)
    }
  }
}
