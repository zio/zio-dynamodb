package zio.dynamodb.blocks

import zio.blocks.schema.Reflect.Bound
import zio.blocks.schema._
import zio.blocks.schema.binding._
import zio.blocks.schema.derive.{ BindingInstance, Deriver }
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.{ AttributeValue, Decoder, Encoder, FromAttributeValue }

import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer

/**
 * borrows heavily from Andriy Plokhotnyuk's zio-blocks codecs https://github.com/zio/zio-blocks
 */
object BlocksDdbDerived2 extends Deriver[DdbCodec] { self =>
  sealed trait VariantMetaData
  // TODO: Avi - extract simple codecs that do not need context as vals to save memory allocations

  override def derivePrimitive[F[_, _], A](
    primitiveType: PrimitiveType[A],
    typeName: TypeName[A],
    binding: Binding[BindingType.Primitive, A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  ): Lazy[DdbCodec[A]] =
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
    fields: IndexedSeq[Term[F, A, ?]],
    typeName: TypeName[A],
    binding: Binding[BindingType.Record, A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[A]] =
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
    cases: IndexedSeq[Term[F, A, ?]], // TOD: update Derive deriveVariant signature to match Variant with ? <: A
    typeName: TypeName[A],
    binding: Binding[BindingType.Variant, A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[A]] =
    Lazy(
      deriveCodec(
        Reflect.Variant(
          cases =
            cases.asInstanceOf[IndexedSeq[Term[Binding, A, _ <: A]]], // TODO: Avi - formatter complains about ? <: A
          typeName = typeName,
          variantBinding = binding,
          doc = doc,
          modifiers = modifiers
        )
      )
    )

  override def deriveSequence[F[_, _], C[_], A](
    element: Reflect[F, A],
    typeName: TypeName[C[A]],
    binding: Binding[BindingType.Seq[C], C[A]],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[C[A]]] =
    Lazy(
      deriveCodec(
        Reflect.Sequence(
          element = element.asInstanceOf[Reflect[Binding, A]],
          typeName = typeName,
          seqBinding = binding,
          doc = doc,
          modifiers = modifiers
        )
      )
    )

  override def deriveMap[F[_, _], M[_, _], K, V](
    key: Reflect[F, K],
    value: Reflect[F, V],
    typeName: TypeName[M[K, V]],
    binding: Binding[BindingType.Map[M], M[K, V]],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[M[K, V]]] =
    Lazy(
      deriveCodec(
        Reflect.Map(
          key = key.asInstanceOf[Reflect[Binding, K]],
          value = value.asInstanceOf[Reflect[Binding, V]],
          typeName = typeName,
          mapBinding = binding,
          doc = doc,
          modifiers = modifiers
        )
      )
    )

  override def deriveDynamic[F[_, _]](
    binding: Binding[BindingType.Dynamic, DynamicValue],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[DynamicValue]] =
    Lazy(
      new DdbCodec[DynamicValue] {
        override def encoder: Encoder[DynamicValue] = ???
        override def decoder: Decoder[DynamicValue] = ???
      }
    )

  override def deriveWrapper[F[_, _], A, B](
    wrapped: Reflect[F, B],
    typeName: TypeName[A],
    wrapperPrimitiveType: Option[PrimitiveType[A]],
    binding: Binding[BindingType.Wrapper[A, B], A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[A]] =
    Lazy(
      new DdbCodec[A] {
        val wrapper                      = Reflect.Wrapper(
          wrapped = wrapped.asInstanceOf[Reflect[Any, B]],
          typeName = typeName,
          wrapperPrimitiveType = wrapperPrimitiveType,
          wrapperBinding = binding,
          doc = doc,
          modifiers = modifiers
        )
        println(wrapper)
        override def encoder: Encoder[A] = ???
        override def decoder: Decoder[A] = ???
      }
    )

  type Elem
  type Col[_]
  type Key
  type Value
  type Map[_, _]

  final class CacheEntry private (
    val fieldCodecs: Array[DdbCodec[?]],
    private val names: Array[String]
  ) {

    private[this] val hasNames: Boolean = names.nonEmpty

    def addEntry(codec: DdbCodec[?], name: String, index: Int): Unit = {
      fieldCodecs(index) = codec
      if (hasNames) names(index) = name
    }

    def byIndex(i: Int): DdbCodec[?] =
      fieldCodecs(i)

    def byName(name: String): Option[DdbCodec[?]] = {
      if (!hasNames) return None

      val arr = names
      var i   = 0
      val n   = arr.length
      while (i < n) {
        if (arr(i) eq name) return Some(fieldCodecs(i))
        if (arr(i) != null && arr(i) == name) return Some(fieldCodecs(i))
        i += 1
      }
      None
    }

    override def toString: String =
      s"CacheEntry(${fieldCodecs.toSeq}, ${names.toSeq})"
  }

  object CacheEntry {
    def makeWithNames(size: Int): CacheEntry =
      new CacheEntry(
        new Array[DdbCodec[?]](size),
        new Array[String](size)
      )

    def makeWithoutNames(size: Int): CacheEntry =
      new CacheEntry(
        new Array[DdbCodec[?]](size),
        Array.empty
      )
  }

  def enumCodec[A](typeName: TypeName[A]): DdbCodec[A] =
    new DdbCodec[A] {
      override def encoder: Encoder[A] = (_: A) => AttributeValue.String(typeName.name)

      override def decoder: Decoder[A] =
        // TODO: Avi - get CacheEntry for enum parent name
        ???
    }

  private val intCodec: DdbCodec[Int] = new DdbCodec[Int] {
    override def encoder: Encoder[Int] =
      (a: Int) => AttributeValue.Number(BigDecimal.valueOf(a.toLong))

    override def decoder: Decoder[Int] =
      (av: AttributeValue) =>
        FromAttributeValue.intFromAttributeValue
          .fromAttributeValue(av)
          .asInstanceOf[Either[zio.dynamodb.DynamoDBError.ItemError, Int]]
  }

  private val stringCodec: DdbCodec[String] = new DdbCodec[String] {
    override def encoder: Encoder[String] =
      (a: String) => AttributeValue.String(a)

    override def decoder: Decoder[String] =
      (av: AttributeValue) => FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av)
  }
  private val longCodec                     = new DdbCodec[Long] {
    override def encoder: Encoder[Long] =
      (a: Long) => AttributeValue.Number(BigDecimal.valueOf(a))

    override def decoder: Decoder[Long] =
      (av: AttributeValue) =>
        FromAttributeValue.longFromAttributeValue
          .fromAttributeValue(av)
          .asInstanceOf[Either[zio.dynamodb.DynamoDBError.ItemError, Long]]
  }

  val byteCodec = new DdbCodec[Byte] {
    override def encoder: Encoder[Byte] =
      (a: Byte) => AttributeValue.Binary(zio.Chunk(a))

    override def decoder: Decoder[Byte] =
      (av: AttributeValue) =>
        FromAttributeValue.byteFromAttributeValue
          .fromAttributeValue(av)
          .asInstanceOf[Either[zio.dynamodb.DynamoDBError.ItemError, Byte]]
  }

  val nativeStringSetCodec: DdbCodec[Set[String]] = new DdbCodec[Set[String]] {
    override def encoder: Encoder[Set[String]] =
      (a: Set[String]) => {
        val ss = a
        AttributeValue.StringSet(ss)
      }

    override def decoder: Decoder[Set[String]] = {
      case AttributeValue.StringSet(value) => Right(value.asInstanceOf[Set[String]])
      case av                              => Left(ItemError.DecodingError(s"Expected AttributeValue.StringSet, found ${av.showType}"))
    }
  }

  trait NumberOps[A] {
    def toBigDecimal(a: A): BigDecimal
    def fromBigDecimal(bd: BigDecimal): A
  }

  object NumberOps {
    implicit val intOps: NumberOps[Int] = new NumberOps[Int] {
      def toBigDecimal(a: Int)           = BigDecimal.valueOf(a.toLong)
      def fromBigDecimal(bd: BigDecimal) = bd.intValue
    }

    implicit val longOps: NumberOps[Long] = new NumberOps[Long] {
      def toBigDecimal(a: Long)          = BigDecimal.valueOf(a)
      def fromBigDecimal(bd: BigDecimal) = bd.longValue
    }

    implicit val floatOps: NumberOps[Float] = new NumberOps[Float] {
      def toBigDecimal(a: Float)         = BigDecimal.decimal(a)
      def fromBigDecimal(bd: BigDecimal) = bd.floatValue
    }

    implicit val doubleOps: NumberOps[Double] = new NumberOps[Double] {
      def toBigDecimal(a: Double)        = BigDecimal.valueOf(a)
      def fromBigDecimal(bd: BigDecimal) = bd.doubleValue
    }

    implicit val shortOps: NumberOps[Short] = new NumberOps[Short] {
      def toBigDecimal(a: Short)         = BigDecimal.valueOf(a.toLong)
      def fromBigDecimal(bd: BigDecimal) = bd.shortValue
    }

    implicit val bigDecOps: NumberOps[BigDecimal] = new NumberOps[BigDecimal] {
      def toBigDecimal(a: BigDecimal)    = a
      def fromBigDecimal(bd: BigDecimal) = bd
    }
  }

  def nativeNumericSetCodec[A](implicit ops: NumberOps[A]): DdbCodec[Set[A]] =
    new DdbCodec[Set[A]] {
      override def encoder: Encoder[Set[A]] =
        (ns: Set[A]) => {
          val builder = HashSet.newBuilder[BigDecimal]
          builder.sizeHint(ns.size)
          ns.foreach(a => builder += ops.toBigDecimal(a))
          AttributeValue.NumberSet(builder.result())
        }

      override def decoder: Decoder[Set[A]] = {
        case AttributeValue.NumberSet(values) =>
          val builder = HashSet.newBuilder[A]
          builder.sizeHint(values.size)
          values.foreach(bd => builder += ops.fromBigDecimal(bd))
          Right(builder.result())
        case av                               =>
          Left(ItemError.DecodingError(s"Expected AttributeValue.NumberSet, found ${av.showType}"))
      }
    }

  def binarySetCodec: DdbCodec[Set[zio.Chunk[Byte]]] =
    new DdbCodec[Set[zio.Chunk[Byte]]] {
      override def encoder: Encoder[Set[zio.Chunk[Byte]]] =
        (bs: Set[zio.Chunk[Byte]]) => AttributeValue.BinarySet(bs)

      override def decoder: Decoder[Set[zio.Chunk[Byte]]] = {
        case AttributeValue.BinarySet(values) => Right(values.asInstanceOf[Set[zio.Chunk[Byte]]])
        case av                               => Left(ItemError.DecodingError(s"Expected AttributeValue.BinarySet, found ${av.showType}"))
      }
    }

  private def primitiveSetCodecOrNull[A](element: Reflect[Binding, A]): DdbCodec[A] = {
    if (!element.isPrimitive) return null

    element.asPrimitive.get.primitiveType match {
      case _: PrimitiveType.String => nativeStringSetCodec.asInstanceOf[DdbCodec[A]]
      case _: PrimitiveType.Int    => nativeNumericSetCodec[Int].asInstanceOf[DdbCodec[A]]
      case _: PrimitiveType.Long   => nativeNumericSetCodec[Long].asInstanceOf[DdbCodec[A]]
      case _                       => null
    }
  }

  private def isByteSequence[A](element: Reflect[Binding, A]): Boolean =
    element.isSequence &&
      element.asSequenceUnknown.exists { unknown =>
        unknown.sequence.asSequence.exists { seq =>
          seq.element.isPrimitive &&
          seq.element.asPrimitive.get.primitiveType.isInstanceOf[PrimitiveType.Byte]
        }
      }
  private def chooseNativeSetCodecOrNull[A](element: Reflect[Binding, A]): DdbCodec[A] = {
    val prim = primitiveSetCodecOrNull(element)
    if (prim != null) prim
    else if (isByteSequence(element)) binarySetCodec.asInstanceOf[DdbCodec[A]]
    else null
  }

  def parseTupleN(s: String): Int = {
    val len = s.length
    // Fast fail if the string is too short
    if (len <= 5) return -1

    // Check pattern "Tuple"
    if (
      s.charAt(0) != 'T' ||
      s.charAt(1) != 'u' ||
      s.charAt(2) != 'p' ||
      s.charAt(3) != 'l' ||
      s.charAt(4) != 'e'
    ) return -1

    // Parse digits after "Tuple" into an Int
    var i     = 5
    var value = 0

    if (i == len) return -1 // no digits

    while (i < len) {
      val c = s.charAt(i)
      if (c < '0' || c > '9') return -1
      value = value * 10 + (c - '0')
      i += 1
    }

    value
  }

  private def deriveCodec[A](
    reflect: Bound[A],
    cache: java.util.HashMap[TypeName[?], CacheEntry] = new java.util.HashMap()
  ): DdbCodec[A] = {
    if (reflect.isPrimitive) {
      val primitiveType = reflect.asPrimitive.get.primitiveType
      primitiveType match {
        // TODO: Avi - handle other primitive types
        case _: PrimitiveType.String => stringCodec
        case _: PrimitiveType.Int    => intCodec
        case _: PrimitiveType.Long   => longCodec
        case _: PrimitiveType.Byte   => byteCodec
        case _                       => ??? // TODO: Avi - other types
      }
    } else if (reflect.isRecord) {

      val record = reflect.asRecord.get

      val recordBinding =
        try record.recordBinding.asInstanceOf[Binding.Record[A]]
        catch {
          case _: Exception =>
            record.recordBinding
              .asInstanceOf[BindingInstance[DdbCodec, ?, A]]
              .binding
              .asInstanceOf[Binding.Record[A]]
        }
      val constructor   = recordBinding.constructor
      val deconstructor = recordBinding.deconstructor
      val fields        = record.fields

      // TODO: Avi - we end up with empty CacheEntry memory alloc for simple enum that is not used
      val fieldCodecs = cache.get(record.typeName) match {
        case null =>
          val codecs: CacheEntry = CacheEntry.makeWithNames(fields.length)
          if (!fields.isEmpty) {
            cache.put(record.typeName, codecs)
            val len = fields.length
            var idx = 0
            while (idx < len) {
              val reflect = fields(idx).value
              codecs.addEntry(deriveCodec(reflect, cache), fields(idx).name, idx)
              idx += 1
            }
          }
          codecs
        case x    => x
      }

      new DdbCodec[A] {
        override def encoder: Encoder[A] = {
          val encoder: Encoder[A] = (a: A) => {
            val mapBuilder = Map.newBuilder[AttributeValue.String, AttributeValue]
            mapBuilder.sizeHint(fields.length)

            val registers = Registers(record.usedRegisters)
            deconstructor.deconstruct(registers, RegisterOffset.Zero, a)
            var offset    = RegisterOffset.Zero
            var idx       = -1

            val av: AttributeValue = {
              fields.foreach { field =>
                idx += 1
                val encoder           = fieldCodecs.byIndex(idx).encoder
                val reflect           = field.value
                val fieldName         = field.name
                val avStringFieldName = AttributeValue.String(fieldName)

                if (reflect.isPrimitive) {
                  val primitiveType = reflect.asPrimitive.get.primitiveType
                  primitiveType match {
                    case _: PrimitiveType.Int  =>
                      val av: AttributeValue =
                        encoder.asInstanceOf[Int => AttributeValue](registers.getInt(offset, 0))
                      mapBuilder.addOne(avStringFieldName -> av)
                      offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
                    case _: PrimitiveType.Long =>
                      val av: AttributeValue =
                        encoder.asInstanceOf[Long => AttributeValue](registers.getLong(offset, 0))
                      mapBuilder.addOne(avStringFieldName -> av)
                      offset = RegisterOffset.add(offset, RegisterOffset(longs = 1))
                    case _                     =>
                      val av = encoder.asInstanceOf[AnyRef => AttributeValue](registers.getObject(offset, 0))
                      mapBuilder.addOne(avStringFieldName -> av)
                      offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                  }
                } else {
                  val av = encoder.asInstanceOf[AnyRef => AttributeValue](registers.getObject(offset, 0))
                  mapBuilder.addOne(avStringFieldName -> av)

                  offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                }
              }
              AttributeValue.Map(mapBuilder.result()) // end of not a TupleN
            }

            av
          }
          encoder
        }

        override def decoder: Decoder[A] =
          (av: AttributeValue) => {
            val errors: ArrayBuffer[String]  = new ArrayBuffer // TODO: Avi - initialise size
            val registers                    = Registers(record.usedRegisters)
            var offset                       = RegisterOffset.Zero
            var idx                          = -1
            var it: Iterator[AttributeValue] = null

            // set up registers with decoded values for later construction
            def decodeAndSetRegisters(av: AttributeValue): Unit = {
              if (av.isInstanceOf[AttributeValue.List])
                it = av.asInstanceOf[AttributeValue.List].value.iterator

              fields.foreach { field =>
                idx += 1
                val fieldDecoder = fieldCodecs.byIndex(idx).decoder
                val fieldReflect = field.value

                // TODO: Avi use Null return to save object allocation
                def getField(av: AttributeValue, fieldName: String): Either[ItemError, AttributeValue] =
                  av match {
                    case m: AttributeValue.Map =>
                      m.get(fieldName)
                        .toRight(
                          ItemError.DecodingError(
                            s"Field name: '$fieldName' not found in record ${av /*av.showType*/}"
                          )
                        )
                    case _                     =>
                      Left(
                        ItemError.DecodingError(
                          s"Error decoded - TODO: better error message ${av.showType}"
                        )
                      )
                  }

                getField(
                  av,
                  field.name
                ) match {
                  case Right(avValue) =>
                    if (fieldReflect.isPrimitive) {
                      val primitiveType = fieldReflect.asPrimitive.get.primitiveType
                      primitiveType match {
                        case _: PrimitiveType.Int  =>
                          fieldDecoder.asInstanceOf[AnyRef => Either[ItemError, Int]](avValue) match {
                            case Left(err)  => errors.addOne(err.message)
                            case Right(int) =>
                              registers.setInt(offset, 0, int)
                              offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
                          }
                        case _: PrimitiveType.Long =>
                          fieldDecoder.asInstanceOf[AnyRef => Either[ItemError, Long]](avValue) match {
                            case Left(err)  => errors.addOne(err.message)
                            case Right(lng) =>
                              registers.setLong(offset, 0, lng)
                              offset = RegisterOffset.add(offset, RegisterOffset(longs = 1))
                          }
                        case _                     => // TODO: Avi - other primitive types
                          fieldDecoder.asInstanceOf[AnyRef => Either[ItemError, AnyRef]](avValue) match {
                            case Left(err)     => errors.addOne(err.message)
                            case Right(anyRef) =>
                              registers.setObject(offset, 0, anyRef)
                              offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                          }
                      }
                    } else
                      fieldDecoder.asInstanceOf[AnyRef => Either[ItemError, AnyRef]](avValue) match {
                        case Left(err)     => errors.addOne(err.message)
                        case Right(anyRef) =>
                          registers.setObject(offset, 0, anyRef)
                          offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                      }
                  case Left(error)    => // TODO: Avi - delay error creation if possible to save a memory allocation
                    errors.addOne(error.message)
                }
              } // end decodeAndSetRegisters
            }

            val isAvMap = av.isInstanceOf[AttributeValue.Map]

            if (isAvMap)
              decodeAndSetRegisters(av)
            else
              errors.addOne(s"Expected AttributeValue.Map, found ${av.showType}")
            if (errors.isEmpty) {
              val a = constructor.construct(registers, RegisterOffset.Zero)
              Right(a)
            } else Left(ItemError.DecodingError(errors.mkString(","))) // TODO: Avi - Make ItemError a composite
          }

      }
    } else if (reflect.isSequence) {
      val errors        = new ArrayBuffer[String]
      val sequence      = reflect.asSequenceUnknown.get.sequence
      val seqBinding    =
        try sequence.seqBinding.asInstanceOf[Binding.Seq[Col, A]]
        catch {
          case _: Exception =>
            sequence.seqBinding.asInstanceOf[BindingInstance[DdbCodec, ?, A]].binding.asInstanceOf[Binding.Seq[Col, A]]
        }
      val constructor   = seqBinding.constructor
      val deconstructor = seqBinding.deconstructor
      val element       = sequence.element
      val elementCodec  = deriveCodec(element, cache)
      val encoder2      = elementCodec.encoder.asInstanceOf[A => AttributeValue]
      val decoder2      = elementCodec.decoder //.asInstanceOf[Any => A]

      val isSet                      = reflect.typeName.name.endsWith("Set")
      val sequenceCodec: DdbCodec[A] =
        new DdbCodec[A] {
          override def encoder: Encoder[A] =
            (a: A) => {
              val res = new ArrayBuffer[AttributeValue]
              val it  = deconstructor.deconstruct(a.asInstanceOf[Col[A]])
              while (it.hasNext) res.addOne(encoder2(it.next()))
              AttributeValue.List(res.toList)
            }

          override def decoder: Decoder[A] =
            (av: AttributeValue) =>
              av match {
                case AttributeValue.List(items) =>
                  val builder = constructor.newObjectBuilder[Elem](8)

                  // TODO: Avi - error handling
                  items.foreach { item =>
                    decoder2(item) match {
                      case Right(a)  => constructor.addObject(builder, a.asInstanceOf[Elem])
                      case Left(err) => errors.addOne(err.message)
                    }
                  }
                  if (errors.isEmpty) {
                    val xs: Col[Elem] = constructor.resultObject[Elem](builder)
                    Right(xs.asInstanceOf[A])
                  } else
                    Left(ItemError.DecodingError(errors.mkString(","))) // TODO: Avi - Make ItemError a composite
                case _                          => Left(ItemError.DecodingError(s"Expected AttributeValue.List, found ${av.showType}"))
              }
        }

      if (isSet) {
        val c = chooseNativeSetCodecOrNull(element)
        if (c ne null) c else sequenceCodec
      } else
        sequenceCodec

    }.asInstanceOf[DdbCodec[A]]
    else if (reflect.isMap) {
      val map           = reflect.asMapUnknown.get.map
      val mapBinding    =
        try map.mapBinding.asInstanceOf[Binding.Map[Map, Key, Value]]
        catch {
          case _: Exception =>
            map.mapBinding
              .asInstanceOf[BindingInstance[DdbCodec, ?, Value]]
              .binding
              .asInstanceOf[Binding.Map[Map, Key, Value]]
        }
      val constructor   = mapBinding.constructor
      val deconstructor = mapBinding.deconstructor
      val keyCodec      =
        deriveCodec(map.key, cache)
          .asInstanceOf[DdbCodec[Key]]
      val keyEncoder    = keyCodec.encoder   //.asInstanceOf[Key => AttributeValue.String]
      val keyDecoder    = keyCodec.decoder.asInstanceOf[Any => Either[ItemError.DecodingError, Key]]
      val valueCodec    = deriveCodec(map.value, cache).asInstanceOf[DdbCodec[Value]]
      val valueEncoder  = valueCodec.encoder //.asInstanceOf[Value => Any]
      val valueDecoder  = valueCodec.decoder //.asInstanceOf[Any => Value]

      val isNativeMap = map.key.asPrimitive.map(_.typeName.name == "String").getOrElse(false)

      if (isNativeMap)
        new DdbCodec[Map[Key, Value]] {
          override def encoder: Encoder[Map[Key, Value]] =
            (m: Map[Key, Value]) => {
              val mapBuilder = AttributeValue.Map.MapBuilder()
              val it         = deconstructor.deconstruct(m)
              while (it.hasNext) {
                val kv             = it.next()
                val key            = deconstructor.getKey(kv)
                val value          = deconstructor.getValue(kv)
                val keyVal: String = keyEncoder.asInstanceOf[Key => AttributeValue.String](key).value
                mapBuilder.add(keyVal, valueEncoder(value))
              }
              mapBuilder.build
            }

          override def decoder: Decoder[Map[Key, Value]] =
            (av: AttributeValue) => {
              if (!av.isInstanceOf[AttributeValue.Map])
                Left(ItemError.DecodingError(s"Expected AttributeValue.Map, found ${av.showType}"))
              else {
                val errors  = new ArrayBuffer[String]
                val map     = av.asInstanceOf[AttributeValue.Map]
                val builder = constructor.newObjectBuilder[Key, Value](8)
                val it      = map.value.iterator
                while (it.hasNext) {
                  val (k, v) = it.next()
                  (keyDecoder(k), valueDecoder(v)) match {
                    case (Right(key), Right(value)) =>
                      constructor.addObject(builder, key, value)
                    case (Left(errL), Left(errR))   =>
                      errors.addOne(errL.message)
                      errors.addOne(errR.message)
                    case (_, Left(err))             => errors.addOne(err.message)
                    case (Left(err), _)             => errors.addOne(err.message)
                  }
                }
                if (errors.isEmpty) {
                  val m = constructor.resultObject[Key, Value](builder)
                  Right(m)
                } else Left(ItemError.DecodingError(errors.mkString(","))) // TODO: Avi - Make ItemError a composite
              }
            }
        }
      else // non native Map encoding - Sequence of tuple2
        new DdbCodec[Map[Key, Value]] {
          override def encoder: Encoder[Map[Key, Value]] =
            (a: Map[Key, Value]) => {
              val avList = new ArrayBuffer[AttributeValue]
              val map    = deconstructor.deconstruct(a)
              while (map.hasNext) {
                val kv                           = map.next()
                val key: Key                     = deconstructor.getKey(kv)
                val value: Value                 = deconstructor.getValue(kv)
                val keyAv: AttributeValue        = keyEncoder(key)
                val valueAv: AttributeValue      = valueEncoder(value)
                val tupleAv: AttributeValue.List = AttributeValue.List(Iterable(keyAv, valueAv))
                avList.addOne(tupleAv)
              }
              AttributeValue.List(avList.toList)
            }

          override def decoder: Decoder[Map[Key, Value]] = {
            case AttributeValue.List(value) =>
              val it      = value.iterator
              val errors  = new ArrayBuffer[String]
              val builder = constructor.newObjectBuilder[Key, Value](8)

              while (it.hasNext) {
                val next = it.next()
                next match {
                  case AttributeValue.List(kvItems) if kvItems.size == 2 =>
                    val it      = kvItems.iterator
                    val keyAv   = it.next()
                    val valueAv = it.next()
                    // TODO: Avi - extract to a local method and call twice for native and non-native
                    (keyDecoder(keyAv), valueDecoder(valueAv)) match {
                      case (Right(key), Right(value)) =>
                        constructor.addObject(builder, key, value)
                      case (Left(errL), Left(errR))   =>
                        errors.addOne(errL.message)
                        errors.addOne(errR.message)
                      case (_, Left(err))             => errors.addOne(err.message)
                      case (Left(err), _)             => errors.addOne(err.message)
                    }
                  case other                                             =>
                    errors.addOne(
                      s"Expected AttributeValue.List of size 2 for Map entry, found: ${other.showType}"
                    )

                }
              }

              if (errors.isEmpty) {
                val m = constructor.resultObject[Key, Value](builder)
                Right(m)
              } else Left(ItemError.DecodingError(errors.mkString(","))) // TODO: Avi - Make ItemError a composite

            case av                         => Left(ItemError.DecodingError(s"Expected AttributeValue.List, found ${av.showType}"))
          }

        }
    }.asInstanceOf[DdbCodec[A]]
    else
      ??? // TODO: Avi - Wrapper, Dynamic, Tuple as nested Lists implementation inside of Map codec
  }

}
