package zio.dynamodb.blocks

import zio.blocks.schema.Reflect.Bound
import zio.blocks.schema._
import zio.blocks.schema.binding._
import zio.blocks.schema.derive.{ BindingInstance, Deriver }
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.{ AttributeValue, Decoder, Encoder, FromAttributeValue }

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait DdbCodec[A] {

  def encoder: Encoder[A]
  def decoder: Decoder[A]
}

object BlocksDdbDerived extends Deriver[DdbCodec] { self =>
  sealed trait VariantMetaData
  object VariantMetaData {
    case object Option extends VariantMetaData
    case object Either extends VariantMetaData

    // Discriminator is added as a top level Map wrapping the Record using the type name as the key
    case object DefaultTaggedDiscriminationPolicy extends VariantMetaData

    // Discriminator is added at the Record level as an extra field
    final case class FieldDiscriminationPolicy(name: String) extends VariantMetaData

    // No discriminator is encoded - so decoding has to try each case until one works - for legacy DBs
    case object NoDiscriminator extends VariantMetaData
  }

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
    binding: Binding[BindingType.Wrapper[A, B], A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[A]] =
    Lazy(
      new DdbCodec[A] {
        val wrapper                      = Reflect.Wrapper(
          wrapped = wrapped.asInstanceOf[Reflect[Any, B]],
          typeName = typeName,
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
  type Map2[_, _]

  final class CacheEntry private (
    val fieldCodecs: Array[DdbCodec[?]],
    names: Array[String]
  )                 {
    def size: Int                 = fieldCodecs.length // TODO: Avi - for debugging - remove
    override def toString: String = s"CacheEntry(${fieldCodecs.toSeq}, ${names.toSeq})"

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

    def addEntry(codec: DdbCodec[?], name: String, index: Int): Unit = {
      fieldCodecs(index) = codec
      if (hasNames)
        names(index) = name
    }

    def byIndex(i: Int): DdbCodec[?] = fieldCodecs(i)

    def byName(name: String): Option[DdbCodec[?]] =
      if (!hasNames) None
      else nameToIndex.get(name).map(fieldCodecs)
  }
  object CacheEntry {
    def makeWithNames(size: Int)       =
      new CacheEntry(new Array[DdbCodec[?]](size), new Array[String](size))
    def makeWithoutNames[A](size: Int) =
      new CacheEntry(new Array[DdbCodec[?]](size), Array.empty)
  }

  def enumCodec[A](typeName: TypeName[A]): DdbCodec[A] =
    new DdbCodec[A] {
      override def encoder: Encoder[A] = (_: A) => AttributeValue.String(typeName.name)

      override def decoder: Decoder[A] =
        // TODO: Avi - get CacheEntry for enum parent name
        ???
    }

  private val intCodec: DdbCodec[Int]       = new DdbCodec[Int] {
    override def encoder: Encoder[Int] =
      (a: Int) => AttributeValue.Number(BigDecimal(a.toString))

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
      (a: Long) => AttributeValue.Number(BigDecimal(a.toString))

    override def decoder: Decoder[Long] =
      (av: AttributeValue) =>
        FromAttributeValue.longFromAttributeValue
          .fromAttributeValue(av)
          .asInstanceOf[Either[zio.dynamodb.DynamoDBError.ItemError, Long]]
  }

  private def deriveCodec[A](
    reflect: Bound[A],
    cache: mutable.HashMap[TypeName[?], CacheEntry] = new mutable.HashMap,
    maybeVariantMetaData: Option[VariantMetaData] = None
  ): DdbCodec[A] = {
    if (reflect.isPrimitive) {
      val primitiveType = reflect.asPrimitive.get.primitiveType
      primitiveType match {
        // TODO: Avi - extract these to vals & handle other primitive types
        case _: PrimitiveType.String => stringCodec
        case _: PrimitiveType.Int    => intCodec
        case _: PrimitiveType.Long   => longCodec
        case _                       => ??? // TODO: Avi - other types
      }
    } else if (reflect.isRecord) {
      val record = reflect.asRecord.get

      val recordPackages = record.typeName.namespace.packages
      val (recordPackageIsScala, recordPackageIsScalaUtil) = {
        val len = recordPackages.length
        if (len == 0) (false, false)
        else if (recordPackages(0) ne "scala") (false, false)
        else if (len == 2 && (recordPackages(1) eq "util")) (true, true)
        else (true, false)
      }

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
        case Some(x) => x
        case _       =>
          val codecs: CacheEntry = CacheEntry.makeWithNames(fields.length)
          if (!fields.isEmpty) {
            cache.put(record.typeName, codecs)
            val len = fields.length
            var idx = 0
            while (idx < len) {
              val reflect = fields(idx).value
              codecs.addEntry(deriveCodec(reflect, cache, maybeVariantMetaData), fields(idx).name, idx)
              idx += 1
            }
          }
          codecs
      }

      new DdbCodec[A] {
        override def encoder: Encoder[A] = {
          val encoder: Encoder[A] = (a: A) => {
            var avMapBuilder = AttributeValue.Map.MapBuilder()
            val registers    = Registers(record.usedRegisters)
            deconstructor.deconstruct(registers, RegisterOffset.Zero, a)
            var offset       = RegisterOffset.Zero
            var idx          = -1

            val av: AttributeValue =
              if (fields.isEmpty) // TODO: Avi - do we need more info to validate this is an enum?
                // TODO: Avi - investigate doing "None" case object enum here as well
                // for simple enums no need to recurse any further as we can decode directly
                AttributeValue.String(record.typeName.name)
              else {
                fields.foreach { field =>
                  idx += 1
                  val encoder   = fieldCodecs.byIndex(idx).encoder
                  val fieldName = field.name
                  val reflect   = field.value
                  if (reflect.isPrimitive) {
                    val primitiveType = reflect.asPrimitive.get.primitiveType
                    primitiveType match {
                      case _: PrimitiveType.Int  =>
                        val av: AttributeValue =
                          encoder.asInstanceOf[Int => AttributeValue](registers.getInt(offset, 0))
                        avMapBuilder = avMapBuilder.add(fieldName, av)
                        offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
                      case _: PrimitiveType.Long =>
                        val av: AttributeValue =
                          encoder.asInstanceOf[Long => AttributeValue](registers.getLong(offset, 0))
                        avMapBuilder = avMapBuilder.add(fieldName, av)
                        offset = RegisterOffset.add(offset, RegisterOffset(longs = 1))
                      case _                     =>
                        val av = encoder.asInstanceOf[AnyRef => AttributeValue](registers.getObject(offset, 0))
                        avMapBuilder = avMapBuilder.add(fieldName, av)
                        offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                    }
                  } else {
                    val av = encoder.asInstanceOf[AnyRef => AttributeValue](registers.getObject(offset, 0))
                    field.value match {
                      case v: Reflect.Variant.Bound[_]
                          if isOption(v) && (av == AttributeValue.String("None") || av == AttributeValue.Null) =>
                        () // skip adding Null Optional fields to the map
                      case _ =>
                        avMapBuilder = avMapBuilder.add(fieldName, av)
                    }
                    offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                  }
                }

                //
                // enrich AttributeValue.Map for sum types
                //

                maybeVariantMetaData match {
                  case None         => avMapBuilder.build
                  case Some(policy) =>
                    policy match {
                      case VariantMetaData.Option | VariantMetaData.Either
                          if fields.length == 1 && (recordPackageIsScala | recordPackageIsScalaUtil) && avMapBuilder.size == 1 =>
                        val it             = avMapBuilder.iterator
                        val (kAttr, vAttr) = it.next()
                        val keyName        = kAttr.value
                        val typeName       = record.typeName.name

                        if (typeName eq "Some")
                          if (keyName eq "value") vAttr // Some is encoded without a Map
                          else avMapBuilder.build
                        else if (typeName eq "Right")
                          if (keyName eq "value") AttributeValue.Map("Right", vAttr)
                          else avMapBuilder.build
                        else if (typeName eq "Left")
                          if (keyName eq "value") AttributeValue.Map("Left", vAttr)
                          else avMapBuilder.build
                        else avMapBuilder.build
                      case VariantMetaData.FieldDiscriminationPolicy(discriminatorFieldName) =>
                        avMapBuilder.add(discriminatorFieldName, AttributeValue.String(record.typeName.name)).build
                      case VariantMetaData.NoDiscriminator                                   =>
                        // TODO: Avi - this is not relevant to record level processing
                        avMapBuilder.build
                      case VariantMetaData.DefaultTaggedDiscriminationPolicy                 =>
                        // default behavior: add discriminator field
                        AttributeValue.Map(record.typeName.name, avMapBuilder.build)
                      case _                                                                 => avMapBuilder.build
                    }
                }
              }
            av
          }
          encoder
        }

        override def decoder: Decoder[A] =
          (av: AttributeValue) => {
            if (fields.isEmpty)
              // for simple enums no need to recurse any further as we can construct directly
              av match {
                case AttributeValue.String(name) if name == record.typeName.name =>
                  val registers = Registers(record.usedRegisters)
                  // looks like we can use constructor for zero fields record to construct a simple enum - nice!!!!
                  val a         = constructor.construct(registers, RegisterOffset.Zero)
                  Right(a)
                case _                                                           =>
                  Left(
                    ItemError.DecodingError(
                      s"Expected enum value ${record.typeName.name}, found AttributeValue: $av"
                    )
                  )
              }
            else { // fields not empty
              // TODO: Avi - determine if we are in context variant - (may need to pass into deriveCodec ???)
              val errors: ArrayBuffer[String] = new ArrayBuffer
              val registers                   = Registers(record.usedRegisters)
              var offset                      = RegisterOffset.Zero
              var idx                         = -1

              def decodeAndSetRegisters(av: AttributeValue): Unit =
                fields.foreach { field =>
                  idx += 1
                  val decoder = fieldCodecs.byIndex(idx).decoder
                  val reflect = field.value

                  // TODO: Avi - see if we can optimise variant based processing
                  val isOpt =
                    if (field.value.isVariant)
                      isOption(field.value.asVariant.get)
                    else false

                  val name =
                    if (fields.length == 1 && recordPackageIsScalaUtil)
                      // both scala.util.Right and scala.util.Left are single field records with field named "value"
                      // however we encode them with a field named "Right" or "Left"
                      record.typeName.name match {
                        case "Right" => "Right"
                        case "Left"  => "Left"
                        case _       => throw new Exception("BOOOOOOOOOm! Should not happen") // TODO: Avi
                      }
                    else field.name

                  def getField(av: AttributeValue.Map, fieldName: String): Either[ItemError, AttributeValue] =
                    av.get(fieldName)
                      .toRight(ItemError.DecodingError(s"Field name: '$fieldName' not found in record ${av.showType}"))

                  getField(
                    av.asInstanceOf[AttributeValue.Map],
                    name
                  ) match {
                    case Right(avValue) =>
                      if (reflect.isPrimitive) {
                        val primitiveType = reflect.asPrimitive.get.primitiveType
                        primitiveType match {
                          case _: PrimitiveType.Int  =>
                            decoder.asInstanceOf[AnyRef => Either[ItemError, Int]](avValue) match {
                              case Left(err)  => errors.addOne(err.message)
                              case Right(int) =>
                                registers.setInt(offset, 0, int)
                                offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
                            }
                          case _: PrimitiveType.Long =>
                            decoder.asInstanceOf[AnyRef => Either[ItemError, Long]](avValue) match {
                              case Left(err)  => errors.addOne(err.message)
                              case Right(lng) =>
                                registers.setLong(offset, 0, lng)
                                offset = RegisterOffset.add(offset, RegisterOffset(longs = 1))
                            }
                          case _                     => // TODO: Avi - other primitive types
                            decoder.asInstanceOf[AnyRef => Either[ItemError, AnyRef]](avValue) match {
                              case Left(err)     => errors.addOne(err.message)
                              case Right(anyRef) =>
                                registers.setObject(offset, 0, anyRef)
                                offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                            }
                        }
                      } else if (av == AttributeValue.Null && isOpt) { // we maybe reading a legacy DB
                        registers.setObject(offset, 0, None)
                        offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                      } else
                        decoder.asInstanceOf[AnyRef => Either[ItemError, AnyRef]](avValue) match {
                          case Left(err)     => errors.addOne(err.message)
                          case Right(anyRef) =>
                            registers.setObject(offset, 0, anyRef)
                            offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                        }
                    case Left(error)    => // TODO: Avi - delay error creation to save a memory allocation
                      if (isOpt) {
                        registers.setObject(offset, 0, None) // Option of None is represented by missing field
                        offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                      } else
                        errors.addOne(error.message)
                  }
                } // end decodeAndSetRegisters

              if (!av.isInstanceOf[AttributeValue.Map])                  // TODO: Avi - do a better condition
                // align shape of AV with Schema for Some
                decodeAndSetRegisters(AttributeValue.Map("value", av))
              else if (av.isInstanceOf[AttributeValue.Map])
                decodeAndSetRegisters(av)
              else
                errors.addOne(s"Expected AttributeValue.Map, found ${av.showType}")
              if (errors.isEmpty) {
                val a = constructor.construct(registers, RegisterOffset.Zero)
                Right(a)
              } else Left(ItemError.DecodingError(errors.mkString(","))) // TODO: Avi - Make ItemError a composite
            }
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
      val elementCodec  = deriveCodec(element, cache, maybeVariantMetaData)
      val encoder2      = elementCodec.encoder.asInstanceOf[A => AttributeValue]
      val decoder2      = elementCodec.decoder //.asInstanceOf[Any => A]

      val isSet          = reflect.typeName.name.endsWith("Set")
      val maybeNativeSet = NativeSet.fromTypeName(reflect.typeName, element.typeName)
      println(s"XXXXXXXXXX maybeNativeSet: $maybeNativeSet  ${element.isPrimitive}")

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

      /*
isPrimitive true => candidate for NativeSet
isPrimitive false => what about BS - is it still a primitive ????

we need:
- Set codecs
  - native set codec using AttributeValue.SS/BS/NS
  - non native set codec whereby enc is AttributeValue.List ie Sequence codec ?
- Sequence codec

if (isSet) {
    if (isPrimitive) {
      element.asPrimitive.get.primitiveType match {
        case _: PrimitiveType.Int  =>
          new DdbEncode[A] = ...
      }
        nativeSetCodec
    } else {
        sequenceCodec
    }
} else {
  sequenceCodec
}

       */

      if (isSet)
        if (element.isPrimitive)
          element.asPrimitive.get.primitiveType match {
            case _: PrimitiveType.String =>
              new DdbCodec[A] {
                override def encoder: Encoder[A] =
                  (a: A) => {
                    val ss = a.asInstanceOf[Set[String]]
                    AttributeValue.StringSet(ss)
                  }

                override def decoder: Decoder[A] = {
                  case AttributeValue.StringSet(value) => Right(value.asInstanceOf[A])
                  case av                              => Left(ItemError.DecodingError(s"Expected AttributeValue.StringSet, found ${av.showType}"))
                }
              }
            case _: PrimitiveType.Int    =>
              new DdbCodec[A] {
                override def encoder: Encoder[A] =
                  (a: A) => {
                    val ns = a.asInstanceOf[Set[Int]]
                    AttributeValue.NumberSet(ns.map(i => BigDecimal(i.toString)))
                  }

                override def decoder: Decoder[A] = {
                  case AttributeValue.NumberSet(value) => Right(value.map(n => n.toInt).asInstanceOf[A])
                  case av                              => Left(ItemError.DecodingError(s"Expected AttributeValue.StringSet, found ${av.showType}"))
                }
              }
            case _                       =>
              sequenceCodec
          }
        else // not a primitive
          sequenceCodec
      else   // not a Set
        sequenceCodec

    } else if (reflect.isMap) {
      // TODO: Avi - Map as Tuple handling - Blocks encodes Tuples as Maps
      val map           = reflect.asMapUnknown.get.map
      val mapBinding    =
        try map.mapBinding.asInstanceOf[Binding.Map[Map2, Key, Value]]
        catch {
          case _: Exception =>
            map.mapBinding
              .asInstanceOf[BindingInstance[DdbCodec, ?, Value]]
              .binding
              .asInstanceOf[Binding.Map[Map2, Key, Value]]
        }
      val constructor   = mapBinding.constructor
      val deconstructor = mapBinding.deconstructor
      val keyCodec      =
        deriveCodec(map.key, cache, maybeVariantMetaData)
          .asInstanceOf[DdbCodec[Key]]
      val keyEncoder    = keyCodec.encoder   //.asInstanceOf[Key => AttributeValue.String]
      val keyDecoder    = keyCodec.decoder.asInstanceOf[Any => Either[ItemError.DecodingError, Key]]
      val valueCodec    = deriveCodec(map.value, cache, maybeVariantMetaData).asInstanceOf[DdbCodec[Value]]
      val valueEncoder  = valueCodec.encoder //.asInstanceOf[Value => Any]
      val valueDecoder  = valueCodec.decoder //.asInstanceOf[Any => Value]

      val isNativeMap = map.key.asPrimitive.map(_.typeName.name == "String").getOrElse(false)

      if (isNativeMap)
        new DdbCodec[Map2[Key, Value]] {
          override def encoder: Encoder[Map2[Key, Value]] =
            (m: Map2[Key, Value]) => {
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

          override def decoder: Decoder[Map2[Key, Value]] =
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
        new DdbCodec[Map2[Key, Value]] {
          override def encoder: Encoder[Map2[Key, Value]] =
            (a: Map2[Key, Value]) => {
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

          override def decoder: Decoder[Map2[Key, Value]] = {
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
    else if (reflect.isVariant) {
      val variant: Reflect.Variant[Binding, A] = reflect.asVariant.get
      val variantBinding                       =
        try variant.variantBinding.asInstanceOf[Binding.Variant[A]]
        catch {
          case _: Exception =>
            variant.variantBinding
              .asInstanceOf[BindingInstance[DdbCodec, ?, Value]]
              .binding
              .asInstanceOf[Binding.Variant[A]]
        }

      val cases                  = variant.cases
      val discriminator          = variantBinding.discriminator
      val variantMetaData2       = variantMetaData(variant, reflect.modifiers)
      val caseCodecs: CacheEntry = cache.get(variant.typeName) match {
        case Some(x) => x
        case _       =>
          val codecs = CacheEntry.makeWithNames(cases.length)
          cache.put(variant.typeName, codecs)
          val len    = cases.length
          var idx    = 0

          while (idx < len) {
            val reflect = cases(idx).value
            codecs.addEntry(
              deriveCodec(reflect, cache, Some(variantMetaData2)),
              cases(idx).name,
              idx
            )
            idx += 1
          }
          codecs
      }

      new DdbCodec[A] {
        override def encoder: Encoder[A] = { (a: A) =>
          val idx     = discriminator.discriminate(a)
          val encoder = caseCodecs.byIndex(idx).encoder.asInstanceOf[A => AttributeValue]
          encoder(a)
        }

        override def decoder: Decoder[A] = { (av: AttributeValue) =>
          if (isOption(variant))
            //someDecoder(variant)(av)
            caseCodecs.byName("Some") match {
              case Some(codec) =>
                codec.decoder.asInstanceOf[Decoder[A]](av)
              case None        =>
                Left(DecodingError(s"Unknown case in Variant decoder for AttributeValue: $av"))
            }
          else
            av match {
              // TODO: Avi - validate against Schema that this is a simple enum variant
              case AttributeValue.String(name) =>
                caseCodecs.byName(name) match {
                  case Some(codec) =>
                    codec.decoder.asInstanceOf[Decoder[A]](av)
                  case None        =>
                    Left(DecodingError(s"Unknown case in Variant decoder for AttributeValue: $av"))
                }
              case m: AttributeValue.Map       =>
                variantMetaData2 match {
                  case VariantMetaData.Either                              =>
                    if (m.size != 1)
                      Left(
                        DecodingError(s"Expected single entry Map for a tagged variant, found size ${m.size}")
                      )
                    else {
                      val it        = m.value.iterator
                      val (key, av) = it.next() // kv: (String, AttributeValue)

                      def decodeForLabel(label: String): Either[ItemError, A] =
                        caseCodecs.byName(label) match {
                          case Some(codec) =>
                            codec.decoder.asInstanceOf[Decoder[A]](m)
                          case None        =>
                            Left(
                              DecodingError(
                                s"Unknown case in Either Variant decoder for AttributeValue: ${av.showType}"
                              )
                            )
                        }

                      val v = key.value
                      if (v eq "Right") decodeForLabel("Right")
                      else if (v eq "Left") decodeForLabel("Left")
                      else
                        Left(DecodingError(s"Unknown key in Either Variant decoder: $key")) // this should never happen
                    }
                  case VariantMetaData.DefaultTaggedDiscriminationPolicy   =>
                    if (m.size != 1)
                      Left(
                        DecodingError(s"Expected single entry Map for a tagged variant, found size ${m.size}")
                      )
                    else {
                      val it        = m.value.iterator
                      val (key, av) = it.next()

                      caseCodecs.byName(key.value) match {
                        case Some(codec) =>
                          codec.decoder.asInstanceOf[Decoder[A]](av)
                        case None        =>
                          Left(DecodingError(s"Unknown case in Variant decoder for AttributeValue: $av"))
                      }
                    }
                  case VariantMetaData.FieldDiscriminationPolicy(discName) =>
                    m.get(discName) match {
                      case Some(AttributeValue.String(typeName)) =>
                        caseCodecs.byName(typeName) match {
                          case Some(codec) =>
                            codec.decoder.asInstanceOf[Decoder[A]](av)
                          case None        =>
                            Left(DecodingError(s"Unknown case in Variant decoder for AttributeValue: $av"))
                        }
                      case Some(otherAV)                         =>
                        Left(
                          DecodingError(
                            s"Expected discriminator field '$discName' to be String, found: ${otherAV.showType}"
                          )
                        )
                      case None                                  =>
                        Left(DecodingError(s"Discriminator field '$discName' not found in AttributeValue: $av"))
                    }
                  case _                                                   =>
                    Left(DecodingError(s"TODO: decode non enums and Either av: $av"))
                }
              case _                           => Left(DecodingError(s"TODO: expected a Map, found ${av.showType}"))
            }
        }
      }
    } else
      ??? // TODO: Avi - Set - Native Sets SS, BS, NS and Non Native Set, Wrapper, Dynamic
    // TODO: Avi - Tuple implementation inside of Map codec
  }

  // TODO: Avi - delete as we have VariantMetaData now
  private def isOption[A](v: Reflect.Variant.Bound[A]): Boolean = {
    val tn = v.typeName
    val ns = tn.namespace.packages
    (tn.name eq "Option") || (tn.name eq "Some") || (tn.name eq "None") match {
      case true if ns.size == 1 && (ns.head eq "scala") => true
      case _                                            => false
    }
  }

  def maybeDiscriminatorNameModifier(
    modifiers: Seq[Modifier]
  ): Option[String] =
    modifiers.collectFirst {
      case Modifier.config("discriminatorName", value) => value
    }

  def variantMetaData[A](
    variant: Reflect.Variant.Bound[A],
    modifiers: Seq[Modifier]
  ): VariantMetaData = {
    val tn = variant.typeName
    val ns = tn.namespace.packages

    val isOption =
      ((tn.name eq "Option") || (tn.name eq "Some") || (tn.name eq "None")) &&
        ns.lengthCompare(1) == 0 &&
        (ns.head eq "scala")

    val isEither =
      (tn.name eq "Either") &&
        ns.lengthCompare(2) == 0 &&
        (ns.head eq "scala") &&
        (ns(1) eq "util")

    if (isOption)
      VariantMetaData.Option
    else if (isEither)
      VariantMetaData.Either
    else
      maybeDiscriminatorNameModifier(modifiers) match {
        case Some(name) => VariantMetaData.FieldDiscriminationPolicy(name)
        case None       => VariantMetaData.DefaultTaggedDiscriminationPolicy
      }
  }

  sealed trait NativeSet
  object NativeSet {
    def fromTypeName(setTypeName: TypeName[?], elementTypeName: TypeName[?]): Option[NativeSet] = {
      val setName     = setTypeName.name
      val elementName = elementTypeName.name
      if (setName eq "Set")
        if (elementName eq "String") Some(StringSet)
        else if (elementName eq "Int") Some(NumberSet)
        else if (elementName eq "Binary") Some(BinarySet) // TODO: Avi - BinarySet handling
        else None
      else None
    }
    case object StringSet extends NativeSet
    case object NumberSet extends NativeSet
    case object BinarySet extends NativeSet

  }
}
