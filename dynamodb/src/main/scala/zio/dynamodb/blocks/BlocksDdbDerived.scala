package zio.dynamodb.blocks

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

  override def derivePrimitive[F[_, _], A](
    primitiveType: PrimitiveType[A],
    typeName: TypeName[A],
    binding: Binding[BindingType.Primitive, A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  ): Lazy[DdbCodec[A]] =
    Lazy(
      deriveCodec(
        new Schema(
          Reflect.Primitive(
            primitiveType = primitiveType,
            typeName = typeName,
            primitiveBinding = binding,
            doc = doc,
            modifiers = modifiers
          )
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
        new Schema(
          Reflect.Record(
            fields = fields.asInstanceOf[IndexedSeq[Term[Binding, A, _]]],
            typeName = typeName,
            recordBinding = binding,
            doc = doc,
            modifiers = modifiers
          )
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
        new Schema(
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
        new Schema(
          Reflect.Sequence(
            element = element.asInstanceOf[Reflect[Binding, A]],
            typeName = typeName,
            seqBinding = binding,
            doc = doc,
            modifiers = modifiers
          )
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
        new Schema(
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

  private def deriveCodec[A](
    schema: Schema[A],
    cache: mutable.HashMap[TypeName[?], CacheEntry] = new mutable.HashMap
  ): DdbCodec[A] = {
    val reflect = schema.reflect
    if (reflect.isPrimitive) {
      val primitiveType = reflect.asPrimitive.get.primitiveType
      primitiveType match {
        case _: PrimitiveType.String =>
          new DdbCodec[A] {
            override def encoder: Encoder[A] =
              (a: A) => AttributeValue.String(a.toString)

            override def decoder: Decoder[A] =
              (av: AttributeValue) => FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av)
          }
        case _: PrimitiveType.Int    =>
          new DdbCodec[A] {
            override def encoder: Encoder[A] =
              (a: A) => AttributeValue.Number(BigDecimal(a.toString))

            override def decoder: Decoder[A] =
              (av: AttributeValue) =>
                FromAttributeValue.intFromAttributeValue
                  .fromAttributeValue(av)
                  .asInstanceOf[Either[zio.dynamodb.DynamoDBError.ItemError, A]]
          }
        case _                       => ??? // TODO: Avi - other types
      }
    } else if (reflect.isRecord) {
      val record         = reflect.asRecord.get
      lazy val recordPkg = record.typeName.namespace.packages.mkString(".")
      val recordBinding  =
        try record.recordBinding.asInstanceOf[Binding.Record[A]]
        catch {
          case _: Exception =>
            record.recordBinding
              .asInstanceOf[BindingInstance[DdbCodec, ?, A]]
              .binding
              .asInstanceOf[Binding.Record[A]]
        }
      val constructor    = recordBinding.constructor
      val deconstructor  = recordBinding.deconstructor
      val fields         = record.fields

      // TODO: Avi - we end up with empty CacheEntry memory alloc for simple enum that is not used
      val fieldCodecs = cache.get(record.typeName) match {
        case Some(x) => x
        case _       =>
          val codecs: CacheEntry = CacheEntry.makeWithNames(fields.length)
          if (!fields.isEmpty) {
            cache.put(record.typeName, codecs) // TODO: Avi - we could add isOption, isEither fields to the cache???
            val len = fields.length
            var idx = 0
            while (idx < len) {
              val reflect = fields(idx).value
              codecs.addEntry(deriveCodec(new Schema(reflect), cache), fields(idx).name, idx)
              idx += 1
            }
          }
          codecs
      }

      new DdbCodec[A] {
        override def encoder: Encoder[A] = {
          val encoder: Encoder[A] = (a: A) => {
            // TODO: Avi - determine if we are in context variant - (may need to pass into deriveCodec ???)
            // discriminator: add discriminator field
            // default: add discriminator field Map -> record
            var avMap     = AttributeValue.Map.empty // TODO: Avi - create a mutable builder API for AV Map
            val registers = Registers(record.usedRegisters)
            deconstructor.deconstruct(registers, RegisterOffset.Zero, a)
            var offset    = RegisterOffset.Zero
            var idx       = -1

            val av =
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
                      case _: PrimitiveType.Int =>
                        val av: AttributeValue =
                          encoder.asInstanceOf[Int => AttributeValue](registers.getInt(offset, 0))
                        avMap = avMap + (fieldName -> av)
                        offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
                      case _                    =>
                        val av = encoder.asInstanceOf[AnyRef => AttributeValue](registers.getObject(offset, 0))
                        avMap = avMap + (fieldName -> av)
                        offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                    }
                  } else {
                    val av = encoder.asInstanceOf[AnyRef => AttributeValue](registers.getObject(offset, 0))
                    field.value match {
                      case v: Reflect.Variant.Bound[_]
                          if isOption(v) && (av == AttributeValue.String("None") || av == AttributeValue.Null) =>
                        () // skip adding Null Optional fields to the map
                      case _ =>
                        avMap = avMap + (fieldName -> av)
                    }
                    offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                  }
                }
                if (fields.length == 1 && (recordPkg == "scala" || recordPkg == "scala.util") && avMap.size == 1) {
                  val it             = avMap.value.iterator
                  val (kAttr, vAttr) = it.next()
                  val keyName        = kAttr.value
                  val typeName       = record.typeName.name

                  if (typeName eq "Some")
                    if (keyName eq "value") vAttr // Some is encoded without a Map
                    else avMap
                  else if (typeName eq "Right")
                    if (keyName eq "value") AttributeValue.Map("Right", vAttr)
                    else avMap
                  else if (typeName eq "Left")
                    if (keyName eq "value") AttributeValue.Map("Left", vAttr)
                    else avMap
                  else avMap
                } else
                  avMap
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
            else {
              // TODO: Avi - determine if we are in context variant - (may need to pass into deriveCodec ???)
              val errors: ArrayBuffer[String] = new ArrayBuffer
              val registers                   = Registers(record.usedRegisters)
              var offset                      = RegisterOffset.Zero
              var idx                         = -1

              def foo(av: AttributeValue): Unit =
                // TODO: Avi - extract to a function -> unit and call with a manufactured AV Map
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
                    if (fields.length == 1 && record.typeName.namespace.packages.mkString(".") == "scala.util")
                      record.typeName.name match {
                        case "Right" => "Right"
                        case "Left"  => "Left"
                        case _       => throw new Exception("BOOOOOOOOOm! Should not happen") // TODO: Avi
                      }
                    else field.name
                  println(s"XXXXXXX foo name: $name")

                  def getField(av: AttributeValue.Map, fieldName: String): Either[ItemError, AttributeValue] =
                    av.get(fieldName)
                      .toRight(ItemError.DecodingError(s"Field name: '$fieldName' not found in record ${av.showType}"))

                  getField(
                    av.asInstanceOf[AttributeValue.Map],
                    name
                  ) match {
                    case Right(avValue) =>
                      if (reflect.isPrimitive) {
                        println(s"XXXX foo isPrimitive")
                        val primitiveType = reflect.asPrimitive.get.primitiveType
                        primitiveType match {
                          case _: PrimitiveType.Int =>
                            decoder.asInstanceOf[AnyRef => Either[ItemError, Int]](avValue) match {
                              case Left(err)  => errors.addOne(err.message)
                              case Right(int) =>
                                registers.setInt(offset, 0, int)
                                offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
                            }
                          case _                    => // TODO: Avi - other primitive types
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
                }

              if (!av.isInstanceOf[AttributeValue.Map]) { // TODO: Avi - do a better condition
                println(s"XXXXXXXXXXXX 4")
                // align shape of AV with Schema for Some
                foo(AttributeValue.Map("value", av))
              } else if (av.isInstanceOf[AttributeValue.Map])
                foo(av)
              else {
                println(s"XXXXXXXXXXXXXX 3 av: $av")
                errors.addOne(s"Expected AttributeValue.Map, found ${av.showType}")
              }
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
      val elementCodec  = deriveCodec(new Schema(element), cache)
      val encoder2      = elementCodec.encoder.asInstanceOf[A => AttributeValue]
      val decoder2      = elementCodec.decoder //.asInstanceOf[Any => A]
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

    } else if (reflect.isMap) { // TODO: Avi - assume native DDB Map with String keys only for now
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
      val keyCodec      = deriveCodec(new Schema(map.key), cache)
      val keyEncoder    = keyCodec.encoder.asInstanceOf[Key => AttributeValue.String]
      val keyDecoder    = keyCodec.decoder.asInstanceOf[Any => Either[ItemError.DecodingError, Key]]
      val valueCodec    = deriveCodec(new Schema(map.value), cache)
      val valueEncoder  = valueCodec.encoder.asInstanceOf[Value => Any]
      val valueDecoder  = valueCodec.decoder //.asInstanceOf[Any => Value]
      new DdbCodec[A] {
        override def encoder: Encoder[A] =
          (x: A) => {
            var map = AttributeValue.Map.empty
            val it  = deconstructor.deconstruct(x.asInstanceOf[Map2[Key, Value]])
            while (it.hasNext) {
              val kv             = it.next()
              val key            = deconstructor.getKey(kv)
              val value          = deconstructor.getValue(kv)
              val keyVal: String = keyEncoder(key).value
              map = map + (keyVal -> valueEncoder(value).asInstanceOf[AttributeValue])
            }
            map
          }

        override def decoder: Decoder[A] =
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
                    // TODO: Avi - why do we need this cast?
                    constructor.addObject(builder, key, value.asInstanceOf[Value])
                  case (Left(errL), Left(errR))   =>
                    errors.addOne(errL.message)
                    errors.addOne(errR.message)
                  case (_, Left(err))             => errors.addOne(err.message)
                  case (Left(err), _)             => errors.addOne(err.message)
                }
              }
              if (errors.isEmpty) {
                val m = constructor.resultObject[Key, Value](builder)
                Right(m.asInstanceOf[A])
              } else Left(ItemError.DecodingError(errors.mkString(","))) // TODO: Avi - Make ItemError a composite
            }
          }
      }
    } else if (reflect.isVariant) {
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
      val caseCodecs: CacheEntry = cache.get(variant.typeName) match {
        case Some(x) => x
        case _       =>
          val codecs = CacheEntry.makeWithNames(cases.length)
          cache.put(variant.typeName, codecs)
          val len    = cases.length
          var idx    = 0
          while (idx < len) {
            val reflect = cases(idx).value
            codecs.addEntry(deriveCodec(new Schema(reflect), cache), cases(idx).name, idx)
            idx += 1
          }
          codecs
      }

      def isEither =
        if (
          cases.length == 2 && variant.typeName.name == "Either" && variant.typeName.namespace.packages
            .mkString(".") == "scala.util"
        ) true
        else false

      new DdbCodec[A] {
        override def encoder: Encoder[A] = { (a: A) =>
//          if (isOption(variant))
//            optionEncoder(variant)(a)
//          else {
          val idx     = discriminator.discriminate(a)
          val encoder = caseCodecs.byIndex(idx).encoder.asInstanceOf[A => AttributeValue]
          encoder(a)
//          }
        }

        override def decoder: Decoder[A] = { (av: AttributeValue) =>
          if (isOption(variant)) {
            println(s"XXXXXXXXXXXXX 1.")
            //someDecoder(variant)(av)
            caseCodecs.byName("Some") match {
              case Some(codec) =>
                println(s"XXXXXXXXXXXXX 2.")
                codec.decoder.asInstanceOf[Decoder[A]](av)
              case None        =>
                Left(DecodingError(s"Unknown case in Variant decoder for AttributeValue: $av"))
            }
          } else
            av match {
              // TODO: Avi - validate against Schema that this is a simple enum variant
              case AttributeValue.String(name)                      =>
                caseCodecs.byName(name) match {
                  case Some(codec) =>
                    codec.decoder.asInstanceOf[Decoder[A]](av)
                  case None        =>
                    Left(DecodingError(s"Unknown case in Variant decoder for AttributeValue: $av"))
                }
              case m: AttributeValue.Map if isEither && m.size == 1 =>
                // examine the single key to determine Left vs Right
                val it  = m.value.iterator
                val kv  = it.next() // kv: (String, AttributeValue)
                val key = kv._1     // access tuple elements directly
                val av  = kv._2

                def decodeForLabel(label: String): Either[ItemError, A] =
                  caseCodecs.byName(label) match {
                    case Some(codec) =>
                      codec.decoder.asInstanceOf[Decoder[A]](m)
                    case None        =>
                      Left(DecodingError(s"Unknown case in Either Variant decoder for AttributeValue: ${av.showType}"))
                  }

                if (key.value == "Right")
                  decodeForLabel("Right")
                else if (key.value == "Left")
                  decodeForLabel("Left")
                else // this should never happen
                  Left(DecodingError(s"Unknown key in Either Variant decoder: $key"))

              case _: AttributeValue.Map                            =>
                Left(DecodingError(s"TODO: decode non enums and Either av: $av"))
              case _                                                => Left(DecodingError(s"TODO: expected a Map, found ${av.showType}"))
            }
        }
      }
    } else
      ??? // TODO: Avi - Variant, Non Native Map, Wrapper, Dynamic
  }

  def isOption[A](variant: Reflect.Variant.Bound[A]): Boolean =
    (variant.typeName.name == "Option" || variant.typeName.name == "None" || variant.typeName.name == "Some") && variant.typeName.namespace.packages
      .mkString(".") == "scala"

  def isEither[A](variant: Reflect.Variant.Bound[A]): Boolean =
    variant.typeName.name == "Either" && variant.typeName.namespace.packages.mkString(".") == "scala.util"

  def optionEncoder[A](v: Reflect.Variant.Bound[A]): Encoder[A] = {
    case Some(a) =>
      reflectBindingForCaseValueField("Some", v) match {
        case Some(value) =>
          val enc = deriveCodec(Schema(value)).encoder
          enc(a.asInstanceOf[value.Structure])
        case None        =>
          throw new Exception(s"Unexpected Schema shape for Some") // this should never happen
      }
    case None    => AttributeValue.Null                              // gets removed at the Record level
    case _       => throw new Exception(s"Input type not an Option") // TODO: tighten up types, this should never happen
  }

  // Note that None decoding (AttributeValue.Null or missing field value) is done upstream
  // so we only focus on the Some case here
  def someDecoder[A](v: Reflect.Variant.Bound[A]): Decoder[A] = { (av: AttributeValue) =>
    // we are dealing with the Some case of Option Variant
    // so we can short cut decoding of Option Variant to decoding of the value field of the Some case
    reflectBindingForCaseValueField("Some", v) match {
      case Some(value) =>
        deriveCodec(Schema(value)).decoder.apply(av).map(Some(_)).asInstanceOf[Either[DecodingError, A]]
      case None        => Left(DecodingError(s"Unexpected Schema shape for Some")) // this should never happen
    }
  }

  def eitherEncoder[A](v: Reflect.Variant.Bound[A]): Encoder[A] = { (a: A) =>
    def encodeCase[A](caseLabel: String, value: Any, v: Reflect.Variant.Bound[A]): AttributeValue =
      reflectBindingForCaseValueField(caseLabel, v) match {
        case Some(binding) =>
          val enc = deriveCodec(Schema(binding)).encoder
          AttributeValue.Map.empty + (caseLabel -> enc(value.asInstanceOf[binding.Structure]))
        case None          =>
          throw new Exception(s"Unexpected Schema shape for $caseLabel") // should never happen
      }

    a match {
      case Right(r) => encodeCase("Right", r, v)
      case Left(l)  => encodeCase("Left", l, v)
      case _        => throw new Exception(s"Input type not an Either") // TODO: tighten types
    }
  }

  def eitherDecoder[A](v: Reflect.Variant.Bound[A]): Decoder[A] = {
    def decodeEitherValue[A](label: String, v: Reflect.Variant.Bound[A]): Decoder[A] =
      // dig into the structure of the found case to get the decoder for the value field
      reflectBindingForCaseValueField(label, v) match {
        case Some(value) =>
          deriveCodec(Schema(value)).decoder
        case None        =>
          (_: AttributeValue) =>
            Left(
              DecodingError(s"Unexpected Schema shape for $label")
            ) // this should never happen
      }

    {
      case AttributeValue.Map(map) if map.size == 1 =>
        val iter  = map.iterator
        val entry = iter.next() // Map.Entry[_, _] under the hood, no extra tuple
        entry._1 match {
          case AttributeValue.String("Right") =>
            decodeEitherValue("Right", v)(entry._2).map(Right(_)).asInstanceOf[Either[DecodingError, A]]
          case AttributeValue.String("Left")  =>
            decodeEitherValue("Left", v)(entry._2).map(Left(_)).asInstanceOf[Either[DecodingError, A]]
          case other                          =>
            Left(DecodingError(s"Unexpected key in Either decoder: $other"))
        }

      case AttributeValue.Map(map)                  =>
        Left(DecodingError(s"Expected single-element map, got keys: ${map.keys}"))

      case av                                       =>
        Left(DecodingError(s"Expected AttributeValue.Map but found ${av.showType}"))
    }
  }

  /**
   * Note that Some, Left and Right use value classes with a single field named "value" which equates to a schema Record.
   * This function searches cases in the Variant for the `caseLabel` and returns the binding for that field as a Some,
   * else returns a None.
   */
  def reflectBindingForCaseValueField[A](
    caseLabel: String,
    v: Reflect.Variant.Bound[A]
  ): Option[Reflect.Bound[A]] = {
    // Find the case for the given label
    val maybeCase: Option[Term[Binding, A, _ <: A]] = v.cases.find(_.name == caseLabel)

    // dig into the structure of the found case to get the binding for the value field
    maybeCase match {
      case Some(recordForValue) =>
        recordForValue.value match {
          case Reflect.Record(fields, _, _, _, _) if fields.size == 1 && fields(0).name == "value" =>
            fields(0) match {
              case Term(_, value, _, _) =>
                Some(value.asInstanceOf[Reflect.Bound[A]])
            }
          case _                                                                                   => None
        }
      case None                 => None
    }
  }

}

/*
zio-dynamodb/runMain zio.dynamodb.blocks.TestDerived
 */
object TestDerived extends App {
  final case class PersonWithCollections(
    id: String,
    numbers: List[Int] = Nil,
    names: Array[String] = Array.empty,
    map: Map[String, Int] = Map.empty
  )
  object PersonWithCollections extends CompanionOptics[PersonWithCollections] {
    implicit val schema: Schema[PersonWithCollections] = Schema.derived
  }
  final case class PersonWithVariant(id: String, either: Either[String, Int])
  object PersonWithVariant     extends CompanionOptics[PersonWithOption]      {
    implicit val schema: Schema[PersonWithOption] = Schema.derived
  }

  final case class PersonWithOption(id: String, option: Option[Int])
  object PersonWithOption extends CompanionOptics[PersonWithOption] {
    implicit val schema: Schema[PersonWithOption] = Schema.derived
  }

  final case class Person(id: String, age: Int)
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived
  }

//  val codec: DdbCodec[PersonWithOption] = PersonWithOption.schema.derive(BlocksDdbDerived)
//  val enc                               = codec.encoder(PersonWithOption("1", Some(42)))
//  val codec: DdbCodec[Person]            = Person.schema.derive(BlocksDdbDerived)
//  val enc                                = codec.encoder(Person("1", 42))
  val codec: DdbCodec[PersonWithCollections] = PersonWithCollections.schema.derive(BlocksDdbDerived)
  val enc                                    = codec.encoder(PersonWithCollections("1", numbers = List(1, 2), map = Map("a" -> 1, "b" -> 2)))
//  val dec                                    = codec.decoder(enc)
//  val codec: DdbCodec[Person] = Person.schema.derive(BlocksDdbDerived)
//  val enc                     = codec.encoder(Person("1", 1))

  val dec = codec.decoder(enc)
  println(s"XXXXXXXX enc: $enc")
  println(s"XXXXXXXX dec: $dec")
//  println(s"XXXXXXXX dec: ${dec.map(_.names.toList)}")

//  val dec = codec.decoder(enc)
//  println(s"XXXXXXXX enc: $enc dec: $dec")
}
