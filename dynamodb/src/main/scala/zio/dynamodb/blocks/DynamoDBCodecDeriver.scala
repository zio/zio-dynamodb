package zio.dynamodb.blocks

import zio.Chunk
import zio.blocks.schema._
import zio.blocks.schema.binding.BindingType.{ Primitive, Variant, Wrapper }
import zio.blocks.schema.binding.RegisterOffset.RegisterOffset
import zio.blocks.schema.binding._
import zio.blocks.schema.derive.{ BindingInstance, Deriver }
import zio.dynamodb.AttributeValue.Map.JMapView
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.{ AttributeValue, Decoder, Encoder }

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/**
 * borrows heavily from Andriy Plokhotnyuk's zio-blocks JSON codec https://github.com/zio/zio-blocks
 */
object DynamoDBCodecDeriver
    extends DynamoDBCodecDeriver(
      // TODO: Avi - should we rename this to be tuple specific?
      zioSchema1Compatibility = true, // for Tuple representation compatibility
      discriminatorKind = DiscriminatorKind.Key,
      enumValuesAsStrings = true,
      fieldNameMapper = NameMapper.Identity,
      caseNameMapper = NameMapper.Identity,
      transientNone = true,
      requireOptionFields = false,
      transientEmptyCollection = false,
      requireCollectionFields = false
    ) {}

// TODO: Avi - create an issue in Blocks to either expose these or to provide higher level APIs to check for common Scala types
private[blocks] object Namespace {
  private[blocks] val javaTime: Namespace                 = new Namespace("java" :: "time" :: Nil)
  private[blocks] val javaUtil: Namespace                 = new Namespace("java" :: "util" :: Nil)
  private[blocks] val scala: Namespace                    = new Namespace("scala" :: Nil)
  private[blocks] val scalaCollectionImmutable: Namespace = new Namespace(
    "scala" :: "collection" :: "immutable" :: Nil
  )
  private[blocks] val zioBlocksSchema: Namespace          = new Namespace("zio" :: "blocks" :: "schema" :: Nil)
}

class DynamoDBCodecDeriver private (
  zioSchema1Compatibility: Boolean,
  discriminatorKind: DiscriminatorKind,
  enumValuesAsStrings: Boolean,
  fieldNameMapper: NameMapper,
  caseNameMapper: NameMapper,
  transientNone: Boolean,
  requireOptionFields: Boolean,
  transientEmptyCollection: Boolean,
  requireCollectionFields: Boolean // Schema1 codecs assumes this is false
) extends Deriver[DynamoDBCodec] { self =>
  // TODO: Avi - promote to config
  val requireDefaultValueFields: Boolean = false

  def withEnumValuesAsStrings(enumValuesAsStrings: Boolean): DynamoDBCodecDeriver           =
    copy(enumValuesAsStrings = enumValuesAsStrings)
  def withFieldNameMapper(fieldNameMapper: NameMapper): DynamoDBCodecDeriver                = copy(fieldNameMapper = fieldNameMapper)
  def withCaseNameMapper(caseNameMapper: NameMapper): DynamoDBCodecDeriver                  = copy(caseNameMapper = caseNameMapper)
  def withTransientNone(transientNone: Boolean): DynamoDBCodecDeriver                       = copy(transientNone = transientNone)
  def withDiscriminatorKind(discriminatorKind: DiscriminatorKind): DynamoDBCodecDeriver     =
    copy(discriminatorKind = discriminatorKind)
  def withRequiredCollectionFields(requireCollectionFields: Boolean): DynamoDBCodecDeriver  =
    copy(requireCollectionFields = requireCollectionFields)
  def withTransientEmptyCollection(transientEmptyCollection: Boolean): DynamoDBCodecDeriver =
    copy(transientEmptyCollection = transientEmptyCollection)

  def copy(
    zioSchema1Compatibility: Boolean = zioSchema1Compatibility,
    discriminatorKind: DiscriminatorKind = discriminatorKind,
    enumValuesAsStrings: Boolean = enumValuesAsStrings,
    fieldNameMapper: NameMapper = fieldNameMapper,
    caseNameMapper: NameMapper = caseNameMapper,
    transientNone: Boolean = transientNone,
    requireOptionFields: Boolean = requireOptionFields,
    transientEmptyCollection: Boolean = transientEmptyCollection,
    requireCollectionFields: Boolean = requireCollectionFields
  ): DynamoDBCodecDeriver =
    new DynamoDBCodecDeriver(
      zioSchema1Compatibility,
      discriminatorKind,
      enumValuesAsStrings,
      fieldNameMapper,
      caseNameMapper,
      transientNone,
      requireOptionFields,
      transientEmptyCollection,
      requireCollectionFields
    )

  type Elem
  type Col[_]
  type Key
  type Value
  type Wrapped
  type Map[_, _]
  type TC[_]

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
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[A]] =
    Lazy(
      deriveCodec(
        new Reflect.Variant(
          cases = cases.asInstanceOf[IndexedSeq[Term[Binding, A, _ <: A]]],
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
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[C[A]]] =
    Lazy {
      deriveCodec(
        new Reflect.Sequence(element.asInstanceOf[Reflect[Binding, A]], typeName, binding, doc, modifiers)
      )
    }

  override def deriveMap[F[_, _], M[_, _], K, V](
    key: Reflect[F, K],
    value: Reflect[F, V],
    typeName: TypeName[M[K, V]],
    binding: Binding[BindingType.Map[M], M[K, V]],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[M[K, V]]] =
    Lazy {
      deriveCodec(
        new Reflect.Map(
          key = key.asInstanceOf[Reflect[Binding, K]],
          value = value.asInstanceOf[Reflect[Binding, V]],
          typeName = typeName,
          mapBinding = binding,
          doc = doc,
          modifiers = modifiers
        )
      )
    }

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
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[A]] =
    Lazy {
      deriveCodec(
        new Reflect.Wrapper(
          wrapped.asInstanceOf[Reflect[Binding, B]],
          typeName,
          wrapperPrimitiveType,
          binding,
          doc,
          modifiers
        )
      )
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

  private[this] val recursiveRecordCache = new ThreadLocal[java.util.HashMap[TypeName[?], Array[FieldInfo]]] {
    override def initialValue: java.util.HashMap[TypeName[?], Array[FieldInfo]] = new java.util.HashMap
  }
  println(s"XXXXXXX recursiveRecordCache: $recursiveRecordCache") // TODO: Avi

  private[this] val discriminatorFields = new ThreadLocal[List[DiscriminatorFieldInfo]] {
    override def initialValue: List[DiscriminatorFieldInfo] = Nil
  }

  def deriveCodec[F[_, _], A](
    reflect: Reflect[F, A]
  ): DynamoDBCodec[A] = {
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
      else primitive.primitiveBinding.asInstanceOf[BindingInstance[TC, ?, A]].instance.force
    } else if (reflect.isVariant) {
      val variant = reflect.asVariant.get
      if (variant.variantBinding.isInstanceOf[Binding[?, ?]])
        option(variant) match {
          case Some(optReflect) =>
            val valueCodec = deriveCodec(optReflect).asInstanceOf[DynamoDBCodec[Any]]
            new DynamoDBCodec[Option[Any]]() {
              override def encoder: Encoder[Option[Any]] = {
                case Some(value) =>
                  valueCodec.encoder(value)
                case None        =>
                  AttributeValue.Null
              }

              override def decoder: Decoder[Option[Any]] = {
                case AttributeValue.Null =>
                  Right(None)
                case av                  =>
                  valueCodec.decoder(av) match {
                    case Right(value) => Right(Some(value))
                    case Left(err)    => Left(err)
                  }
              }
            }.asInstanceOf[DynamoDBCodec[A]]
          case _                => // Enum or Vanilla Variant
            val discr: Discriminator[A] = variant.variantBinding.asInstanceOf[Binding.Variant[A]].discriminator
            if (isEnumeration(variant)) {
              val map = new java.util.HashMap[String, Constructor[?]](variant.cases.length)

              def getInfos(variant: Reflect.Variant[F, A]): Array[EnumInfo] = {
                val cases = variant.cases
                val len   = cases.length
                val infos = new Array[EnumInfo](len)
                var idx   = 0
                while (idx < len) {
                  val case_       = cases(idx)
                  val caseReflect = case_.value
                  infos(idx) =
                    if (caseReflect.isVariant)
                      new EnumNodeInfo(
                        discriminator(caseReflect),
                        getInfos(caseReflect.asVariant.get.asInstanceOf[Reflect.Variant[F, A]])
                      )
                    else {
                      val constructor  = caseReflect.asRecord.get.recordBinding
                        .asInstanceOf[BindingInstance[TC, ?, ?]]
                        .binding
                        .asInstanceOf[Binding.Record[?]]
                        .constructor
                      var name: String = null
                      case_.modifiers.foreach {
                        case m: Modifier.rename => if (name eq null) name = m.name
                        case m: Modifier.alias  => map.put(m.name, constructor)
                        case _                  =>
                      }
                      if (name eq null) name = caseNameMapper(case_.name)
                      map.put(name, constructor)
                      new EnumLeafInfo(name, constructor)
                    }
                  idx += 1
                }
                infos
              }

              new DynamoDBCodec[A]() {
                private[this] val root           = EnumNodeInfo(discr, getInfos(variant))
                private[this] val constructorMap = map

                override def encoder: Encoder[A] =
                  (a: A) => {
                    val leafInfo: EnumLeafInfo = root.discriminate(a)
                    AttributeValue.String(leafInfo.name)
                  }

                override def decoder: Decoder[A] = {
                  case AttributeValue.String(name) =>
                    val constructor = constructorMap.get(name)
                    if (constructor eq null)
                      Left(ItemError.DecodingError("TODO"))
                    else {
                      val a: A = constructor.construct(null, 0).asInstanceOf[A]
                      Right(a)
                    }
                  case av                          =>
                    // TODO: Avi - debug why this is part of happy path
                    Left(ItemError.DecodingError(s"TODO ${av}"))
                }
              }
            } else // TODO: Avi - Vanilla Variants
              discriminatorKind match {

                case DiscriminatorKind.Field(fieldName) if hasOnlyRecordAndVariantCases(variant) =>
                  val map = new java.util.HashMap[String, CaseLeafInfo](variant.cases.length)

                  def getInfos(variant: Reflect.Variant[F, A], spans: List[DynamicOptic.Node.Case]): Array[CaseInfo] = {
                    val cases = variant.cases
                    val len   = cases.length
                    val infos = new Array[CaseInfo](len)
                    var idx   = 0
                    while (idx < len) {
                      val case_       = cases(idx)
                      val caseReflect = case_.value
                      val span        = new DynamicOptic.Node.Case(case_.name)
                      infos(idx) = if (caseReflect.isVariant) {
                        val caseVariant = caseReflect.asVariant.get.asInstanceOf[Reflect.Variant[F, A]]
                        new CaseNodeInfo(discriminator(caseReflect), getInfos(caseVariant, span :: spans))
                      } else {
                        val caseLeafInfo = new CaseLeafInfo(null, span :: spans)
                        var name: String = null
                        case_.modifiers.foreach {
                          case m: Modifier.rename => if (name eq null) name = m.name
                          case m: Modifier.alias  => map.put(m.name, caseLeafInfo)
                          case _                  =>
                        }
                        if (name eq null) name = caseNameMapper(case_.name)
                        map.put(name, caseLeafInfo)
                        discriminatorFields.set(new DiscriminatorFieldInfo(fieldName, name) :: discriminatorFields.get)
                        caseLeafInfo.codec = deriveCodec(caseReflect)
                        discriminatorFields.set(discriminatorFields.get.tail)
                        caseLeafInfo
                      }
                      idx += 1
                    }
                    infos
                  }

                  new DynamoDBCodec[A]() {
                    private[this] val root                   = new CaseNodeInfo(discr, getInfos(variant, Nil))
                    private[this] val caseMap                = map
                    private[this] val discriminatorFieldName = AttributeValue.String(fieldName)

                    override def encoder: Encoder[A] =
                      (a: A) => root.discriminate(a).codec.asInstanceOf[DynamoDBCodec[A]].encoder(a)

                    override def decoder: Decoder[A] = { (av: AttributeValue) =>
                      av match {
                        case avm: AttributeValue.Map =>
                          val maybeDiscriminatorValue = avm.value.get(discriminatorFieldName)
                          maybeDiscriminatorValue match {
                            case Some(AttributeValue.String(discriminatorValue)) =>
                              val caseInfo = caseMap.get(discriminatorValue)
                              if (caseInfo ne null)
                                caseInfo.codec.decoder(av)
                              else
                                Left(ItemError.DecodingError(s"Discriminator case for $discriminatorValue not found"))
                            case _                                               =>
                              Left(
                                ItemError.DecodingError(
                                  s"Not implemented, discriminatorValue: $maybeDiscriminatorValue"
                                )
                              )
                          }

                        case av                      =>
                          Left(ItemError.DecodingError(s"Expected an AttributeValue.Map but found ${av.showType}"))
                      }

                    }.asInstanceOf[Decoder[A]]
                  }

                case DiscriminatorKind.None                                                      =>
                  val codecs = Array.newBuilder[DynamoDBCodec[?]]

                  def getInfos(variant: Reflect.Variant[F, A]): Array[CaseInfo] = {
                    val cases = variant.cases
                    val len   = cases.length
                    val infos = new Array[CaseInfo](len)
                    var idx   = 0
                    while (idx < len) {
                      val caseReflect = cases(idx).value
                      infos(idx) = if (caseReflect.isVariant) {
                        val caseVariant = caseReflect.asVariant.get.asInstanceOf[Reflect.Variant[F, A]]
                        new CaseNodeInfo(discriminator(caseReflect), getInfos(caseVariant))
                      } else {
                        val codec = deriveCodec(caseReflect)
                        codecs.addOne(codec)
                        new CaseLeafInfo(codec, Nil)
                      }
                      idx += 1
                    }
                    infos
                  }

                  new DynamoDBCodec[A]() {
                    private[this] val root           = new CaseNodeInfo(discr, getInfos(variant))
                    private[this] val caseLeafCodecs = codecs.result()

                    override def encoder: Encoder[A] =
                      (a: A) => root.discriminate(a).codec.asInstanceOf[DynamoDBCodec[A]].encoder(a)

                    override def decoder: Decoder[A] =
                      (av: AttributeValue) => {
                        var idx                        = 0
                        var rtrn: Either[ItemError, A] = null
                        while (idx < caseLeafCodecs.length && (rtrn eq null)) {
                          val codec = caseLeafCodecs(idx).asInstanceOf[DynamoDBCodec[A]]
                          val x     = codec.decoder(av)
                          if (x.isRight)
                            rtrn = x
                          idx += 1
                        }

                        if (rtrn eq null)
                          Left(ItemError.DecodingError("Tried all cases using DiscriminatorKind.None without success"))
                        else
                          rtrn
                      }
                  }
                // DiscriminatorKind.Key
                case _                                                                           =>
                  val map = new java.util.HashMap[String, CaseLeafInfo](variant.cases.length)

                  def getInfos(variant: Reflect.Variant[F, A], spans: List[DynamicOptic.Node.Case]): Array[CaseInfo] = {
                    val cases = variant.cases
                    val len   = cases.length
                    val infos = new Array[CaseInfo](len)
                    var idx   = 0
                    while (idx < len) {
                      val case_       = cases(idx)
                      val caseReflect = case_.value
                      val span        = new DynamicOptic.Node.Case(case_.name)
                      infos(idx) = if (caseReflect.isVariant) {
                        val caseVariant = caseReflect.asVariant.get.asInstanceOf[Reflect.Variant[F, A]]
                        new CaseNodeInfo(discriminator(caseReflect), getInfos(caseVariant, span :: spans))
                      } else {
                        val caseLeafInfo = new CaseLeafInfo(deriveCodec(caseReflect), span :: spans)
                        var name: String = null
                        case_.modifiers.foreach {
                          case m: Modifier.rename => if (name eq null) name = m.name
                          case m: Modifier.alias  => map.put(m.name, caseLeafInfo)
                          case _                  =>
                        }
                        if (name eq null) name = caseNameMapper(case_.name)
                        map.put(name, caseLeafInfo)
                        caseLeafInfo.setName(name)
                        caseLeafInfo
                      }
                      idx += 1
                    }
                    infos
                  }

                  new DynamoDBCodec[A]() {
                    private[this] val root    = new CaseNodeInfo(discr, getInfos(variant, Nil))
                    private[this] val caseMap = map

                    override def encoder: Encoder[A] =
                      (a: A) => {
                        // TODO: Avi - create a wrapper Singleton Map with CaseName as key
                        val caseInfo = root.discriminate(a)
                        val av       = caseInfo.codec.asInstanceOf[DynamoDBCodec[A]].encoder(a)
                        AttributeValue.Map(AttributeValue.Map.JMapView.hash.single(caseInfo.getName, av))
                      }

                    override def decoder: Decoder[A] = { (avKeyMap: AttributeValue) =>
                      avKeyMap match {
                        case AttributeValue.Map(m) =>
                          val it = m.iterator
                          if (it.hasNext) {
                            val (key, avInner) = it.next()
                            val caseLeafInfo   = caseMap.get(key.value)
                            if (caseLeafInfo ne null)
                              caseLeafInfo.codec.decoder(avInner)
                            else Left(ItemError.DecodingError(s"Case ${key.value} not found for Variant"))
                          } else Left(ItemError.DecodingError(s"Can't decode an empty AttributeValue.Map"))
                        case av                    =>
                          Left(ItemError.DecodingError(s"Unexpected AttributeValue ${av.showType}"))
                      }
                    }.asInstanceOf[Decoder[A]]
                  }

              }
        }
      else
        variant.variantBinding.asInstanceOf[BindingInstance[TC, ?, A]].instance.force
    } else if (reflect.isMap) {
      val map = reflect.asMapUnknown.get.map
      if (map.mapBinding.isInstanceOf[Binding[?, ?]]) {
        val binding       = map.mapBinding.asInstanceOf[Binding.Map[Map, Key, Value]]
        val constructor   = binding.constructor
        val deconstructor = binding.deconstructor
        val keyCodec      =
          deriveCodec(map.key)
            .asInstanceOf[DynamoDBCodec[Key]]
        val keyEncoder    = keyCodec.encoder
        val keyDecoder    = keyCodec.decoder.asInstanceOf[Any => Either[ItemError.DecodingError, Key]]
        val valueCodec    = deriveCodec(map.value).asInstanceOf[DynamoDBCodec[Value]]
        val valueEncoder  = valueCodec.encoder
        val valueDecoder  = valueCodec.decoder
        val isNativeMap   = map.key.asPrimitive.exists(_.typeName.name == "String")

        if (isNativeMap)
          new DynamoDBCodec[Map[Key, Value]] {
            override def encoder: Encoder[Map[Key, Value]] =
              (m: Map[Key, Value]) => {
                val mapBuilder = AttributeValue.Map.JMapView.hash.builder
                val it         = deconstructor.deconstruct(m)
                while (it.hasNext) {
                  val kv             = it.next()
                  val key            = deconstructor.getKey(kv)
                  val value          = deconstructor.getValue(kv)
                  val keyVal: String = keyEncoder.asInstanceOf[Key => AttributeValue.String](key).value
                  mapBuilder.addOne(keyVal, valueEncoder(value))
                }
                AttributeValue.Map(mapBuilder.result)
              }

            override def decoder: Decoder[Map[Key, Value]] =
              (av: AttributeValue) => {
                if (!av.isInstanceOf[AttributeValue.Map])
                  Left(
                    ItemError.DecodingError(
                      s"Expected AttributeValue.Map, found ${if (av == null) "NULL!!!!!!" else av.showType}"
                    )
                  )
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
        else // TODO: non native Map encoding - Sequence of tuple2
          new DynamoDBCodec[Map[Key, Value]] {
            def encoder: Encoder[Map[Key, Value]] =
              (m: Map[Key, Value]) => {
                val len = deconstructor.size(m)
                val avs = new Array[AttributeValue](len)
                var idx = 0
                val it  = deconstructor.deconstruct(m)
                while (it.hasNext) {
                  val kv      = it.next()
                  val keyAv   = keyEncoder(deconstructor.getKey(kv))
                  val valueAv = valueEncoder(deconstructor.getValue(kv))
                  val tupleAv = AttributeValue.List(
                    scala.collection.immutable.ArraySeq(keyAv, valueAv)
                  )
                  avs(idx) = tupleAv
                  idx += 1
                }
                AttributeValue.List(scala.collection.immutable.ArraySeq.unsafeWrapArray(avs))
              }

            def decoder: Decoder[Map[Key, Value]] = {
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
      } else map.mapBinding.asInstanceOf[BindingInstance[TC, ?, A]].instance.force
    }.asInstanceOf[DynamoDBCodec[A]]
    else if (reflect.isSequence) {
      val sequence = reflect.asSequenceUnknown.get.sequence
      if (sequence.seqBinding.isInstanceOf[Binding[?, ?]]) {
        val binding       = sequence.seqBinding.asInstanceOf[Binding.Seq[Col, Elem]]
        val constructor   = binding.constructor
        val deconstructor = binding.deconstructor
        val elementCodec  = deriveCodec(sequence.element).asInstanceOf[DynamoDBCodec[Elem]]

        // TODO: optimise for primitive types
        new DynamoDBCodec[Col[Elem]] {
          override def encoder: Encoder[Col[Elem]] = { (col: Col[Elem]) =>
            val len = deconstructor.size(col)
            val avs = new Array[AttributeValue](len)
            var idx = 0
            val it  = deconstructor.deconstruct(col)
            while (it.hasNext) {
              val el = it.next()
              val av = elementCodec.encoder(el)
              avs(idx) = av
              idx += 1
            }
            AttributeValue.List(scala.collection.immutable.ArraySeq.unsafeWrapArray(avs))
          }

          override def decoder: Decoder[Col[Elem]] = { (av: AttributeValue) =>
            val errors = new ArrayBuffer[String]
            av match {
              case AttributeValue.List(items) =>
                val builder = constructor.newObjectBuilder[Elem](8)

                // TODO: Avi - error handling
                items.foreach { item =>
                  elementCodec.decoder(item) match {
                    case Right(a)  => constructor.addObject(builder, a.asInstanceOf[Elem])
                    case Left(err) => errors.addOne(err.message)
                  }
                }
                if (errors.isEmpty) {
                  val xs: Col[Elem] = constructor.resultObject[Elem](builder)
                  Right(xs)
                } else
                  Left(ItemError.DecodingError(errors.mkString(","))) // TODO: Avi - Make ItemError a composite
              case _                          =>
                Left(ItemError.DecodingError(s"unable to decode ${av.showType} as a list"))
            }
          }
        }
      }.asInstanceOf[DynamoDBCodec[A]]
      else sequence.seqBinding.asInstanceOf[BindingInstance[TC, ?, A]].instance.force
    } else if (reflect.isRecord) {
      val record = reflect.asRecord.get
      if (record.recordBinding.isInstanceOf[Binding[?, ?]]) {
        val binding = record.recordBinding.asInstanceOf[Binding.Record[A]]
        var offset  = 0L
        val fields  = record.fields

        var fieldInfos: Array[FieldInfo] = null // TODO: investigate recursive cache
        val len                          = fields.length
        val aliasMap                     = new java.util.HashMap[String, FieldInfo](len)
        if (fieldInfos eq null) {
          fieldInfos = new Array[FieldInfo](len)
          var idx = 0
          while (idx < len) {
            val field        = fields(idx)
            val fieldReflect = field.value
            val codec        = deriveCodec(fieldReflect)
            val optRequired  = isOptional(fieldReflect)
            val fieldInfo    = new FieldInfo(field.name, offset, codec, optRequired, isCollection(fieldReflect))
            fieldInfos(idx) = fieldInfo
            var name: String = null
            // TODO: Avi - have a separate cache for tuple as it needs less info
            field.modifiers.foreach {
              case m: Modifier.rename    => if (name eq null) name = m.name
              case m: Modifier.alias     => aliasMap.put(m.name, fieldInfo)
              case _: Modifier.transient => fieldInfo.nonTransient = false
              case _                     =>
            }
            if (name eq null) name = fieldNameMapper(field.name)
            aliasMap.put(name, fieldInfo)
            fieldInfo.setName(name)

            offset = RegisterOffset.add(codec.valueOffset, offset)
            idx += 1
          }
        }
        if (isTuple(reflect)) {
          new DynamoDBCodec[A] {
            private[this] val deconstructor = binding.deconstructor
            private[this] val constructor   = binding.constructor
            private[this] val usedRegisters = offset

            override def encoder: Encoder[A] = { value =>
              val arr: Array[AttributeValue]   = new Array[AttributeValue](len)
              val regs                         = Registers(usedRegisters)
              deconstructor.deconstruct(regs, 0, value)
              var idx                          = 0
              while (idx < len) {
                val field  = fieldInfos(idx)
                val offset = field.offset
                val codec  = field.codec
                field.valueType match {
                  case DynamoDBCodec.intType    =>
                    val v  = regs.getInt(offset)
                    // TODO: Avi - investigate direct encoding optimisations for primitives
                    val av = codec.asInstanceOf[DynamoDBCodec[Int]].encoder(v)
                    arr(idx) = av
                  case DynamoDBCodec.longType   =>
                    val v  = regs.getLong(offset)
                    val av = codec.asInstanceOf[DynamoDBCodec[Long]].encoder(v)
                    arr(idx) = av
                  case DynamoDBCodec.objectType =>
                    val v  = regs.getObject(offset)
                    val av = codec.asInstanceOf[DynamoDBCodec[AnyRef]].encoder(v)
                    arr(idx) = av
                  case _                        =>
                    val v  = regs.getObject(offset)
                    val av = codec.asInstanceOf[DynamoDBCodec[AnyRef]].encoder(v)
                    arr(idx) = av
                }
                idx += 1
              }
              val it: Iterable[AttributeValue] = scala.collection.immutable.ArraySeq.unsafeWrapArray(arr)
              AttributeValue.List(it)
            }

            override def decoder: Decoder[A] = {
              val regs                        = Registers(usedRegisters)
              val errors: ArrayBuffer[String] = new ArrayBuffer[String]()

              def setValue(field: FieldInfo, value: AttributeValue): Unit =
                field.valueType match {
                  case DynamoDBCodec.intType    =>
                    field.codec.asInstanceOf[DynamoDBCodec[Int]].decoder(value) match {
                      case Right(v)  => regs.setInt(field.offset, v)
                      case Left(err) => errors.addOne(err.message)
                    }
                  case DynamoDBCodec.longType   =>
                    field.codec.asInstanceOf[DynamoDBCodec[Long]].decoder(value) match {
                      case Right(v)  => regs.setLong(field.offset, v)
                      case Left(err) => errors.addOne(err.message)
                    }
                  case DynamoDBCodec.objectType =>
                    field.codec.asInstanceOf[DynamoDBCodec[AnyRef]].decoder(value) match {
                      case Right(v)  => regs.setObject(field.offset, v)
                      case Left(err) => errors.addOne(err.message)
                    }
                  case _                        => throw new Exception("TODO: decide what to do here")
                }

              def decodeLegacy(av: AttributeValue.List): Either[ItemError.DecodingError, A] = {

                @tailrec
                def setRegisterValueForLastElement(
                  avList: Chunk[AttributeValue],
                  count: Int
                ): Unit = {
                  val len = avList.size
                  avList match {
                    case Chunk(avRest, avLastElement) =>
                      val field          = fieldInfos(count)
                      setValue(field, avLastElement)
                      val isNotFinalPair = count > 1
                      avRest match {
                        case l: AttributeValue.List if isNotFinalPair =>
                          setRegisterValueForLastElement(l.value.asInstanceOf[Chunk[AttributeValue]], count - 1)
                        case avFirst                                  =>
                          val field = fieldInfos(count - 1) // skip to first element in list
                          setValue(field, avFirst)
                      }
                    case _                            => errors.addOne(s"Expected list size of 2 but found $len")
                  }
                }

                setRegisterValueForLastElement(av.value.asInstanceOf[Chunk[AttributeValue]], len - 1)

                if (errors.isEmpty) {
                  val a = constructor.construct(regs, RegisterOffset.Zero)
                  Right(a)
                } else Left(ItemError.DecodingError(errors.mkString(","))) // TODO: Avi - Make ItemError a composite

              }

              (av: AttributeValue) =>
                av match {
                  case avList: AttributeValue.List =>
                    val it  = avList.value.iterator
                    var idx = 0
                    while (it.hasNext && idx < len) {
                      val field = fieldInfos(idx)
                      val value = it.next()

                      setValue(field, value)
                      idx += 1
                    } // end while
                    if (errors.isEmpty) {
                      val a = constructor.construct(regs, RegisterOffset.Zero)
                      Right(a)
                    } else if (zioSchema1Compatibility) {
                      errors.clear()
                      decodeLegacy(avList)
                    } else
                      Left(ItemError.DecodingError(errors.mkString(","))) // TODO: Avi - Make ItemError a composite
                  case av: AttributeValue          =>
                    Left(DecodingError(s"Expected List attribute value but got: ${av.showType}"))
                }
            }
          }
        } else // Vanilla Record
          new DynamoDBCodec[A] {
            private[this] val constructor         = binding.constructor
            private[this] val deconstructor       = binding.deconstructor
            private[this] val usedRegisters       = offset
            private[this] val fields              = fieldInfos
            private[this] val skipNone            = transientNone
            private[this] val skipEmptyCollection = transientEmptyCollection
            private[this] val discriminatorField  = discriminatorFields.get.headOption.orNull

            override def encoder: Encoder[A] = { value =>
              val regs                         = Registers(usedRegisters)
              var idx                          = 0
              val mapBuilder: JMapView.Builder = AttributeValue.Map.JMapView.hash.builder
              deconstructor.deconstruct(regs, 0, value)
              val len                          = fields.length
              if (discriminatorField ne null) {
                val name  = discriminatorField.name
                val value = discriminatorField.value
                mapBuilder.addOne(name, AttributeValue.String(value))
              }

              while (idx < len) {
                val field  = fields(idx)
                val name   = field.name
                val offset = field.offset
                val codec  = field.codec
                val isOpt  = field.isOptional

                field.valueType match {
                  case DynamoDBCodec.intType    =>
                    val value = regs.getInt(offset)
                    val av    = codec.asInstanceOf[DynamoDBCodec[Int]].encoder(value)
                    mapBuilder.addOne(name, av)
                  case DynamoDBCodec.longType   =>
                    val value = regs.getLong(offset)
                    val av    = codec.asInstanceOf[DynamoDBCodec[Long]].encoder(value)
                    mapBuilder.addOne(name, av)
                  case DynamoDBCodec.objectType =>
                    val value = regs.getObject(offset)

                    if (isOpt && skipNone && (value == None))
                      ()
                    else if (field.isCollection && skipEmptyCollection && isCollectionEmpty(value))
                      ()
                    else {
                      val av = codec.asInstanceOf[DynamoDBCodec[AnyRef]].encoder(value)
                      mapBuilder.addOne(name, av)
                    }

                  case _                        =>
                    // TODO: think about what we do here
                    val value = regs.getObject(offset)
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

                      var av: AttributeValue          = avMap.value.getOrElse(AttributeValue.String(name), null)
                      val skipEmptyValueForOption     = field.isOptional && skipNone && (av eq null)
                      val skipEmptyValueForCollection = field.isCollection && skipEmptyCollection && (av eq null)
                      if (skipEmptyValueForOption)
                        av = AttributeValue.Null
                      else if (skipEmptyValueForCollection)
                        av = AttributeValue.List.empty

                      if (av eq null) // TODO: Avi - should we fail fast on this?
                        errors.addOne(s"Missing attribute value for field: $name")
                      else
                        field.valueType match {
                          case DynamoDBCodec.intType    =>
                            field.codec.asInstanceOf[DynamoDBCodec[Int]].decoder(av) match {
                              case Right(value) => regs.setInt(offset, value)
                              case Left(err)    => errors.addOne(err.message)
                            }
                          case DynamoDBCodec.longType   =>
                            field.codec.asInstanceOf[DynamoDBCodec[Long]].decoder(av) match {
                              case Right(value) => regs.setLong(offset, value)
                              case Left(err)    => errors.addOne(err.message)
                            }
                          case DynamoDBCodec.objectType =>
                            field.codec.asInstanceOf[DynamoDBCodec[AnyRef]].decoder(av) match {
                              case Right(value) => regs.setObject(offset, value)
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
          } // end if else not tuple
      } else
        record.recordBinding.asInstanceOf[BindingInstance[TC, ?, A]].instance.force
    } else if (reflect.isWrapper) {
      val wrapper = reflect.asWrapperUnknown.get.wrapper
      if (wrapper.wrapperBinding.isInstanceOf[Binding[?, ?]]) {
        val binding = wrapper.wrapperBinding.asInstanceOf[Binding.Wrapper[A, Wrapped]]
        val codec   = deriveCodec(wrapper.wrapped).asInstanceOf[DynamoDBCodec[Wrapped]]
        new DynamoDBCodec[A](wrapper.wrapperPrimitiveType.fold(DynamoDBCodec.objectType) {
          case _: PrimitiveType.Int  => DynamoDBCodec.intType
          case _: PrimitiveType.Long => DynamoDBCodec.longType
          case _                     => DynamoDBCodec.objectType
        }) {
          private[this] val unwrap       = binding.unwrap
          private[this] val wrap         = binding.wrap
          private[this] val wrappedCodec = codec

          override def encoder: Encoder[A] = (a: A) => wrappedCodec.encoder(unwrap(a))

          override def decoder: Decoder[A] =
            (av: AttributeValue) => {
              val x: Either[ItemError, Wrapped] = wrappedCodec.decoder(av)
              x match {
                // TODO: Avi - interpret Block's wrapped error
                case Right(w) => wrap(w).left.map(schemaError => ItemError.DecodingError(schemaError.message))
                case Left(e)  => Left(e)
              }
            }
        }
      } else
        wrapper.wrapperBinding.asInstanceOf[BindingInstance[TC, ?, A]].instance.force
    } else {
      println(s"XXXXX reflect type $reflect not handled yet")
      ???
    }
  }.asInstanceOf[DynamoDBCodec[A]]

  private[this] def option[F[_, _], A](variant: Reflect.Variant[F, A]): Option[Reflect[F, ?]] = {
    val typeName = variant.typeName
    val cases    = variant.cases
    if (
      typeName.namespace == Namespace.scala && typeName.name == "Option" &&
      cases.length == 2 && cases(1).name == "Some"
    ) cases(1).value.asRecord.map(_.fields(0).value)
    else None
  }

  private[this] def isOptional[F[_, _], A](reflect: Reflect[F, A]): Boolean =
    !requireOptionFields && reflect.isVariant && {
      val variant  = reflect.asVariant.get
      val typeName = reflect.typeName
      val cases    = variant.cases
      typeName.namespace == Namespace.scala && typeName.name == "Option" &&
      cases.length == 2 && cases(1).name == "Some"
    }

  private[this] def isTuple[F[_, _], A](reflect: Reflect[F, A]): Boolean =
    reflect.isRecord && {
      val typeName = reflect.typeName
      typeName.namespace == Namespace.scala && typeName.name.startsWith("Tuple")
    }

  private[this] def isEnumeration[F[_, _], A](variant: Reflect.Variant[F, A]): Boolean =
    enumValuesAsStrings && variant.cases.forall { case_ =>
      val caseReflect = case_.value
      caseReflect.asRecord.exists(_.fields.isEmpty) ||
      caseReflect.isVariant && caseReflect.asVariant.forall(isEnumeration)
    }

  /*private[this]*/
  def defaultValue[F[_, _], A](fieldReflect: Reflect[F, A]): Option[() => ?] =
    if (requireDefaultValueFields) None
    else {
      if (fieldReflect.isPrimitive) fieldReflect.asPrimitive.get.primitiveBinding
      else if (fieldReflect.isRecord) fieldReflect.asRecord.get.recordBinding
      else if (fieldReflect.isVariant) fieldReflect.asVariant.get.variantBinding
      else if (fieldReflect.isSequence) fieldReflect.asSequenceUnknown.get.sequence.seqBinding
      else if (fieldReflect.isMap) fieldReflect.asMapUnknown.get.map.mapBinding
      else if (fieldReflect.isWrapper) fieldReflect.asWrapperUnknown.get.wrapper.wrapperBinding
      else fieldReflect.asDynamic.get.dynamicBinding
    }.asInstanceOf[BindingInstance[TC, ?, A]].binding.defaultValue

  private[this] def isCollection[F[_, _], A](reflect: Reflect[F, A]): Boolean =
    !requireCollectionFields && (reflect.isSequence || reflect.isMap)

  private[this] def isCollectionEmpty(value: AnyRef): Boolean =
    value match {
      case value: Iterable[?] => value.isEmpty
      case value: Array[?]    => value.length == 0
      case _                  => false
    }

  private[this] def discriminator[F[_, _], A](caseReflect: Reflect[F, A]): Discriminator[?] =
    caseReflect.asVariant.get.variantBinding
      .asInstanceOf[BindingInstance[TC, ?, ?]]
      .binding
      .asInstanceOf[Binding.Variant[A]]
      .discriminator

  private[this] def hasOnlyRecordAndVariantCases[F[_, _], A](variant: Reflect.Variant[F, A]): Boolean =
    variant.cases.forall { case_ =>
      val caseReflect = case_.value
      caseReflect.isRecord || caseReflect.isVariant && caseReflect.asVariant.forall(hasOnlyRecordAndVariantCases)
    }

} // end class DynamoDBCodecDeriver

// TODO: Avi - change to non case class
private final case class FieldInfo(
  var name: String, // TODO: Avi - use DynamicOptic.Node.Field
  offset: RegisterOffset,
  codec: DynamoDBCodec[?],
  isOptional: Boolean,
  isCollection: Boolean
) {
  val valueType: Int        = codec.valueType
  var nonTransient: Boolean = true // TODO: Avi - override in the field processing loop

  def setName(name: String): Unit =
    this.name = name
}

private case class DiscriminatorFieldInfo(name: String, value: String)

trait CaseInfo

private class CaseLeafInfo(
  var codec: DynamoDBCodec[?],
  val spans: List[DynamicOptic.Node.Case]
) extends CaseInfo {
  private[this] var name: String = null

  def setName(name: String): Unit =
    this.name = name

  def getName = this.name
}

private class CaseNodeInfo[A](
  private[this] val discriminator: Discriminator[A],
  private[this] val caseInfos: Array[CaseInfo]
) extends CaseInfo {
  @tailrec
  final def discriminate(x: A): CaseLeafInfo =
    caseInfos(discriminator.discriminate(x)) match {
      case eli: CaseLeafInfo => eli
      case eni               => eni.asInstanceOf[CaseNodeInfo[A]].discriminate(x)
    }
}

trait EnumInfo

private case class EnumLeafInfo(name: String, constructor: Constructor[?]) extends EnumInfo

private case class EnumNodeInfo[A](
  discriminator: Discriminator[A],
  enumInfos: Array[EnumInfo]
) extends EnumInfo {
  @tailrec
  final def discriminate(x: A): EnumLeafInfo =
    enumInfos(discriminator.discriminate(x)) match {
      case eli: EnumLeafInfo => eli
      case eni               => eni.asInstanceOf[EnumNodeInfo[A]].discriminate(x)
    }

  // TODO: Avi - delete
  override def toString: String = s"enumInfos: ${enumInfos.toList}"
}
