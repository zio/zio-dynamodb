package zio.dynamodb.blocks

import zio.blocks.schema.binding.{ Binding, BindingType, HasBinding, RegisterOffset, Registers, SeqDeconstructor }
import zio.blocks.schema.derive.{ BindingInstance, Deriver }
import zio.blocks.schema._
import zio.dynamodb.DynamoDBError.ItemError
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
    Lazy(new DdbCodec[A] {
      val variant = Reflect.Variant(
        cases = cases.asInstanceOf[IndexedSeq[Term[Binding, A, _ <: A]]], // TODO: get scalafmt error when I use ? <: A
        typeName = typeName,
        variantBinding = binding,
        doc = doc,
        modifiers = modifiers
      )
      override def encoder: Encoder[A] = { (a: A) =>
        val enc = BlocksCodec.reflectEncoder(variant)
//      enc(a.asInstanceOf[variant.Structure])
        enc(a)
      }
      override def decoder: Decoder[A] = { (av: AttributeValue) =>
        val dec = BlocksCodec.reflectDecoder(variant)
        dec(av)
      }
    })

  def deriveSequence2[F[_, _], C[_], A](
    element: Reflect[F, A],
    typeName: TypeName[C[A]],
    binding: F[BindingType.Seq[C], C[A]],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[C[A]]] =
    Lazy(
      new DdbCodec[C[A]] {
        val sequence: Reflect.Sequence[F, A, C] = Reflect.Sequence(
          element = element,
          typeName = typeName,
          seqBinding = binding,
          doc = doc,
          modifiers = modifiers
        )
        println(s"$sequence $D")
        val v: C[A]                             = ???
        val deconstructor                       = sequence.seqDeconstructor // no type casts needed
        val it: Iterator[A]                     = deconstructor.deconstruct(v)
        println(it)
        override def encoder: Encoder[C[A]]     =
          ???
        override def decoder: Decoder[C[A]]     = ???
      }
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

  def deriveSequenceOld[F[_, _], C[_], A](
    element: Reflect[F, A],
    typeName: TypeName[C[A]],
    binding: Binding[BindingType.Seq[C], C[A]],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[C[A]]] = {
    println(D)
    Lazy(
      new DdbCodec[C[A]] {
        val sequence: Reflect.Sequence[F, A, C] = Reflect.Sequence(
          element = element,
          typeName = typeName,
          seqBinding = binding.asInstanceOf[F[BindingType.Seq[C], C[A]]],
          doc = doc,
          modifiers = modifiers
        )
        println(sequence)
        val v: C[A]                             = ???
        val deconstructor: SeqDeconstructor[C]  = sequence.asInstanceOf[Reflect.Sequence[F, A, C]].seqDeconstructor
        val it: Iterator[A]                     = deconstructor.deconstruct(v)
        println(it)
        override def encoder: Encoder[C[A]]     =
          (ca: C[A]) => {
            val x: Iterator[A]  = deconstructor.deconstruct(ca)
            val enc: Encoder[A] = BlocksCodec.reflectEncoder(element.asInstanceOf[Reflect.Bound[A]])
            println(enc)
            x.foreach { a =>
              val av = enc(a)
              println(s"XXXXXX av: $av")
            }
            ???
          }
        override def decoder: Decoder[C[A]]     = ???
      }
    )
  }

  override def deriveMap[F[_, _], M[_, _], K, V](
    key: Reflect[F, K],
    value: Reflect[F, V],
    typeName: TypeName[M[K, V]],
    binding: Binding[BindingType.Map[M], M[K, V]],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[M[K, V]]] =
    Lazy(
      new DdbCodec[M[K, V]] {
        val map                                = Reflect.Map(
          key = key.asInstanceOf[Reflect[Any, K]],
          value = value.asInstanceOf[Reflect[Any, V]],
          typeName = typeName,
          mapBinding = binding,
          doc = doc,
          modifiers = modifiers
        )
        println(map)
        override def encoder: Encoder[M[K, V]] = ???
        override def decoder: Decoder[M[K, V]] = ???
      }
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
  type Map[_, _]

  private def deriveCodec[A](
    schema: Schema[A],
    cache: mutable.HashMap[TypeName[?], Array[DdbCodec[?]]] = new mutable.HashMap
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
      val record        = reflect.asRecord.get
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
      val fieldCodecs   = cache.get(record.typeName) match {
        case Some(x) => x
        case _       =>
          val codecs = new Array[DdbCodec[?]](fields.length)
          cache.put(record.typeName, codecs)
          val len    = fields.length
          var idx    = 0
          while (idx < len) {
            val reflect = fields(idx).value
            codecs(idx) = deriveCodec(new Schema(reflect), cache)
            idx += 1
          }
          codecs
      }

      new DdbCodec[A] {
        override def encoder: Encoder[A] = {
          val encoder: Encoder[A] = (a: A) => {
            var avMap     = AttributeValue.Map.empty // TODO: Avi - create a mutable builder API for AV Map
            val registers = Registers(record.usedRegisters)
            deconstructor.deconstruct(registers, RegisterOffset.Zero, a)
            var offset    = RegisterOffset.Zero
            var idx       = -1
            fields.foreach { field =>
              idx += 1
              val encoder   = fieldCodecs(idx).encoder
              val fieldName = field.name
              val reflect   = field.value
              if (reflect.isPrimitive) {
                val primitiveType = reflect.asPrimitive.get.primitiveType
                primitiveType match {
                  case _: PrimitiveType.Int =>
                    val av: AttributeValue = encoder.asInstanceOf[Int => AttributeValue](registers.getInt(offset, 0))
                    avMap = avMap + (fieldName -> av)
                    offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
                  case _                    =>
                    val av = encoder.asInstanceOf[AnyRef => AttributeValue](registers.getObject(offset, 0))
                    avMap = avMap + (fieldName -> av)
                    offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                }
              } else {
                val av = encoder.asInstanceOf[AnyRef => AttributeValue](registers.getObject(offset, 0))
                avMap = avMap + (fieldName -> av)
                offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
              }
            }
            avMap
          }
          encoder
        }

        override def decoder: Decoder[A] =
          (av: AttributeValue) => {

            val errors: ArrayBuffer[String] = new ArrayBuffer
            val registers                   = Registers(record.usedRegisters)
            var offset                      = RegisterOffset.Zero
            var idx                         = -1
            if (av.isInstanceOf[AttributeValue.Map])
              fields.foreach { field =>
                idx += 1
                val decoder                                                                                = fieldCodecs(idx).decoder
                val reflect                                                                                = field.value
                def getField(av: AttributeValue.Map, fieldName: String): Either[ItemError, AttributeValue] =
                  av.get(fieldName).toRight(ItemError.DecodingError(s"Field $fieldName not found in record $av"))

                getField(
                  av.asInstanceOf[AttributeValue.Map],
                  field.name
                ) match {
                  case Right(avValue) =>
                    if (reflect.isPrimitive) {
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
                    } else
                      decoder.asInstanceOf[AnyRef => Either[ItemError, AnyRef]](avValue) match {
                        case Left(err)     => errors.addOne(err.message)
                        case Right(anyRef) =>
                          registers.setObject(offset, 0, anyRef)
                          offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                      }
                  case _              => errors.addOne(s"Field ${field.name} not found in record $av")
                }
              }
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
      val elementCodec  = deriveCodec(new Schema(element), cache)
      val encoder2      = elementCodec.encoder.asInstanceOf[A => AttributeValue]
      val decoder2      = elementCodec.decoder //.asInstanceOf[Any => A]
      println(s"$constructor $decoder2")
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
      val keyCodec      = deriveCodec(new Schema(map.key), cache)
      val keyEncoder    = keyCodec.encoder.asInstanceOf[Key => AttributeValue.String]
      val keyDecoder    = keyCodec.decoder.asInstanceOf[Any => Either[ItemError.DecodingError, Key]]
      val valueCodec    = deriveCodec(new Schema(map.value), cache)
      val valueEncoder  = valueCodec.encoder.asInstanceOf[Value => Any]
      val valueDecoder  = valueCodec.decoder //.asInstanceOf[Any => Value]
      println(s"$constructor $keyDecoder $valueDecoder")
      new DdbCodec[A] {
        override def encoder: Encoder[A] =
          (x: A) => {
            var map = AttributeValue.Map.empty
            val it  = deconstructor.deconstruct(x.asInstanceOf[Map[Key, Value]])
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
                val kv = it.next()
                (keyDecoder(kv._1), valueDecoder(kv._2)) match {
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
    } else
      ??? // TODO: Avi - Variant, Non Native Map, Wrapper, Dynamic
  }

  def isOption[A](variant: Reflect.Variant.Bound[A]): Boolean =
    variant.typeName.name == "Option" && variant.typeName.namespace.packages.mkString(".") == "scala"

  def isEither[A](variant: Reflect.Variant.Bound[A]): Boolean =
    variant.typeName.name == "Either" && variant.typeName.namespace.packages.mkString(".") == "scala.util"

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
  object PersonWithVariant     extends CompanionOptics[PersonWithVariant]     {
    implicit val schema: Schema[PersonWithVariant] = Schema.derived
  }

  final case class Person(id: String, age: Int)
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived
  }

//  val codec: DdbCodec[PersonWithVariant] = PersonWithVariant.schema.derive(BlocksDdbDerived)
//  val enc                                = codec.encoder(PersonWithVariant("1", Right(42)))
//  val codec: DdbCodec[Person] = Person.schema.derive(BlocksDdbDerived)
//  val enc                     = codec.encoder(Person("1", 42))
  val codec: DdbCodec[PersonWithCollections] = PersonWithCollections.schema.derive(BlocksDdbDerived)
  val enc                                    = codec.encoder(PersonWithCollections("1", numbers = List(1, 2), map = Map("a" -> 1, "b" -> 2)))
  val dec                                    = codec.decoder(enc)
//  val codec: DdbCodec[Person] = Person.schema.derive(BlocksDdbDerived)
//  val enc                     = codec.encoder(Person("1", 1))
//  val dec                     = codec.decoder(enc)
  println(s"XXXXXXXX enc: $enc")
  println(s"XXXXXXXX dec: $dec")
//  println(s"XXXXXXXX dec: ${dec.map(_.names.toList)}")

//  val dec = codec.decoder(enc)
//  println(s"XXXXXXXX enc: $enc dec: $dec")
}
