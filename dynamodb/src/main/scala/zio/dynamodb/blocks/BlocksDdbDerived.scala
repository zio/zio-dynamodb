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

  /*
Runtime class cast exception for Variant integration - looks like a bug in Blocks - park for now

sbt:root> zio-dynamodb/runMain zio.dynamodb.blocks.TestDerived
[info] compiling 1 Scala source to /Users/avinder/Workspaces/git/zio-dynamodb/dynamodb/target/scala-2.13/classes ...
[info] running zio.dynamodb.blocks.TestDerived
[error] java.lang.ClassCastException: class zio.blocks.schema.derive.BindingInstance cannot be cast to class zio.blocks.schema.binding.Binding (zio.blocks.schema.derive.BindingInstance and zio.blocks.schema.binding.Binding are in unnamed module of loader sbt.internal.LayeredClassLoader @40611741)
[error]         at zio.blocks.schema.binding.Binding$$anon$26.binding(Binding.scala:375)
[error]         at zio.blocks.schema.binding.HasBinding.variant(HasBinding.scala:37)
[error]         at zio.blocks.schema.binding.HasBinding.variant$(HasBinding.scala:36)
[error]         at zio.blocks.schema.binding.Binding$$anon$26.variant(Binding.scala:375)
[error]         at zio.blocks.schema.binding.HasBinding.discriminator(HasBinding.scala:70)
[error]         at zio.blocks.schema.binding.HasBinding.discriminator$(HasBinding.scala:70)
[error]         at zio.blocks.schema.binding.Binding$$anon$26.discriminator(Binding.scala:375)
[error]         at zio.blocks.schema.Reflect$Variant.discriminator(Reflect.scala:540)
[error]         at zio.dynamodb.blocks.BlocksCodec$.$anonfun$reflectEncoder$3(BlocksCodec.scala:155)
[error]         at zio.dynamodb.blocks.BlocksCodec$.$anonfun$reflectEncoder$2(BlocksCodec.scala:139)
[error]         at scala.collection.IterableOnceOps.foldLeft(IterableOnce.scala:687)
[error]         at scala.collection.IterableOnceOps.foldLeft$(IterableOnce.scala:721)
[error]         at scala.collection.AbstractIterable.foldLeft(Iterable.scala:935)
[error]         at zio.dynamodb.blocks.BlocksCodec$.$anonfun$reflectEncoder$1(BlocksCodec.scala:130)
[error]         at zio.dynamodb.blocks.BlocksDdbDerived$$anon$2.$anonfun$encoder$1(BlocksDdbDerived.scala:55)
[error]         at zio.dynamodb.blocks.TestDerived$.delayedEndpoint$zio$dynamodb$blocks$TestDerived$1(BlocksDdbDerived.scala:130)
[error]         at zio.dynamodb.blocks.TestDerived$delayedInit$body.apply(BlocksDdbDerived.scala:123)
[error]         at scala.Function0.apply$mcV$sp(Function0.scala:42)
[error]         at scala.Function0.apply$mcV$sp$(Function0.scala:42)
[error]         at scala.runtime.AbstractFunction0.apply$mcV$sp(AbstractFunction0.scala:17)
[error]         at scala.App.$anonfun$main$1(App.scala:98)
[error]         at scala.App.$anonfun$main$1$adapted(App.scala:98)
[error]         at scala.collection.IterableOnceOps.foreach(IterableOnce.scala:619)
[error]         at scala.collection.IterableOnceOps.foreach$(IterableOnce.scala:617)
[error]         at scala.collection.AbstractIterable.foreach(Iterable.scala:935)
[error]         at scala.App.main(App.scala:98)
[error]         at scala.App.main$(App.scala:96)
[error]         at zio.dynamodb.blocks.TestDerived$.main(BlocksDdbDerived.scala:123)
[error]         at zio.dynamodb.blocks.TestDerived.main(BlocksDdbDerived.scala)
[error]         at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
[error]         at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
[error]         at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
[error]         at java.base/java.lang.reflect.Method.invoke(Method.java:566)
   */
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

  /*
   Type class derivation compile problem
   I have a type class DdbCodec for which I am trying to derive a Sequence instance but it looks like the Derive trait
   `def deriveSequence` method does not align with Reflect.Sequence constructor
   When using

   */

  /*
  This fixes need for element.asInstanceOf[Reflect[Any, A]] cast that causes F to be lost as Any
  binding: F[BindingType.Seq[C], C[A]],   // <- use F here, not Binding
  se deriveSequence2 above
   */
  /*
  case class Sequence[F[_, _], A, C[_]](
    element: Reflect[F, A],
    typeName: TypeName[C[A]],
    seqBinding: F[BindingType.Seq[C], C[A]],
    doc: Doc = Doc.Empty,
    modifiers: Seq[Modifier.Reflect] = Nil
  ) extends Reflect[F, C[A]] { self =>
   */

  override def deriveSequence[F[_, _], C[_], A](
    element: Reflect[F, A],
    typeName: TypeName[C[A]],
    binding: Binding[BindingType.Seq[C], C[A]],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[C[A]]]    =
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
      println(constructor)
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
                    // TODO: Avi - AnyRef -> AV ?????
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
          (indexedRecord: AttributeValue) => {
            def fooGet(av: AttributeValue.Map, fieldName: String): Either[ItemError, AttributeValue] =
              av.get(fieldName).toRight(ItemError.DecodingError(s"Field $fieldName not found in record $av"))

            val errors: ArrayBuffer[String] = new ArrayBuffer
            val registers                   = Registers(record.usedRegisters)
            var offset                      = RegisterOffset.Zero
            var idx                         = -1
            if (indexedRecord.isInstanceOf[AttributeValue.Map])
              fields.foreach { field =>
                idx += 1
                val decoder = fieldCodecs(idx).decoder
                val reflect = field.value
                fooGet(
                  indexedRecord.asInstanceOf[AttributeValue.Map],
                  field.name
                ) match {
                  case Right(avValue) =>
                    if (reflect.isPrimitive) {
                      val primitiveType = reflect.asPrimitive.get.primitiveType
                      primitiveType match {
                        case _: PrimitiveType.Int =>
                          decoder.asInstanceOf[AnyRef => Either[ItemError, Int]](avValue) match {
                            case Left(err)  => errors.addOne(s"TODO: Avi - 3 error handling $err")
                            case Right(int) =>
                              registers.setInt(offset, 0, int)
                              offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
                          }
                        case x                    =>
                          println(s"XXXXXX unexpected primitive type $x")
                          decoder.asInstanceOf[AnyRef => Either[ItemError, AnyRef]](avValue) match {
                            case Left(err)     => errors.addOne(s"TODO: Avi - 4 error handling $err")
                            case Right(anyRef) =>
                              registers.setObject(offset, 0, anyRef)
                              offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                          }
                      }
                    } else
                      decoder.asInstanceOf[AnyRef => Either[ItemError, AnyRef]](avValue) match {
                        case Left(err)     => errors.addOne(s"TODO: Avi - 5 error handling $err")
                        case Right(anyRef) =>
                          registers.setObject(offset, 0, anyRef)
                          offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
                      }
                  case _              => errors.addOne(s"Field ${field.name} not found in record $indexedRecord")
                }
              }
            else
              errors.addOne(s"Expected AttributeValue.Map, found $indexedRecord")
            if (errors.isEmpty) {
              val a = constructor.construct(registers, RegisterOffset.Zero)
              Right(a) // TODO: Avi - handle errors
            } else Left(ItemError.DecodingError(s"TODO: Avi - 6 error handling ${errors.toList}"))
          }

      }
    } else
      ??? // TODO: Avi - Variant, Sequence, Map, Wrapper, Dynamic
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
  final case class PersonWithCollections(id: String, numbers: List[Int] = Nil, names: Array[String] = Array.empty)
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
//  val codec: DdbCodec[PersonWithCollections] = PersonWithCollections.schema.derive(BlocksDdbDerived)
//  val enc                                    = codec.encoder(PersonWithCollections("1", numbers = List(1, 2)))
  val codec: DdbCodec[Person] = Person.schema.derive(BlocksDdbDerived)
  val enc                     = codec.encoder(Person("1", 1))
  val dec                     = codec.decoder(enc)
  println(s"XXXXXXXX enc: $enc")
  println(s"XXXXXXXX dec: $dec")

//  val dec = codec.decoder(enc)
//  println(s"XXXXXXXX enc: $enc dec: $dec")
}
