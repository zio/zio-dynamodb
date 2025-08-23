package zio.dynamodb.blocks

import zio.blocks.schema.derive.Deriver
import zio.blocks.schema.{ Doc, DynamicValue, Lazy, Modifier }
import zio.blocks.schema.binding.{ Binding, BindingType, HasBinding }
import zio.blocks.schema.PrimitiveType
import zio.blocks.schema.TypeName
import zio.blocks.schema.Term
import zio.blocks.schema.Reflect
import zio.blocks.schema.CompanionOptics
import zio.blocks.schema.Schema
import zio.dynamodb.{ Decoder, Encoder }
import zio.dynamodb.AttributeValue

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
    modifiers: Seq[Modifier.Primitive]
  ): Lazy[DdbCodec[A]] =
    Lazy(
      new DdbCodec[A] {
        override def encoder: Encoder[A] = BlocksCodec.primitiveEncoder(primitiveType)
        override def decoder: Decoder[A] = BlocksCodec.primitiveDecoder(primitiveType)
      }
    )

  override def deriveRecord[F[_, _], A](
    fields: IndexedSeq[Term[F, A, ?]],
    typeName: TypeName[A],
    binding: Binding[BindingType.Record, A],
    doc: Doc,
    modifiers: Seq[Modifier.Record]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[A]]    =
    Lazy(
      new DdbCodec[A] {
        val record = Reflect.Record(
          fields = fields.asInstanceOf[IndexedSeq[Term[Binding, A, ?]]],
          typeName = typeName,
          recordBinding = binding,
          doc = doc,
          modifiers = modifiers
        )
        override def encoder: Encoder[A] = { (a: A) =>
          val enc = BlocksCodec.reflectEncoder(record)
          enc(a.asInstanceOf[record.Structure])
        }
        override def decoder: Decoder[A] = { (av: AttributeValue) =>
          val dec = BlocksCodec.reflectDecoder(record)
          dec(av)
        }
      }
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
    modifiers: Seq[Modifier.Variant]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[A]] = 
    Lazy(
      new DdbCodec[A] {
    val variant = Reflect.Variant(
      cases = cases.asInstanceOf[IndexedSeq[Term[Binding, A, ? <: A]]],
      typeName = typeName,
      variantBinding = binding,
      doc = doc,
      modifiers = modifiers
    )
    override def encoder: Encoder[A] = { (a: A) =>
      val enc = BlocksCodec.reflectEncoder(variant)
      enc(a.asInstanceOf[variant.Structure])
    }
    override def decoder: Decoder[A] = { (av: AttributeValue) =>
      val dec = BlocksCodec.reflectDecoder(variant)
      dec(av)
    }
  })

  override def deriveSequence[F[_, _], C[_], A](
    element: Reflect[F, A],
    typeName: TypeName[C[A]],
    binding: Binding[BindingType.Seq[C], C[A]],
    doc: Doc,
    modifiers: Seq[Modifier.Seq]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[C[A]]] = ???

  override def deriveMap[F[_, _], M[_, _], K, V](
    key: Reflect[F, K],
    value: Reflect[F, V],
    typeName: TypeName[M[K, V]],
    binding: Binding[BindingType.Map[M], M[K, V]],
    doc: Doc,
    modifiers: Seq[Modifier.Map]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[M[K, V]]] = ???

  override def deriveDynamic[F[_, _]](
    binding: Binding[BindingType.Dynamic, DynamicValue],
    doc: Doc,
    modifiers: Seq[Modifier.Dynamic]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[DynamicValue]] = ???

  override def deriveWrapper[F[_, _], A, B](
    wrapped: Reflect[F, B],
    typeName: TypeName[A],
    binding: Binding[BindingType.Wrapper[A, B], A],
    doc: Doc,
    modifiers: Seq[Modifier.Wrapper]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[A]] = ???

}

/*
zio-dynamodb/runMain zio.dynamodb.blocks.TestDerived
 */
object TestDerived extends App {
  final case class Person(id: String, either: Either[String, Int])
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived
  }

  val codec: DdbCodec[Person] = Person.schema.derive(BlocksDdbDerived)
  val enc                     = codec.encoder(Person("1", Right(42) ))
  val dec                     = codec.decoder(enc)
  println(s"XXXXXXXX enc: $enc dec: $dec")
}
