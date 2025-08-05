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
import zio.blocks.schema.Lens
import zio.dynamodb.{ Decoder, Encoder }

trait DdbCodec[A] {

  def encoder: Encoder[A]
  def decoder: Decoder[A]
}

object BlocksDdbDerived extends Deriver[DdbCodec] {

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
    fields: IndexedSeq[Term[F, A, _]],
    typeName: TypeName[A],
    binding: Binding[BindingType.Record, A],
    doc: Doc,
    modifiers: Seq[Modifier.Record]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[A]] =
    Lazy(
      new DdbCodec[A] {
        override def encoder: Encoder[A] = ???
        override def decoder: Decoder[A] = ???
      }
    )

  override def deriveVariant[F[_, _], A](
    cases: IndexedSeq[Term[F, A, _]],
    typeName: TypeName[A],
    binding: Binding[BindingType.Variant, A],
    doc: Doc,
    modifiers: Seq[Modifier.Variant]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[A]] = ???

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

object TestDerived extends App {
  final case class Person(id: String, count: Int)
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived
    val id: Lens[Person, String]        = optic(_.id)
    val count: Lens[Person, Int]        = optic(_.count)
    val codec: DdbCodec[Person]         = schema.derive(BlocksDdbDerived)

    val y = codec.encoder(Person("1", 42))
    println(s"Encoded Person: $y")
  }
}
