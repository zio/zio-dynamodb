package zio.dynamodb.blocks

import zio.blocks.schema.Reflect.Bound
import zio.blocks.schema.binding.BindingType.{ Primitive, Variant, Wrapper }
import zio.blocks.schema.{
  Doc,
  DynamicValue,
  Lazy,
  Modifier,
  Namespace,
  PrimitiveType,
  Reflect,
  Schema,
  Term,
  TypeName,
  Validation
}
import zio.blocks.schema.binding.{ Binding, BindingType, HasBinding }
import zio.blocks.schema.derive.Deriver
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.{ AttributeValue, Decoder, Encoder }

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

  def stringOnlyCodec[A](reflect: Bound[A]): DdbCodec[A] =
    new DdbCodec[A] {
      override def encoder: Encoder[A] =
        _ => {
          println(s"XXXXXXXXXXXX $reflect")
          AttributeValue.String("blah blah blah")
        }

      override def decoder: Decoder[A] = {
        case AttributeValue.String(s) => Left(DecodingError(s"Cannot decode to $s"))
        case other                    => Left(DecodingError(s"Expected String attribute value but got: $other"))
      }
    }

  class DummyDeriver extends Deriver[DdbCodec] {
    override def derivePrimitive[F[_, _], A](
      primitiveType: PrimitiveType[A],
      typeName: TypeName[A],
      binding: Binding[Primitive, A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    ): Lazy[DdbCodec[A]] =
      Lazy(
        stringOnlyCodec(
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
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[A]] = ???

    override def deriveVariant[F[_, _], A](
      cases: IndexedSeq[Term[F, A, _]],
      typeName: TypeName[A],
      binding: Binding[Variant, A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[A]] = ???

    override def deriveSequence[F[_, _], C[_], A](
      element: Reflect[F, A],
      typeName: TypeName[C[A]],
      binding: Binding[BindingType.Seq[C], C[A]],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[C[A]]] = ???

    override def deriveMap[F[_, _], M[_, _], K, V](
      key: Reflect[F, K],
      value: Reflect[F, V],
      typeName: TypeName[M[K, V]],
      binding: Binding[BindingType.Map[M], M[K, V]],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[M[K, V]]] = ???

    override def deriveDynamic[F[_, _]](
      binding: Binding[BindingType.Dynamic, DynamicValue],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[DynamicValue]] = ???

    override def deriveWrapper[F[_, _], A, B](
      wrapped: Reflect[F, B],
      typeName: TypeName[A],
      wrapperPrimitiveType: Option[PrimitiveType[A]],
      binding: Binding[Wrapper[A, B], A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DdbCodec[A]] = ???
  }

}
