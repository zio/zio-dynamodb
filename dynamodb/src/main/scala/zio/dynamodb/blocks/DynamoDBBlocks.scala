package zio.dynamodb.blocks

import zio.blocks.schema.Reflect.Bound
import zio.blocks.schema._
import zio.blocks.schema.binding.BindingType.{ Primitive, Variant, Wrapper }
import zio.blocks.schema.binding._
import zio.blocks.schema.derive.Deriver

object DynamoDBBlocks {

  object DynamoDBDeriver extends Deriver[DynamoDBCodec] {
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
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[A]] = ???

    override def deriveSequence[F[_, _], C[_], A](
      element: Reflect[F, A],
      typeName: TypeName[C[A]],
      binding: Binding[BindingType.Seq[C], C[A]],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[C[A]]] = ???

    override def deriveMap[F[_, _], M[_, _], K, V](
      key: Reflect[F, K],
      value: Reflect[F, V],
      typeName: TypeName[M[K, V]],
      binding: Binding[BindingType.Map[M], M[K, V]],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[M[K, V]]] = ???

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
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[A]] = ???
  }

  def deriveCodec[A](
    reflect: Bound[A]
  ): DynamoDBCodec[A] = ???
}
