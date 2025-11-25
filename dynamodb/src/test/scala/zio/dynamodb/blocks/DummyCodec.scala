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
import zio.blocks.schema.binding.{ Binding, BindingType, HasBinding, Registers }
import zio.blocks.schema.derive.{ BindingInstance, Deriver }
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
        a => {
          println(s"XXXXX reflect: $reflect encoding value: $a")
          AttributeValue.String(a.toString)
        }

      override def decoder: Decoder[A] = {
        case AttributeValue.String(s) => Left(DecodingError(s"Cannot decode to $s"))
        case other                    => Left(DecodingError(s"Expected String attribute value but got: $other"))
      }
    }

  object DummyDeriver extends Deriver[DdbCodec] {
    override def derivePrimitive[F[_, _], A](
      primitiveType: PrimitiveType[A],
      typeName: TypeName[A],
      binding: Binding[Primitive, A],
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
      fields: IndexedSeq[Term[F, A, _]],
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

  def deriveCodec[A](
    reflect: Bound[A],
    cache: java.util.HashMap[TypeName[?], CacheEntry] = new java.util.HashMap
  ): DdbCodec[A] =
    if (reflect.isPrimitive) {
      val primitive = reflect.asPrimitive.get
      if (primitive.primitiveBinding.isInstanceOf[Binding[?, ?]]) {
        println(s"XXXXX Deriving primitive codec for $reflect")
        stringOnlyCodec(reflect)
      } else primitive.primitiveBinding.asInstanceOf[BindingInstance[DdbCodec, ?, A]].instance.force
    } else if (reflect.isRecord) {
      val record = reflect.asRecord.get
      println(s"XXXXX reflect is record: ${record.typeName.name}")
      if (record.recordBinding.isInstanceOf[Binding[?, ?]]) {
        println(s"XXXXX record is Binding: ${record.typeName.name}")
        val binding = record.recordBinding.asInstanceOf[Binding.Record[A]]
        val offset  = 0
        val fields  = record.fields

        //      val registers = Registers.computeRegisters(record)
        println(s"XXXXX Deriving record codec for ${record.typeName.name}")

        val fieldCodecs = cache.get(record.typeName) match {
          case null =>
            println(s"XXXXX Cache miss for record type: ${record.typeName.name}")
            val codecs: CacheEntry = CacheEntry.makeWithNames(fields.length)
            if (!fields.isEmpty) {
              println(s"XXXXX Cache PUT for record type: ${record.typeName.name}")
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
          case x    =>
            println(s"XXXXX Cache HIT for record type: ${record.typeName.name}")
            x
        }

        new DdbCodec[A] {
          println(s"XXXXXXXXXXXXXXX new DdbCodec for ${record.typeName.name}")
//          val constructor   = binding.constructor
          val deconstructor = binding.deconstructor
          val usedRegisters = offset

          override def encoder: Encoder[A] = { value =>
            println(s"XXXXX Encoding record value: $value")
            val regs       = Registers(usedRegisters)
            var idx        = 0
            deconstructor.deconstruct(regs, 0, value)
            val mapBuilder = Map.newBuilder[AttributeValue.String, AttributeValue]
            while (idx < fields.length) {
              val field      = fields(idx)
              val fieldValue = regs.getObject(offset, 0)
//              val encAv      = deriveCodec(field.value).asInstanceOf[DdbCodec[AnyRef]].encoder(fieldValue)
              val encAv      = fieldCodecs.byIndex(idx).asInstanceOf[DdbCodec[AnyRef]].encoder(fieldValue)
              println(s"XXXXX Field: ${field.name} -> $fieldValue -> enc: $encAv")
              // For demonstration, we encode all fields as String "dummy"
              mapBuilder.addOne(AttributeValue.String(field.name) -> encAv)
              idx += 1
            }
            AttributeValue.Map(mapBuilder.result())
          }

          override def decoder: Decoder[A] = ???
        }
      } else {
        println(s"XXXXX record is NOT Binding: $reflect")
        record.recordBinding.asInstanceOf[BindingInstance[DdbCodec, ?, A]].instance.force
      }
    } else {
      println(s"XXXXX reflect: $reflect not handled yet")
      ???
    }

}
