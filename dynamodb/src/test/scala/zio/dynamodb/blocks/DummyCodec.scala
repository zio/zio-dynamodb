package zio.dynamodb.blocks

import zio.blocks.schema.Reflect.Bound
import zio.blocks.schema.binding.BindingType.{ Primitive, Variant, Wrapper }
import zio.blocks.schema.binding.RegisterOffset.RegisterOffset
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

import java.util

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

  val cache: ThreadLocal[java.util.HashMap[TypeName[?], CacheEntry2]] =
    new ThreadLocal[util.HashMap[TypeName[_], CacheEntry2]] {
      override def initialValue(): java.util.HashMap[TypeName[?], CacheEntry2] = new java.util.HashMap
    }

  val stringCodec: DynamoDbCodec[String] =
    new DynamoDbCodec[String](valueType = DynamoDbCodec.objectType) {
      override def encoder: Encoder[String] =
        a => {
          println(s"XXXXX encoding value: $a")
          AttributeValue.String(a.toString)
        }

      override def decoder: Decoder[String] = {
        case AttributeValue.String(s) => Right(s)
        case other                    => Left(DecodingError(s"Expected String attribute value but got: $other"))
      }
    }

  object DummyDeriver extends Deriver[DynamoDbCodec] {
    override def derivePrimitive[F[_, _], A](
      primitiveType: PrimitiveType[A],
      typeName: TypeName[A],
      binding: Binding[Primitive, A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    ): Lazy[DynamoDbCodec[A]] =
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
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec[A]] =
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
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec[A]] = ???

    override def deriveSequence[F[_, _], C[_], A](
      element: Reflect[F, A],
      typeName: TypeName[C[A]],
      binding: Binding[BindingType.Seq[C], C[A]],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec[C[A]]] = ???

    override def deriveMap[F[_, _], M[_, _], K, V](
      key: Reflect[F, K],
      value: Reflect[F, V],
      typeName: TypeName[M[K, V]],
      binding: Binding[BindingType.Map[M], M[K, V]],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec[M[K, V]]] = ???

    override def deriveDynamic[F[_, _]](
      binding: Binding[BindingType.Dynamic, DynamicValue],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec[DynamicValue]] = ???

    override def deriveWrapper[F[_, _], A, B](
      wrapped: Reflect[F, B],
      typeName: TypeName[A],
      wrapperPrimitiveType: Option[PrimitiveType[A]],
      binding: Binding[Wrapper[A, B], A],
      doc: Doc,
      modifiers: Seq[Modifier.Reflect]
    )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDbCodec[A]] = ???
  }

  def deriveCodec[A](
    reflect: Bound[A]
  ): DynamoDbCodec[A] =
    if (reflect.isPrimitive) {
      val primitive = reflect.asPrimitive.get
      if (primitive.primitiveBinding.isInstanceOf[Binding[?, ?]])
        (primitive.primitiveType match {
          case _: PrimitiveType.String => stringCodec
          case _                       => ???
        }).asInstanceOf[DynamoDbCodec[A]]
      else primitive.primitiveBinding.asInstanceOf[BindingInstance[DynamoDbCodec, ?, A]].instance.force
    } else if (reflect.isRecord) {
      val record = reflect.asRecord.get
      if (record.recordBinding.isInstanceOf[Binding[?, ?]]) {
        val binding = record.recordBinding.asInstanceOf[Binding.Record[A]]
        val offset  = 0
        val fields  = record.fields

        //      val registers = Registers.computeRegisters(record)

        val fieldCodecs = cache.get.get(record.typeName) match {
          case null =>
            val codecs: CacheEntry2 = CacheEntry2.makeWithNames(fields.length)
            if (!fields.isEmpty) {
              println(s"XXXXX Cache PUT for record type: ${record.typeName.name}")
              cache.get.put(record.typeName, codecs)
              val len = fields.length
              var idx = 0
              while (idx < len) {
                val reflect = fields(idx).value
                codecs.addEntry(deriveCodec(reflect), fields(idx).name, idx)
                idx += 1
              }
            }
            codecs
          case x    =>
            println(s"XXXXX Cache HIT for record type: ${record.typeName.name}")
            x
        }

        new DynamoDbCodec[A] {
//          val constructor   = binding.constructor
          val deconstructor = binding.deconstructor
          val usedRegisters = offset

          override def encoder: Encoder[A] = { value =>
            val regs       = Registers(usedRegisters)
            var idx        = 0
            deconstructor.deconstruct(regs, 0, value)
            val mapBuilder = Map.newBuilder[AttributeValue.String, AttributeValue]
            while (idx < fields.length) {
              val field      = fields(idx)
              val fieldValue = regs.getObject(offset, 0)
//              val encAv      = deriveCodec(field.value).asInstanceOf[DynamoDbCodec[AnyRef]].encoder(fieldValue)
              val encAv      = fieldCodecs.byIndex(idx).asInstanceOf[DynamoDbCodec[AnyRef]].encoder(fieldValue)
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
        record.recordBinding.asInstanceOf[BindingInstance[DynamoDbCodec, ?, A]].instance.force
      }
    } else {
      println(s"XXXXX reflect: $reflect not handled yet")
      ???
    }

  final class CacheEntry2 private (
    val fieldCodecs: Array[DynamoDbCodec[?]],
    names: Array[String]
  )                  {
    def size: Int                 = fieldCodecs.length // TODO: Avi - for debugging - remove
    override def toString: String = s"CacheEntry2(${fieldCodecs.toSeq}, ${names.toSeq})"

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

    def addEntry(codec: DynamoDbCodec[?], name: String, index: Int): Unit = {
      fieldCodecs(index) = codec
      if (hasNames)
        names(index) = name
    }

    def byIndex(i: Int): DynamoDbCodec[?] = fieldCodecs(i)

    def byName(name: String): Option[DynamoDbCodec[?]] =
      if (!hasNames) None
      else nameToIndex.get(name).map(fieldCodecs)
  }
  object CacheEntry2 {
    def makeWithNames(size: Int) =
      new CacheEntry2(new Array[DynamoDbCodec[?]](size), new Array[String](size))
  }

  final case class FieldInfo(name: String, offset: RegisterOffset, codec: DynamoDbCodec[?], isOptional: Boolean)
}
