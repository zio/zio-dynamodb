/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb.blocks.schema

import zio.blocks.chunk.{ Chunk, ChunkBuilder }
import zio.blocks.docs.Doc
import zio.blocks.schema._
import zio.blocks.schema.binding.RegisterOffset.RegisterOffset
import zio.blocks.schema.binding._
import zio.blocks.schema.derive.{ BindingInstance, Deriver, InstanceOverride, InstanceOverrideByType }
import zio.blocks.schema.NameMapper
import zio.blocks.schema.json.{ DiscriminatorKind, Json }
import zio.blocks.typeid.{ Owner, TypeId }
import zio.dynamodb.{ AttributeValue, Decoder, Encoder, JMapView }
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.blocks.schema.DynamoDBCodecDeriver.{ dynamicValueCodec, jsonAVCodec }

import java.time._
import java.time.format.{ DateTimeFormatterBuilder, SignStyle }
import java.time.temporal.ChronoField
import java.util.{ Currency, UUID }

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * borrows heavily from Andriy Plokhotnyuk's zio-blocks JSON codec https://github.com/zio/zio-blocks
 */
object DynamoDBCodecDeriver
    extends DynamoDBCodecDeriver(
      schema1TupleCompat = Schema1Compat.ReadBothWriteNew,
      schema1ByteSequenceCompat = Schema1Compat.ReadNewWriteNew,
      schema1YearCompat = Schema1Compat.ReadNewWriteNew,
      discriminatorKind = DiscriminatorKind.Key,
      enumValuesAsStrings = true,
      fieldNameMapper = NameMapper.Identity,
      caseNameMapper = NameMapper.Identity,
      transientNone = true,
      requireOptionFields = false,
      transientEmptyCollection = false,
      requireCollectionFields = false,
      transientDefaultValue = false,
      requireDefaultValueFields = false,
      rejectExtraFields = false
    ) {

  /**
   * Codec for `DynamicValue` stored as native DynamoDB `AttributeValue` types.
   *
   * Wire conventions:
   *   - `DynamicValue.Null`     → `AttributeValue.Null`
   *   - `DynamicValue.unit`     → `AttributeValue.Map` (empty)
   *   - `DynamicValue.Record`   → `AttributeValue.Map` (non-empty)
   *   - `DynamicValue.Sequence` → `AttributeValue.List`
   *   - `DynamicValue.boolean`  → `AttributeValue.Bool`
   *   - Numeric primitives      → `AttributeValue.Number`
   *   - String-like primitives  → `AttributeValue.String`
   *
   * `DynamicValue.Null` and `DynamicValue.unit` use distinct wire types, so they are
   * unambiguous on both encode and decode.
   *
   * '''Known limitation''' — do not use `Option[DynamicValue]` as a field type when
   * the inner value may be `DynamicValue.Null`. The `Option` codec encodes `None` as
   * `AttributeValue.Null`, which is indistinguishable on decode from
   * `Some(DynamicValue.Null)`. Use `DynamicValue` directly instead: `DynamicValue.Null`
   * already carries the null semantic without needing an `Option` wrapper.
   */
  val dynamicValueCodec: DynamoDBCodec[DynamicValue] =
    new DynamoDBCodec[DynamicValue](valueType = DynamoDBCodec.objectType) {

      override def encoder: Encoder[DynamicValue] =
        (dv: DynamicValue) =>
          dv match {
            case primitive: DynamicValue.Primitive =>
              primitive.value match {
                case _: PrimitiveValue.Unit.type      => AttributeValue.Map.empty
                case v: PrimitiveValue.String         => AttributeValue.String(v.value)
                case v: PrimitiveValue.Boolean        => AttributeValue.Bool(v.value)
                case v: PrimitiveValue.Byte           => AttributeValue.Number(BigDecimal.valueOf(v.value.toLong))
                case v: PrimitiveValue.Short          => AttributeValue.Number(BigDecimal.valueOf(v.value.toLong))
                case v: PrimitiveValue.Int            => AttributeValue.Number(BigDecimal.valueOf(v.value.toLong))
                case v: PrimitiveValue.Long           => AttributeValue.Number(BigDecimal.valueOf(v.value))
                case v: PrimitiveValue.Float          => AttributeValue.Number(BigDecimal.valueOf(v.value.toDouble))
                case v: PrimitiveValue.Double         => AttributeValue.Number(BigDecimal.valueOf(v.value))
                case v: PrimitiveValue.Char           => AttributeValue.String(v.value.toString)
                case v: PrimitiveValue.BigInt         => AttributeValue.Number(BigDecimal(v.value))
                case v: PrimitiveValue.BigDecimal     => AttributeValue.Number(BigDecimal(v.value.bigDecimal))
                case v: PrimitiveValue.UUID           => AttributeValue.String(v.value.toString)
                case v: PrimitiveValue.Currency       => AttributeValue.String(v.value.getCurrencyCode)
                case v: PrimitiveValue.DayOfWeek      => AttributeValue.String(v.value.name)
                case v: PrimitiveValue.Month          => AttributeValue.String(v.value.name)
                case v: PrimitiveValue.Year           => AttributeValue.Number(BigDecimal.valueOf(v.value.getValue.toLong))
                case v: PrimitiveValue.YearMonth      => AttributeValue.String(v.value.toString)
                case v: PrimitiveValue.MonthDay       => AttributeValue.String(v.value.toString)
                case v: PrimitiveValue.Duration       => AttributeValue.String(v.value.toString)
                case v: PrimitiveValue.Instant        => AttributeValue.String(v.value.toString)
                case v: PrimitiveValue.LocalDate      => AttributeValue.String(v.value.toString)
                case v: PrimitiveValue.LocalTime      => AttributeValue.String(v.value.toString)
                case v: PrimitiveValue.LocalDateTime  => AttributeValue.String(v.value.toString)
                case v: PrimitiveValue.OffsetTime     => AttributeValue.String(v.value.toString)
                case v: PrimitiveValue.OffsetDateTime => AttributeValue.String(v.value.toString)
                case v: PrimitiveValue.ZonedDateTime  => AttributeValue.String(v.value.toString)
                case v: PrimitiveValue.ZoneId         => AttributeValue.String(v.value.getId)
                case v: PrimitiveValue.ZoneOffset     => AttributeValue.String(v.value.getId)
                case v: PrimitiveValue.Period         => AttributeValue.String(v.value.toString)
              }
            case DynamicValue.Null                 => AttributeValue.Null
            case v: DynamicValue.Variant           =>
              v.value match {
                case record: DynamicValue.Record =>
                  // Key discriminator: {"CaseName": {fields}} — mirrors the default sealed-trait
                  // encoding. Schema.toDynamicValue always produces a Record inner value (empty for
                  // no-field cases), so this branch handles all cases derived from Schema.
                  val builder = JMapView.linked.builder[AttributeValue.String, AttributeValue]
                  builder.addOne(AttributeValue.String(v.caseNameValue), encoder(record))
                  AttributeValue.Map(builder.result)
                case _                           =>
                  AttributeValue.String(v.caseNameValue)
              }
            case sequence: DynamicValue.Sequence   =>
              val builder = ChunkBuilder.make[AttributeValue]()
              val it      = sequence.elements.iterator
              while (it.hasNext) {
                val item = it.next()
                val enc  = encoder(item)
                builder.addOne(enc)
              }
              AttributeValue.List(builder.result())
            case record: DynamicValue.Record       =>
              val builder = JMapView.linked.builder[AttributeValue.String, AttributeValue]
              val fields  = record.fields
              val it      = fields.iterator
              while (it.hasNext) {
                val kv  = it.next()
                val enc = encoder(kv._2)
                builder.addOne(AttributeValue.String(kv._1), enc)
              }
              AttributeValue.Map(builder.result)
            case _: DynamicValue.Map               =>
              throw new IllegalStateException("DynamicValue.Map not supported")
          }

      override def decoder: Decoder[DynamicValue] = {
        case m: AttributeValue.Map if m.value.isEmpty => Right(DynamicValue.unit)
        case AttributeValue.Null                      => Right(DynamicValue.Null)
        case av: AttributeValue.String                => Right(DynamicValue.string(av.value))
        case av: AttributeValue.Number                =>
          val bd: BigDecimal = av.value
          val longValue      = bd.bigDecimal.longValue
          // Start with BigDecimal and attempt to narrow down to the smallest numeric type
          val dv             = if (bd == BigDecimal(longValue)) {
            val intValue = longValue.toInt
            if (longValue == intValue) DynamicValue.int(intValue)
            else DynamicValue.long(longValue)
          } else DynamicValue.bigDecimal(bd)
          Right(dv)
        case av: AttributeValue.List                  =>
          val builder = ChunkBuilder.make[DynamicValue]()
          val it      = av.value.iterator
          val errors  = new ArrayBuffer[DecodingError.Cause]
          while (it.hasNext) {
            val item = it.next()
            decoder(item) match {
              case Right(dv) => builder.addOne(dv)
              case Left(err) => errors.addAll(err.causes)
            }
          }
          if (errors.isEmpty)
            Right(new DynamicValue.Sequence(builder.result()))
          else
            Left(ItemError.DecodingError(errors.toArray))
        case av: AttributeValue.Bool                  =>
          Right(DynamicValue.boolean(av.value))
        case av: AttributeValue.Map                   =>
          val builder = ChunkBuilder.make[(String, DynamicValue)]()
          val it      = av.value.iterator
          val errors  = new ArrayBuffer[DecodingError.Cause]
          while (it.hasNext) {
            val kv = it.next()
            decoder(kv._2) match {
              case Right(dv) => builder.addOne((kv._1.value, dv))
              case Left(err) => errors.addAll(err.causes)
            }
          }
          if (errors.isEmpty)
            Right(new DynamicValue.Record(builder.result()))
          else
            Left(ItemError.DecodingError(errors.toArray))
        case av                                       =>
          Left(ItemError.DecodingError(s"Unknown AttributeValue type:${av.showType}"))
      }

    }

  val jsonAVCodec: DynamoDBCodec[Json] =
    new DynamoDBCodec[Json](valueType = DynamoDBCodec.objectType) {

      override def encoder: Encoder[Json] = {
        case obj: Json.Object  =>
          val b  = JMapView.linked.builder[AttributeValue.String, AttributeValue]
          val it = obj.value.iterator
          while (it.hasNext) { val kv = it.next(); b.addOne(AttributeValue.String(kv._1), encoder(kv._2)) }
          AttributeValue.Map(b.result)
        case arr: Json.Array   =>
          val len = arr.value.size
          val avs = new Array[AttributeValue](len)
          var idx = 0
          val it  = arr.value.iterator
          while (it.hasNext) { avs(idx) = encoder(it.next()); idx += 1 }
          AttributeValue.List(scala.collection.immutable.ArraySeq.unsafeWrapArray(avs))
        case str: Json.String  => AttributeValue.String(str.value)
        case num: Json.Number  => AttributeValue.Number(num.value)
        case boo: Json.Boolean => AttributeValue.Bool(boo.value)
        case Json.Null         => AttributeValue.Null
      }

      override def decoder: Decoder[Json] = {
        case av: AttributeValue.Map    =>
          val errors = new ArrayBuffer[DecodingError.Cause]
          val b      = ChunkBuilder.make[(String, Json)]()
          val it     = av.value.iterator
          while (it.hasNext) {
            val kv = it.next()
            decoder(kv._2) match {
              case Right(j)  => b.addOne((kv._1.value, j))
              case Left(err) => errors.addAll(err.causes)
            }
          }
          if (errors.isEmpty) Right(new Json.Object(b.result()))
          else Left(ItemError.DecodingError(errors.toArray))
        case av: AttributeValue.List   =>
          val errors = new ArrayBuffer[DecodingError.Cause]
          val b      = ChunkBuilder.make[Json]()
          val it     = av.value.iterator
          while (it.hasNext)
            decoder(it.next()) match {
              case Right(j)  => b.addOne(j)
              case Left(err) => errors.addAll(err.causes)
            }
          if (errors.isEmpty) Right(new Json.Array(b.result()))
          else Left(ItemError.DecodingError(errors.toArray))
        case av: AttributeValue.String => Right(new Json.String(av.value))
        case av: AttributeValue.Number => Right(Json.Number(av.value))
        case av: AttributeValue.Bool   => Right(Json.Boolean(av.value))
        case AttributeValue.Null       => Right(Json.Null)
        case av                        => Left(ItemError.DecodingError(s"Cannot decode ${av.showType} as Json"))
      }
    }
}

class DynamoDBCodecDeriver private (
  schema1TupleCompat: Schema1Compat,
  schema1ByteSequenceCompat: Schema1Compat,
  schema1YearCompat: Schema1Compat,
  discriminatorKind: DiscriminatorKind,
  enumValuesAsStrings: Boolean,
  fieldNameMapper: NameMapper,
  caseNameMapper: NameMapper,
  transientNone: Boolean,
  requireOptionFields: Boolean,
  transientEmptyCollection: Boolean,
  requireCollectionFields: Boolean, // Schema1 codecs assumes this is false
  transientDefaultValue: Boolean,
  requireDefaultValueFields: Boolean,
  rejectExtraFields: Boolean
) extends Deriver[DynamoDBCodec] { self =>

  def withSchema1TupleCompatibility(v: Schema1Compat): DynamoDBCodecDeriver                   =
    copy(schema1TupleCompat = v)
  def withSchema1ByteSequenceCompatibility(v: Schema1Compat): DynamoDBCodecDeriver            =
    copy(schema1ByteSequenceCompat = v)
  def withSchema1YearCompatibility(v: Schema1Compat): DynamoDBCodecDeriver                    =
    copy(schema1YearCompat = v)
  def withEnumValuesAsStrings(enumValuesAsStrings: Boolean): DynamoDBCodecDeriver             =
    copy(enumValuesAsStrings = enumValuesAsStrings)
  def withFieldNameMapper(fieldNameMapper: NameMapper): DynamoDBCodecDeriver                  = copy(fieldNameMapper = fieldNameMapper)
  def withCaseNameMapper(caseNameMapper: NameMapper): DynamoDBCodecDeriver                    = copy(caseNameMapper = caseNameMapper)
  def withTransientNone(transientNone: Boolean): DynamoDBCodecDeriver                         = copy(transientNone = transientNone)
  def withDiscriminatorKind(discriminatorKind: DiscriminatorKind): DynamoDBCodecDeriver       =
    copy(discriminatorKind = discriminatorKind)
  def withRequiredCollectionFields(requireCollectionFields: Boolean): DynamoDBCodecDeriver    =
    copy(requireCollectionFields = requireCollectionFields)
  def withTransientEmptyCollection(transientEmptyCollection: Boolean): DynamoDBCodecDeriver   =
    copy(transientEmptyCollection = transientEmptyCollection)
  def withTransientDefaultValue(transientDefaultValue: Boolean): DynamoDBCodecDeriver         =
    copy(transientDefaultValue = transientDefaultValue)
  def withRequireDefaultValueFields(requireDefaultValueFields: Boolean): DynamoDBCodecDeriver =
    copy(requireDefaultValueFields = requireDefaultValueFields)
  def withRejectExtraFields(rejectExtraFields: Boolean): DynamoDBCodecDeriver                 =
    copy(rejectExtraFields = rejectExtraFields)

  def copy(
    schema1TupleCompat: Schema1Compat = schema1TupleCompat,
    schema1ByteSequenceCompat: Schema1Compat = schema1ByteSequenceCompat,
    schema1YearCompat: Schema1Compat = schema1YearCompat,
    discriminatorKind: DiscriminatorKind = discriminatorKind,
    enumValuesAsStrings: Boolean = enumValuesAsStrings,
    fieldNameMapper: NameMapper = fieldNameMapper,
    caseNameMapper: NameMapper = caseNameMapper,
    transientNone: Boolean = transientNone,
    requireOptionFields: Boolean = requireOptionFields,
    transientEmptyCollection: Boolean = transientEmptyCollection,
    requireCollectionFields: Boolean = requireCollectionFields,
    transientDefaultValue: Boolean = transientDefaultValue,
    requireDefaultValueFields: Boolean = requireDefaultValueFields,
    rejectExtraFields: Boolean = rejectExtraFields
  ): DynamoDBCodecDeriver =
    new DynamoDBCodecDeriver(
      schema1TupleCompat,
      schema1ByteSequenceCompat,
      schema1YearCompat,
      discriminatorKind,
      enumValuesAsStrings,
      fieldNameMapper,
      caseNameMapper,
      transientNone,
      requireOptionFields,
      transientEmptyCollection,
      requireCollectionFields,
      transientDefaultValue,
      requireDefaultValueFields,
      rejectExtraFields
    )

  type Elem
  type Col[_]
  type Key
  type Value
  type Wrapped
  type Map[_, _]
  type TC[_]

  // Encodes a tuple as nested right-folded pairs — the schema1 / legacy format.
  // (a, b, c, d) → List(List(List(a, b), c), d)
  private[this] def encodeLegacyTuple(arr: Array[AttributeValue], len: Int): AttributeValue.List =
    if (len == 2)
      AttributeValue.List(scala.collection.immutable.ArraySeq(arr(0), arr(1)))
    else
      AttributeValue.List(scala.collection.immutable.ArraySeq(encodeLegacyTuple(arr, len - 1), arr(len - 1)))

  override def derivePrimitive[A](
    primitiveType: PrimitiveType[A],
    typeId: TypeId[A],
    binding: Binding.Primitive[A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  ): Lazy[DynamoDBCodec[A]] = {
    if (binding.isInstanceOf[Binding[?, ?]]) {
      Lazy(primitiveType match {
        case _: PrimitiveType.Unit.type      => unitCodec
        case _: PrimitiveType.Boolean        => booleanCodec
        case _: PrimitiveType.Byte           => byteCodec
        case _: PrimitiveType.Short          => shortCodec
        case _: PrimitiveType.Int            => intCodec
        case _: PrimitiveType.Long           => longCodec
        case _: PrimitiveType.Float          => floatCodec
        case _: PrimitiveType.Double         => doubleCodec
        case _: PrimitiveType.Char           => charCodec
        case _: PrimitiveType.String         => stringCodec
        case _: PrimitiveType.BigInt         => bigIntCodec
        case _: PrimitiveType.BigDecimal     => bigDecimalCodec
        case _: PrimitiveType.DayOfWeek      => dayOfWeekCodec
        case _: PrimitiveType.Duration       => durationCodec
        case _: PrimitiveType.Instant        => instantCodec
        case _: PrimitiveType.LocalDate      => localDateCodec
        case _: PrimitiveType.LocalDateTime  => localDateTimeCodec
        case _: PrimitiveType.LocalTime      => localTimeCodec
        case _: PrimitiveType.Month          => monthCodec
        case _: PrimitiveType.MonthDay       => monthDayCodec
        case _: PrimitiveType.OffsetDateTime => offsetDateTimeCodec
        case _: PrimitiveType.OffsetTime     => offsetTimeCodec
        case _: PrimitiveType.Period         => periodCodec
        case _: PrimitiveType.Year           => yearCodec
        case _: PrimitiveType.YearMonth      => yearMonthCodec
        case _: PrimitiveType.ZoneId         => zoneIdCodec
        case _: PrimitiveType.ZoneOffset     => zoneOffsetCodec
        case _: PrimitiveType.ZonedDateTime  => zonedDateTimeCodec
        case _: PrimitiveType.Currency       => currencyCodec
        case _: PrimitiveType.UUID           => uuidCodec
      })
    } else binding.asInstanceOf[BindingInstance[TC, ?, A]].instance
  }.asInstanceOf[Lazy[DynamoDBCodec[A]]]

  override def deriveRecord[F[_, _], A](
    fields: IndexedSeq[Term[F, A, _]],
    typeId: TypeId[A],
    binding: Binding.Record[A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[A]] = {
    if (binding.isInstanceOf[Binding[?, ?]]) {
      val recordBinding = binding.asInstanceOf[Binding.Record[A]]
      if (typeId.isTuple) Lazy {
        var offset                       = 0L
        var fieldInfos: Array[FieldInfo] = null // TODO: investigate recursive cache
        val len                          = fields.length
        val aliasMap                     = new java.util.HashMap[String, FieldInfo](len)
        if (fieldInfos eq null) {
          fieldInfos = new Array[FieldInfo](len)
          var idx = 0
          while (idx < len) {
            val field        = fields(idx)
            val fieldReflect = field.value
            val codec        = D.instance(fieldReflect.metadata).force
            val fieldInfo    = new FieldInfo(
              span = new DynamicOptic.Node.Field(field.name),
              defaultValue = None,
              emptyCollectionConstructor = null,
              offset = offset,
              isOptional = isOptional(fieldReflect)
            )
            fieldInfo.setCodec(codec)
            fieldInfos(idx) = fieldInfo
            var name: String = null
            field.modifiers.foreach {
              case m: Modifier.rename    => if (name eq null) name = m.name
              case m: Modifier.alias     => aliasMap.put(m.name, fieldInfo)
              case _: Modifier.transient => fieldInfo.nonTransient = false
              case _                     =>
            }
            if (name eq null) name = fieldNameMapper(field.name)
            aliasMap.put(name, fieldInfo)
            fieldInfo.setName(name)
            offset = RegisterOffset.add(registerOffset(fieldReflect), offset)
            idx += 1
          }
        }
        new DynamoDBCodec[A] {
          private[this] val deconstructor = recordBinding.deconstructor
          private[this] val constructor   = recordBinding.constructor
          private[this] val usedRegisters = offset

          override def encoder: Encoder[A] = { value =>
            val arr: Array[AttributeValue] = new Array[AttributeValue](len)
            val regs                       = Registers(usedRegisters)
            deconstructor.deconstruct(regs, 0, value)
            var idx                        = 0
            while (idx < len) {
              val field  = fieldInfos(idx)
              val offset = field.offset
              val codec  = field.getCodec
              field.getValueType match {
                case DynamoDBCodec.intType     =>
                  val v  = regs.getInt(offset)
                  val av = codec.asInstanceOf[DynamoDBCodec[Int]].encoder(v)
                  arr(idx) = av
                case DynamoDBCodec.longType    =>
                  val v  = regs.getLong(offset)
                  val av = codec.asInstanceOf[DynamoDBCodec[Long]].encoder(v)
                  arr(idx) = av
                case DynamoDBCodec.booleanType =>
                  val v  = regs.getBoolean(offset)
                  val av = codec.asInstanceOf[DynamoDBCodec[Boolean]].encoder(v)
                  arr(idx) = av
                case DynamoDBCodec.byteType    =>
                  val v  = regs.getByte(offset)
                  val av = codec.asInstanceOf[DynamoDBCodec[Byte]].encoder(v)
                  arr(idx) = av
                case DynamoDBCodec.charType    =>
                  val v  = regs.getChar(offset)
                  val av = codec.asInstanceOf[DynamoDBCodec[Char]].encoder(v)
                  arr(idx) = av
                case DynamoDBCodec.shortType   =>
                  val v  = regs.getShort(offset)
                  val av = codec.asInstanceOf[DynamoDBCodec[Short]].encoder(v)
                  arr(idx) = av
                case DynamoDBCodec.floatType   =>
                  val v  = regs.getFloat(offset)
                  val av = codec.asInstanceOf[DynamoDBCodec[Float]].encoder(v)
                  arr(idx) = av
                case DynamoDBCodec.doubleType  =>
                  val v  = regs.getDouble(offset)
                  val av = codec.asInstanceOf[DynamoDBCodec[Double]].encoder(v)
                  arr(idx) = av
                case DynamoDBCodec.unitType    =>
                  arr(idx) = AttributeValue.Map.empty
                case DynamoDBCodec.objectType  =>
                  val v  = regs.getObject(offset)
                  val av = codec.asInstanceOf[DynamoDBCodec[AnyRef]].encoder(v)
                  arr(idx) = av
                case _                         =>
                  val v  = regs.getObject(offset)
                  val av = codec.asInstanceOf[DynamoDBCodec[AnyRef]].encoder(v)
                  arr(idx) = av
              }
              idx += 1
            }
            if (schema1TupleCompat == Schema1Compat.ReadBothWriteOld) encodeLegacyTuple(arr, len)
            else AttributeValue.List(scala.collection.immutable.ArraySeq.unsafeWrapArray(arr))
          }

          override def decoder: Decoder[A] = { (av: AttributeValue) =>
            val regs                 = Registers(usedRegisters)
            var error: DecodingError = null

            def accumulate(e: DecodingError): Unit =
              error = if (error == null) e else error ++ e

            def setValue(field: FieldInfo, value: AttributeValue): Unit =
              field.getValueType match {
                case DynamoDBCodec.intType     =>
                  field.getCodec.asInstanceOf[DynamoDBCodec[Int]].decoder(value) match {
                    case Right(v)  => regs.setInt(field.offset, v)
                    case Left(err) => accumulate(err)
                  }
                case DynamoDBCodec.longType    =>
                  field.getCodec.asInstanceOf[DynamoDBCodec[Long]].decoder(value) match {
                    case Right(v)  => regs.setLong(field.offset, v)
                    case Left(err) => accumulate(err)
                  }
                case DynamoDBCodec.booleanType =>
                  field.getCodec.asInstanceOf[DynamoDBCodec[Boolean]].decoder(value) match {
                    case Right(v)  => regs.setBoolean(field.offset, v)
                    case Left(err) => accumulate(err)
                  }
                case DynamoDBCodec.byteType    =>
                  field.getCodec.asInstanceOf[DynamoDBCodec[Byte]].decoder(value) match {
                    case Right(v)  => regs.setByte(field.offset, v)
                    case Left(err) => accumulate(err)
                  }
                case DynamoDBCodec.charType    =>
                  field.getCodec.asInstanceOf[DynamoDBCodec[Char]].decoder(value) match {
                    case Right(v)  => regs.setChar(field.offset, v)
                    case Left(err) => accumulate(err)
                  }
                case DynamoDBCodec.shortType   =>
                  field.getCodec.asInstanceOf[DynamoDBCodec[Short]].decoder(value) match {
                    case Right(v)  => regs.setShort(field.offset, v)
                    case Left(err) => accumulate(err)
                  }
                case DynamoDBCodec.floatType   =>
                  field.getCodec.asInstanceOf[DynamoDBCodec[Float]].decoder(value) match {
                    case Right(v)  => regs.setFloat(field.offset, v)
                    case Left(err) => accumulate(err)
                  }
                case DynamoDBCodec.doubleType  =>
                  field.getCodec.asInstanceOf[DynamoDBCodec[Double]].decoder(value) match {
                    case Right(v)  => regs.setDouble(field.offset, v)
                    case Left(err) => accumulate(err)
                  }
                case DynamoDBCodec.unitType    => () // Unit fields occupy no register
                case DynamoDBCodec.objectType  =>
                  field.getCodec.asInstanceOf[DynamoDBCodec[AnyRef]].decoder(value) match {
                    case Right(v)  => regs.setObject(field.offset, v)
                    case Left(err) => accumulate(err)
                  }
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
                        setRegisterValueForLastElement(Chunk.fromIterable(l.value), count - 1)
                      case avFirst                                  =>
                        val field = fieldInfos(count - 1) // skip to first element in list
                        setValue(field, avFirst)
                    }
                  case _                            => accumulate(DecodingError.failure(s"Expected list size of 2 but found $len"))
                }
              }

              setRegisterValueForLastElement(Chunk.fromIterable(av.value), len - 1)

              if (error == null) {
                val a = constructor.construct(regs, RegisterOffset.Zero)
                Right(a)
              } else Left(error)

            }

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
                if (error == null) {
                  val a = constructor.construct(regs, RegisterOffset.Zero)
                  Right(a)
                } else if (schema1TupleCompat != Schema1Compat.ReadNewWriteNew) {
                  error = null
                  decodeLegacy(avList)
                } else
                  Left(error)
              case av: AttributeValue          =>
                Left(DecodingError(s"Expected List attribute value but got: ${av.showType}"))
            }
          }
        }
      }
      else
        Lazy { // Vanilla Record
          var offset                               = 0L
          val isRecursive                          = fields.exists(_.value.isInstanceOf[Reflect.Deferred[F, ?]])
          var fieldInfos: Array[FieldInfo]         =
            if (isRecursive) recursiveRecordCache.get.get(typeId)
            else null
          val deriveCodecs                         = fieldInfos eq null
          val len                                  = fields.length
          val aliasMap                             = new java.util.HashMap[String, FieldInfo](len)
          // @Modifier.fieldNaming overrides the deriver-level fieldNameMapper for this type.
          var effectiveFieldNameMapper: NameMapper = null
          modifiers.foreach {
            case m: Modifier.fieldNaming =>
              if (effectiveFieldNameMapper eq null) effectiveFieldNameMapper = NameMapper.fromString(m.strategy)
            case _                       =>
          }
          if (effectiveFieldNameMapper eq null) effectiveFieldNameMapper = fieldNameMapper
          if (deriveCodecs) {
            fieldInfos = new Array[FieldInfo](len)
            var idx = 0
            while (idx < len) {
              val field        = fields(idx)
              val fieldReflect = field.value
              val fieldInfo    = new FieldInfo(
                span = new DynamicOptic.Node.Field(field.name),
                defaultValue = getDefaultValue(fieldReflect),
                emptyCollectionConstructor = emptyCollectionConstructorFor(fieldReflect),
                offset = offset,
                isOptional = isOptional(fieldReflect),
                usesNullSentinel = fieldReflect.typeId.isMaybe
              )
              fieldInfos(idx) = fieldInfo
              var name: String = null
              field.modifiers.foreach {
                case m: Modifier.rename          => if (name eq null) name = m.name
                case m: Modifier.alias           => aliasMap.put(m.name, fieldInfo)
                case _: Modifier.transient       =>
                  fieldInfo.nonTransient = false
                  fieldInfo.nonEncodeTransient = false
                case _: Modifier.encodeTransient => fieldInfo.nonEncodeTransient = false
                case _                           =>
              }
              if (name eq null) name = effectiveFieldNameMapper(field.name)
              aliasMap.put(name, fieldInfo)
              fieldInfo.setName(name)

              offset = RegisterOffset.add(registerOffset(fieldReflect), offset)
              idx += 1
            }

            if (isRecursive) recursiveRecordCache.get.put(typeId, fieldInfos)
            discriminatorFields.set(null :: discriminatorFields.get)
          }
          var idx                                  = 0
          while (idx < len) {
            val field = fields(idx)
            if (deriveCodecs) {
              fieldInfos(idx).setCodec(D.instance(field.value.metadata).force)
            }
            idx += 1
          }
          if (deriveCodecs) discriminatorFields.set(discriminatorFields.get.tail)
          val rejectExtra                          = rejectExtraFields || modifiers.exists(_.isInstanceOf[Modifier.noExtraFields])

          new DynamoDBCodec[A] {
            override val recordFieldNames: Chunk[String]                        =
              Chunk.fromArray(fieldInfos.filter(_.nonTransient).map(_.getName))
            private[this] val constructor                                       = recordBinding.constructor
            private[this] val deconstructor                                     = recordBinding.deconstructor
            private[this] val usedRegisters                                     = offset
            private[this] val fields                                            = fieldInfos
            private[this] val skipNone                                          = transientNone
            private[this] val skipEmptyCollection                               = transientEmptyCollection
            private[this] val skipDefaultValue                                  = transientDefaultValue
            private[this] val discriminatorField                                = discriminatorFields.get.headOption.orNull
            // null when extra-field rejection is off — avoids capturing aliasMap in the default case
            private[this] val knownFields: java.util.HashMap[String, FieldInfo] =
              if (rejectExtra) aliasMap else null

            override def encoder: Encoder[A] = { value =>
              val regs       = Registers(usedRegisters)
              var idx        = 0
              val mapBuilder = JMapView.hash.builder[AttributeValue.String, AttributeValue]
              deconstructor.deconstruct(regs, 0, value)
              val len        = fields.length
              if (discriminatorField ne null) {
                val name  = discriminatorField.name
                val value = discriminatorField.value
                mapBuilder.addOne(AttributeValue.String(name), AttributeValue.String(value))
              }

              while (idx < len) {
                val field = fields(idx)
                if (field.nonTransient && field.nonEncodeTransient) {
                  val avName = AttributeValue.String(field.getName)
                  val offset = field.offset
                  val codec  = field.getCodec
                  val isOpt  = field.isOptional

                  if (skipDefaultValue && field.hasDefault && field.defaultEqualsValue(regs)) ()
                  else
                    field.getValueType match {
                      case DynamoDBCodec.intType     =>
                        val value = regs.getInt(offset)
                        val av    = codec.asInstanceOf[DynamoDBCodec[Int]].encoder(value)
                        mapBuilder.addOne(avName, av)
                      case DynamoDBCodec.longType    =>
                        val value = regs.getLong(offset)
                        val av    = codec.asInstanceOf[DynamoDBCodec[Long]].encoder(value)
                        mapBuilder.addOne(avName, av)
                      case DynamoDBCodec.booleanType =>
                        val value = regs.getBoolean(offset)
                        val av    = codec.asInstanceOf[DynamoDBCodec[Boolean]].encoder(value)
                        mapBuilder.addOne(avName, av)
                      case DynamoDBCodec.byteType    =>
                        val value = regs.getByte(offset)
                        val av    = codec.asInstanceOf[DynamoDBCodec[Byte]].encoder(value)
                        mapBuilder.addOne(avName, av)
                      case DynamoDBCodec.charType    =>
                        val value = regs.getChar(offset)
                        val av    = codec.asInstanceOf[DynamoDBCodec[Char]].encoder(value)
                        mapBuilder.addOne(avName, av)
                      case DynamoDBCodec.shortType   =>
                        val value = regs.getShort(offset)
                        val av    = codec.asInstanceOf[DynamoDBCodec[Short]].encoder(value)
                        mapBuilder.addOne(avName, av)
                      case DynamoDBCodec.floatType   =>
                        val value = regs.getFloat(offset)
                        val av    = codec.asInstanceOf[DynamoDBCodec[Float]].encoder(value)
                        mapBuilder.addOne(avName, av)
                      case DynamoDBCodec.doubleType  =>
                        val value = regs.getDouble(offset)
                        val av    = codec.asInstanceOf[DynamoDBCodec[Double]].encoder(value)
                        mapBuilder.addOne(avName, av)
                      case DynamoDBCodec.unitType    =>
                        mapBuilder.addOne(avName, AttributeValue.Map.empty)
                      case DynamoDBCodec.objectType  =>
                        val value = regs.getObject(offset)

                        if (
                          isOpt && skipNone && (if (field.usesNullSentinel) MaybeSupport.isAbsent(value)
                                                else value == None)
                        )
                          ()
                        else if (field.isCollection && skipEmptyCollection && isCollectionEmpty(value))
                          ()
                        else {
                          val av = codec.asInstanceOf[DynamoDBCodec[AnyRef]].encoder(value)
                          mapBuilder.addOne(avName, av)
                        }

                      case _ =>
                        val value = regs.getObject(offset)
                        val av    = codec.asInstanceOf[DynamoDBCodec[AnyRef]].encoder(value)
                        mapBuilder.addOne(avName, av)
                    }
                } // if nonTransient && nonEncodeTransient
                idx += 1
              }
              AttributeValue.Map(mapBuilder.result)
            }

            override def decoder: Decoder[A] = {
              val len = fields.length

              (av: AttributeValue) => {
                var idx                  = 0
                val regs                 = Registers(usedRegisters)
                var error: DecodingError = null

                def accumulate(e: DecodingError): Unit =
                  error = if (error == null) e else error ++ e

                av match {
                  case avMap: AttributeValue.Map =>
                    if (knownFields ne null) {
                      val mapIt = avMap.value.iterator
                      while (mapIt.hasNext) {
                        val (key, _) = mapIt.next()
                        val keyStr   = key.value
                        if (
                          !knownFields.containsKey(keyStr) &&
                          ((discriminatorField eq null) || discriminatorField.name != keyStr)
                        )
                          accumulate(DecodingError.failure(s"Unknown field '$keyStr'"))
                      }
                    }
                    while (idx < len) {
                      val field = fields(idx)
                      if (field.nonTransient) {
                        val offset = field.offset
                        val name   = field.getName
                        val av     = avMap.value.getOrElse(AttributeValue.String(name), null)

                        if (av eq null)
                          field.setMissingValueOrDefault(regs, accumulate)
                        else
                          field.getValueType match {
                            case DynamoDBCodec.intType     =>
                              field.getCodec.asInstanceOf[DynamoDBCodec[Int]].decoder(av) match {
                                case Right(value) => regs.setInt(offset, value)
                                case Left(err)    => accumulate(err)
                              }
                            case DynamoDBCodec.longType    =>
                              field.getCodec.asInstanceOf[DynamoDBCodec[Long]].decoder(av) match {
                                case Right(value) => regs.setLong(offset, value)
                                case Left(err)    => accumulate(err)
                              }
                            case DynamoDBCodec.booleanType =>
                              field.getCodec.asInstanceOf[DynamoDBCodec[Boolean]].decoder(av) match {
                                case Right(value) => regs.setBoolean(offset, value)
                                case Left(err)    => accumulate(err)
                              }
                            case DynamoDBCodec.byteType    =>
                              field.getCodec.asInstanceOf[DynamoDBCodec[Byte]].decoder(av) match {
                                case Right(value) => regs.setByte(offset, value)
                                case Left(err)    => accumulate(err)
                              }
                            case DynamoDBCodec.charType    =>
                              field.getCodec.asInstanceOf[DynamoDBCodec[Char]].decoder(av) match {
                                case Right(value) => regs.setChar(offset, value)
                                case Left(err)    => accumulate(err)
                              }
                            case DynamoDBCodec.shortType   =>
                              field.getCodec.asInstanceOf[DynamoDBCodec[Short]].decoder(av) match {
                                case Right(value) => regs.setShort(offset, value)
                                case Left(err)    => accumulate(err)
                              }
                            case DynamoDBCodec.floatType   =>
                              field.getCodec.asInstanceOf[DynamoDBCodec[Float]].decoder(av) match {
                                case Right(value) => regs.setFloat(offset, value)
                                case Left(err)    => accumulate(err)
                              }
                            case DynamoDBCodec.doubleType  =>
                              field.getCodec.asInstanceOf[DynamoDBCodec[Double]].decoder(av) match {
                                case Right(value) => regs.setDouble(offset, value)
                                case Left(err)    => accumulate(err)
                              }
                            case DynamoDBCodec.unitType    => () // Unit fields occupy no register
                            case DynamoDBCodec.objectType  =>
                              field.getCodec.asInstanceOf[DynamoDBCodec[AnyRef]].decoder(av) match {
                                case Right(value) => regs.setObject(offset, value)
                                case Left(err)    => accumulate(err)
                              }
                          }
                      } // if nonTransient
                      idx += 1
                    } // end while
                    if (error == null) {
                      val a = constructor.construct(regs, RegisterOffset.Zero)
                      Right(a)
                    } else Left(error)

                  case av: AttributeValue =>
                    Left(DecodingError(s"Expected Map attribute value but got: ${av.showType}"))
                }
              }
            }
          } // end if else not tuple
        }

    } else binding.asInstanceOf[BindingInstance[TC, ?, A]].instance
  }.asInstanceOf[Lazy[DynamoDBCodec[A]]]

  override def deriveVariant[F[_, _], A](
    cases: IndexedSeq[Term[F, A, _]],
    typeId: TypeId[A],
    binding: Binding.Variant[A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[A]] = {
    if (binding.isInstanceOf[Binding[?, ?]]) {
      if (typeId.isOption) {
        D.instance(cases(1).value.asRecord.get.fields(0).value.metadata).map { codec =>
          new DynamoDBCodec[Option[Any]]() {
            private[this] val valueCodec = codec.asInstanceOf[DynamoDBCodec[Any]]

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
          }
        }
      } else if (typeId.isMaybe) {
        D.instance(cases(1).value.asRecord.get.fields(0).value.metadata).map { codec =>
          new DynamoDBCodec[AnyRef]() {
            private[this] val valueCodec = codec.asInstanceOf[DynamoDBCodec[AnyRef]]

            override def encoder: Encoder[AnyRef] = { value =>
              if (MaybeSupport.isAbsent(value)) AttributeValue.Null
              else valueCodec.encoder(MaybeSupport.innerValue(value))
            }

            override def decoder: Decoder[AnyRef] = {
              case AttributeValue.Null =>
                Right(MaybeSupport.absent)
              case av                  =>
                valueCodec.decoder(av) match {
                  case Right(inner) => Right(MaybeSupport.present(inner))
                  case Left(err)    => Left(err)
                }
            }
          }
        }
      } else {
        val discr                                        = binding.asInstanceOf[Binding.Variant[A]].discriminator
        var resolvedCaseNameMapper: NameMapper           = null
        var resolvedDiscriminatorKind: DiscriminatorKind = null
        modifiers.foreach {
          case m: Modifier.caseNaming    =>
            if (resolvedCaseNameMapper eq null) resolvedCaseNameMapper = NameMapper.fromString(m.strategy)
          case m: Modifier.discriminator =>
            if (resolvedDiscriminatorKind eq null) resolvedDiscriminatorKind = DiscriminatorKind.Field(m.name)
          case _                         =>
        }
        if (resolvedCaseNameMapper eq null) resolvedCaseNameMapper = caseNameMapper
        if (resolvedDiscriminatorKind eq null) resolvedDiscriminatorKind = discriminatorKind
        if (isEnumeration(cases)) Lazy {
          val map = new java.util.HashMap[String, Constructor[?]](cases.length)

          def getInfos(cases: IndexedSeq[Term[F, A, ?]]): Array[EnumInfo] = {
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
                    getInfos(caseReflect.asVariant.get.asInstanceOf[Reflect.Variant[F, A]].cases)
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
                  if (name eq null) name = resolvedCaseNameMapper(case_.name)
                  map.put(name, constructor)
                  new EnumLeafInfo(name, constructor)
                }
              idx += 1
            }
            infos
          }

          val enumInfos = getInfos(cases)

          new DynamoDBCodec[A]() {
            private[this] val root           = EnumNodeInfo(discr, enumInfos)
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
                  Left(ItemError.DecodingError.failure(s"unknown enum value '$name'"))
                else {
                  val a: A = constructor.construct(null, 0).asInstanceOf[A]
                  Right(a)
                }
              case av                          =>
                Left(ItemError.DecodingError(s"expected String for enum, got ${av.showType}"))
            }
          }
        }
        else
          resolvedDiscriminatorKind match {

            case DiscriminatorKind.Field(fieldName) if hasOnlyRecordAndVariantCases(cases) =>
              Lazy {
                val map = new java.util.HashMap[String, CaseLeafInfo](cases.length)

                def getInfos(cases: IndexedSeq[Term[F, A, ?]], spans: List[DynamicOptic.Node.Case]): Array[CaseInfo] = {
                  val len   = cases.length
                  val infos = new Array[CaseInfo](len)
                  var idx   = 0
                  while (idx < len) {
                    val case_       = cases(idx)
                    val caseReflect = case_.value
                    val span        = new DynamicOptic.Node.Case(case_.name)
                    infos(idx) = if (caseReflect.isVariant) {
                      val caseVariant = caseReflect.asVariant.get.asInstanceOf[Reflect.Variant[F, A]]
                      new CaseNodeInfo(discriminator(caseReflect), getInfos(caseVariant.cases, span :: spans))
                    } else {
                      val caseLeafInfo = new CaseLeafInfo(null, span :: spans)
                      var name: String = null
                      case_.modifiers.foreach {
                        case m: Modifier.rename => if (name eq null) name = m.name
                        case m: Modifier.alias  => map.put(m.name, caseLeafInfo)
                        case _                  =>
                      }
                      if (name eq null) name = resolvedCaseNameMapper(case_.name)
                      map.put(name, caseLeafInfo)
                      discriminatorFields.set(new DiscriminatorFieldInfo(fieldName, name) :: discriminatorFields.get)
                      caseLeafInfo.codec = D.instance(caseReflect.metadata).force
                      discriminatorFields.set(discriminatorFields.get.tail)
                      caseLeafInfo
                    }
                    idx += 1
                  }
                  infos
                }

                new DynamoDBCodec[A]() {
                  private[this] val root                   = new CaseNodeInfo(discr, getInfos(cases, Nil))
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
                          case Some(other)                                     =>
                            Left(
                              ItemError.DecodingError(
                                s"Expected discriminator field '$fieldName' to be an AttributeValue.String but found ${other.showType}"
                              )
                            )
                          case None                                            =>
                            Left(ItemError.DecodingError(s"Missing discriminator field '$fieldName'"))
                        }

                      case av =>
                        Left(ItemError.DecodingError(s"Expected an AttributeValue.Map but found ${av.showType}"))
                    }

                  }.asInstanceOf[Decoder[A]]
                }
              }

            case DiscriminatorKind.None =>
              Lazy {
                val codecs = Array.newBuilder[DynamoDBCodec[?]]

                def getInfos(cases: IndexedSeq[Term[F, A, ?]]): Array[CaseInfo] = {
                  val len   = cases.length
                  val infos = new Array[CaseInfo](len)
                  var idx   = 0
                  while (idx < len) {
                    val caseReflect = cases(idx).value
                    infos(idx) = if (caseReflect.isVariant) {
                      val caseVariant = caseReflect.asVariant.get.asInstanceOf[Reflect.Variant[F, A]]
                      new CaseNodeInfo(discriminator(caseReflect), getInfos(caseVariant.cases))
                    } else {
                      val codec = D.instance(caseReflect.metadata).force
                      codecs.addOne(codec)
                      new CaseLeafInfo(codec, Nil)
                    }
                    idx += 1
                  }
                  infos
                }

                new DynamoDBCodec[A]() {
                  private[this] val root           = new CaseNodeInfo(discr, getInfos(cases))
                  private[this] val caseLeafCodecs = codecs.result()

                  override def encoder: Encoder[A] =
                    (a: A) => root.discriminate(a).codec.asInstanceOf[DynamoDBCodec[A]].encoder(a)

                  override def decoder: Decoder[A] =
                    (av: AttributeValue) => {
                      var idx                                      = 0
                      var rtrn: Either[ItemError.DecodingError, A] = null
                      while (idx < caseLeafCodecs.length && (rtrn eq null)) {
                        val codec = caseLeafCodecs(idx).asInstanceOf[DynamoDBCodec[A]]
                        val x     = codec.decoder(av)
                        if (x.isRight)
                          rtrn = x
                        idx += 1
                      }

                      if (rtrn eq null)
                        Left(ItemError.DecodingError("All sub type decoders failed for AttributeValue.Map"))
                      else
                        rtrn
                    }
                }
              }
            // DiscriminatorKind.Key
            case _                      =>
              Lazy {
                val map = new java.util.HashMap[String, CaseLeafInfo](cases.length)

                def getInfos(cases: IndexedSeq[Term[F, A, ?]], spans: List[DynamicOptic.Node.Case]): Array[CaseInfo] = {
                  val len   = cases.length
                  val infos = new Array[CaseInfo](len)
                  var idx   = 0
                  while (idx < len) {
                    val case_       = cases(idx)
                    val caseReflect = case_.value
                    val span        = new DynamicOptic.Node.Case(case_.name)
                    infos(idx) = if (caseReflect.isVariant) {
                      val caseVariant = caseReflect.asVariant.get.asInstanceOf[Reflect.Variant[F, A]]
                      new CaseNodeInfo(discriminator(caseReflect), getInfos(caseVariant.cases, span :: spans))
                    } else {
                      val caseLeafInfo =
                        new CaseLeafInfo(D.instance(caseReflect.metadata).force, span :: spans) // Stackoverflow (2)
                      var name: String = null
                      case_.modifiers.foreach {
                        case m: Modifier.rename => if (name eq null) name = m.name
                        case m: Modifier.alias  => map.put(m.name, caseLeafInfo)
                        case _                  =>
                      }
                      if (name eq null) name = resolvedCaseNameMapper(case_.name)
                      map.put(name, caseLeafInfo)
                      caseLeafInfo.setName(name)
                      caseLeafInfo
                    }
                    idx += 1
                  }
                  infos
                }

                new DynamoDBCodec[A]() {
                  private[this] val root    = new CaseNodeInfo(discr, getInfos(cases, Nil)) // Stackoverflow (3)
                  private[this] val caseMap = map

                  override def encoder: Encoder[A] =
                    (a: A) => {
                      val caseInfo = root.discriminate(a)
                      val av       = caseInfo.codec.asInstanceOf[DynamoDBCodec[A]].encoder(a)
                      AttributeValue.Map(JMapView.hash.single(AttributeValue.String(caseInfo.getName), av))
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
      }
    } else binding.asInstanceOf[BindingInstance[TC, ?, A]].instance
  }.asInstanceOf[Lazy[DynamoDBCodec[A]]]

  override def deriveSequence[F[_, _], C[_], A](
    element: Reflect[F, A],
    typeId: TypeId[C[A]],
    binding: Binding.Seq[C, A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[C[A]],
    examples: Seq[C[A]]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[C[A]]] = {
    if (binding.isInstanceOf[Binding[?, ?]]) {
      val seqBinding = binding.asInstanceOf[Binding.Seq[Col, Elem]]
      D.instance(element.metadata).map { elementCodec =>
        val constructor   = seqBinding.constructor
        val deconstructor = seqBinding.deconstructor
        val elemClassTag  = element.typeId.classTag.asInstanceOf[ClassTag[Elem]]
        val isSet         = typeId.name == "Set" && typeId.owner == TypeId.set.owner

        // Native DynamoDB set types: Set[String] → SS, Set[numeric] → NS, Set[byte-seq] → BS.
        // These branches must come before the byteType/generic-List branches so that e.g.
        // Set[Byte] is encoded as NumberSet rather than being mistaken for a byte-sequence.
        if (
          isSet && element.isPrimitive && element.asPrimitive.exists(_.primitiveType.isInstanceOf[PrimitiveType.String])
        ) {
          // Set[String] → AttributeValue.StringSet
          val codec = elementCodec.asInstanceOf[DynamoDBCodec[Elem]]
          new DynamoDBCodec[Col[Elem]] {
            override def encoder: Encoder[Col[Elem]] = { col =>
              val it = deconstructor.deconstruct(col)
              val sb = Set.newBuilder[String]
              while (it.hasNext)
                codec.encoder(it.next()) match {
                  case AttributeValue.String(s) => sb.addOne(s)
                  case _                        =>
                }
              AttributeValue.StringSet(sb.result())
            }

            override def decoder: Decoder[Col[Elem]] = {
              case AttributeValue.StringSet(strings) =>
                val builder = constructor.newBuilder[Elem](strings.size)(elemClassTag)
                val errors  = new ArrayBuffer[DecodingError.Cause]
                strings.foreach { s =>
                  codec.decoder(AttributeValue.String(s)) match {
                    case Right(v)  => constructor.add(builder, v.asInstanceOf[Elem])
                    case Left(err) => errors.addAll(err.causes)
                  }
                }
                if (errors.isEmpty) Right(constructor.result[Elem](builder))
                else Left(ItemError.DecodingError(errors.toArray))
              case av                                =>
                Left(ItemError.DecodingError(s"Expected StringSet but found ${av.showType}"))
            }
          }
        } else if (
          isSet && element.isPrimitive && element.asPrimitive.exists { p =>
            // Set[Byte|Short|Int|Long|Float|Double|BigDecimal|BigInt] → AttributeValue.NumberSet
            val pt = p.primitiveType
            pt.isInstanceOf[PrimitiveType.Byte] ||
            pt.isInstanceOf[PrimitiveType.Short] ||
            pt.isInstanceOf[PrimitiveType.Int] ||
            pt.isInstanceOf[PrimitiveType.Long] ||
            pt.isInstanceOf[PrimitiveType.Float] ||
            pt.isInstanceOf[PrimitiveType.Double] ||
            pt.isInstanceOf[PrimitiveType.BigDecimal] ||
            pt.isInstanceOf[PrimitiveType.BigInt]
          }
        ) {
          val codec = elementCodec.asInstanceOf[DynamoDBCodec[Elem]]
          new DynamoDBCodec[Col[Elem]] {
            override def encoder: Encoder[Col[Elem]] = { col =>
              val it = deconstructor.deconstruct(col)
              val sb = Set.newBuilder[BigDecimal]
              while (it.hasNext)
                codec.encoder(it.next()) match {
                  case AttributeValue.Number(bd) => sb.addOne(bd)
                  case _                         =>
                }
              AttributeValue.NumberSet(sb.result())
            }

            override def decoder: Decoder[Col[Elem]] = {
              case AttributeValue.NumberSet(numbers) =>
                val builder = constructor.newBuilder[Elem](numbers.size)(elemClassTag)
                val errors  = new ArrayBuffer[DecodingError.Cause]
                numbers.foreach { bd =>
                  codec.decoder(AttributeValue.Number(bd)) match {
                    case Right(v)  => constructor.add(builder, v.asInstanceOf[Elem])
                    case Left(err) => errors.addAll(err.causes)
                  }
                }
                if (errors.isEmpty) Right(constructor.result[Elem](builder))
                else Left(ItemError.DecodingError(errors.toArray))
              case av                                =>
                Left(ItemError.DecodingError(s"Expected NumberSet but found ${av.showType}"))
            }
          }
        } else if (
          isSet && element.isSequence && element.asSequenceUnknown.exists { u =>
            // Set[C[Byte]] where C is any byte-sequence collection (Chunk, Array, List, …)
            // → AttributeValue.BinarySet. The element codec must already produce Binary.
            val inner = u.sequence.element
            inner.isPrimitive && inner.asPrimitive.exists(_.primitiveType.isInstanceOf[PrimitiveType.Byte])
          }
        ) {
          val codec = elementCodec.asInstanceOf[DynamoDBCodec[Elem]]
          new DynamoDBCodec[Col[Elem]] {
            override def encoder: Encoder[Col[Elem]] = { col =>
              val it  = deconstructor.deconstruct(col)
              val buf = new ArrayBuffer[Iterable[Byte]](8)
              while (it.hasNext)
                codec.encoder(it.next()) match {
                  case AttributeValue.Binary(bytes) =>
                    buf.addOne(scala.collection.immutable.ArraySeq.unsafeWrapArray(bytes))
                  case other                        =>
                    // DynamoDB's BinarySet (BS) can only hold raw binary values — there's no
                    // "BinarySet of List[Number]" wire representation. withSchema1ByteSequenceCompatibility
                    // (ReadBothWriteOld) makes the element codec encode to AttributeValue.List instead of
                    // Binary; silently dropping those elements here would corrupt the set rather than
                    // surface the incompatibility, so fail loudly instead.
                    throw new IllegalStateException(
                      s"Cannot encode a Set of byte-sequences to BinarySet: element codec produced " +
                        s"${other.showType} instead of Binary. This combination isn't supported — " +
                        s"check withSchema1ByteSequenceCompatibility(ReadBothWriteOld), which changes " +
                        s"individual byte-sequences to encode as AttributeValue.List, not Binary."
                    )
                }
              AttributeValue.BinarySet(buf)
            }

            override def decoder: Decoder[Col[Elem]] = {
              case AttributeValue.BinarySet(binaryItems) =>
                val builder = constructor.newBuilder[Elem](8)(elemClassTag)
                val errors  = new ArrayBuffer[DecodingError.Cause]
                binaryItems.foreach { bytes =>
                  codec.decoder(AttributeValue.Binary(bytes)) match {
                    case Right(v)  => constructor.add(builder, v.asInstanceOf[Elem])
                    case Left(err) => errors.addAll(err.causes)
                  }
                }
                if (errors.isEmpty) Right(constructor.result[Elem](builder))
                else Left(ItemError.DecodingError(errors.toArray))
              case av                                    =>
                Left(ItemError.DecodingError(s"Expected BinarySet but found ${av.showType}"))
            }
          }
        } else if (elementCodec.valueType == DynamoDBCodec.byteType) {
          // Byte-sequence path. Shape depends on schema1ByteSequenceCompat:
          //   ReadNewWriteNew  — encode Binary,      decode Binary only         (default)
          //   ReadBothWriteNew — encode Binary,      decode Binary + List[Number] fallback
          //   ReadBothWriteOld — encode List[Number], decode both               (schema1 format)
          val elemClassTag      = element.typeId.classTag.asInstanceOf[ClassTag[Byte]]
          // TypeId[Array[_]].name == "Array"; used to take the zero-copy return path for Array[Byte].
          val isArrayByteTarget = typeId.name == "Array"

          def encodeBinary(col: Col[Elem]): AttributeValue =
            col match {
              case arr: Array[Byte] => AttributeValue.Binary(arr) // zero copy
              case _                =>
                val len = deconstructor.size(col)
                val arr = new Array[Byte](len)
                val it  = deconstructor.deconstruct(col).asInstanceOf[Iterator[Byte]]
                var i   = 0
                while (it.hasNext) { arr(i) = it.next(); i += 1 }
                AttributeValue.Binary(arr)
            }

          def decodeBinary(bytes: Array[Byte]): Either[ItemError.DecodingError, Col[Elem]] =
            if (isArrayByteTarget)
              Right(bytes.asInstanceOf[Col[Elem]]) // zero copy — return stored byte[] directly
            else {
              val builder = constructor.newBuilder[Elem](bytes.length)(elemClassTag.asInstanceOf[ClassTag[Elem]])
              var i       = 0
              while (i < bytes.length) {
                constructor.addByte(builder.asInstanceOf[constructor.Builder[Byte]], bytes(i))
                i += 1
              }
              Right(constructor.result[Elem](builder))
            }

          def decodeList(items: Iterable[AttributeValue]): Either[ItemError.DecodingError, Col[Elem]] = {
            val builder = constructor.newBuilder[Elem](8)(elemClassTag.asInstanceOf[ClassTag[Elem]])
            val errors  = new ArrayBuffer[DecodingError.Cause]
            items.foreach {
              case AttributeValue.Number(bd) =>
                constructor.addByte(builder.asInstanceOf[constructor.Builder[Byte]], bd.byteValue)
              case av                        =>
                errors.addOne(DecodingError.Failure(s"expected Number, got ${av.showType}"))
            }
            if (errors.isEmpty) Right(constructor.result[Elem](builder))
            else Left(ItemError.DecodingError(errors.toArray))
          }

          schema1ByteSequenceCompat match {
            case Schema1Compat.ReadNewWriteNew  =>
              new DynamoDBCodec[Col[Elem]] {
                override def encoder: Encoder[Col[Elem]] = encodeBinary(_)
                override def decoder: Decoder[Col[Elem]] = {
                  case AttributeValue.Binary(bytes) => decodeBinary(bytes)
                  case av                           => Left(ItemError.DecodingError(s"unable to decode ${av.showType} as Binary"))
                }
              }
            case Schema1Compat.ReadBothWriteNew =>
              new DynamoDBCodec[Col[Elem]] {
                override def encoder: Encoder[Col[Elem]] = encodeBinary(_)
                override def decoder: Decoder[Col[Elem]] = {
                  case AttributeValue.Binary(bytes) => decodeBinary(bytes)
                  case AttributeValue.List(items)   => decodeList(items)
                  case av                           => Left(ItemError.DecodingError(s"unable to decode ${av.showType} as Binary or List"))
                }
              }
            case Schema1Compat.ReadBothWriteOld =>
              new DynamoDBCodec[Col[Elem]] {
                override def encoder: Encoder[Col[Elem]] = { (col: Col[Elem]) =>
                  val len = deconstructor.size(col)
                  val avs = new Array[AttributeValue](len)
                  var idx = 0
                  val it  = deconstructor.deconstruct(col).asInstanceOf[Iterator[Byte]]
                  while (it.hasNext) {
                    avs(idx) = AttributeValue.Number(BigDecimal.valueOf(it.next().toLong))
                    idx += 1
                  }
                  AttributeValue.List(scala.collection.immutable.ArraySeq.unsafeWrapArray(avs))
                }
                override def decoder: Decoder[Col[Elem]] = {
                  case AttributeValue.List(items)   => decodeList(items)
                  case AttributeValue.Binary(bytes) => decodeBinary(bytes)
                  case av                           => Left(ItemError.DecodingError(s"unable to decode ${av.showType} as List or Binary"))
                }
              }
          }
        } else {
          new DynamoDBCodec[Col[Elem]] {
            override def encoder: Encoder[Col[Elem]] = { (col: Col[Elem]) =>
              val len = deconstructor.size(col)
              val avs = new Array[AttributeValue](len)
              var idx = 0
              val it  = deconstructor.deconstruct(col)
              while (it.hasNext) {
                val el = it.next()
                val av = elementCodec.asInstanceOf[DynamoDBCodec[Elem]].encoder(el)
                avs(idx) = av
                idx += 1
              }
              AttributeValue.List(scala.collection.immutable.ArraySeq.unsafeWrapArray(avs))
            }

            private[this] val elemClassTag = element.typeId.classTag.asInstanceOf[ClassTag[Elem]]

            override def decoder: Decoder[Col[Elem]] = { (av: AttributeValue) =>
              val errors = new ArrayBuffer[DecodingError.Cause]
              av match {
                case AttributeValue.List(items) =>
                  val builder = constructor.newBuilder[Elem](8)(elemClassTag)

                  items.foreach { item =>
                    elementCodec.decoder(item) match {
                      case Right(a)  => constructor.add(builder, a.asInstanceOf[Elem])
                      case Left(err) => errors.addAll(err.causes)
                    }
                  }
                  if (errors.isEmpty) {
                    val xs: Col[Elem] = constructor.result[Elem](builder)
                    Right(xs)
                  } else
                    Left(ItemError.DecodingError(errors.toArray))
                case _                          =>
                  Left(ItemError.DecodingError(s"unable to decode ${av.showType} as a list"))
              }
            }
          }
        }
      }
    } else binding.asInstanceOf[BindingInstance[TC, ?, A]].instance
  }.asInstanceOf[Lazy[DynamoDBCodec[C[A]]]]

  override def deriveMap[F[_, _], M[_, _], K, V](
    key: Reflect[F, K],
    value: Reflect[F, V],
    typeId: TypeId[M[K, V]],
    binding: Binding.Map[M, K, V],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[M[K, V]],
    examples: Seq[M[K, V]]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[M[K, V]]] = {
    if (binding.isInstanceOf[Binding[?, ?]]) {
      val mapBinding = binding.asInstanceOf[Binding.Map[Map, Key, Value]]
      D.instance(key.metadata).zip(D.instance(value.metadata)).map { case (codec1, codec2) =>
        val constructor   = mapBinding.constructor
        val deconstructor = mapBinding.deconstructor
        val keyCodec      = codec1.asInstanceOf[DynamoDBCodec[Key]]
        val keyEncoder    = keyCodec.encoder
        val keyDecoder    = keyCodec.decoder.asInstanceOf[Any => Either[ItemError.DecodingError, Key]]
        val valueCodec    = codec2.asInstanceOf[DynamoDBCodec[Value]]
        val valueEncoder  = valueCodec.encoder
        val valueDecoder  = valueCodec.decoder
        val isNativeMap   =
          if (key.isPrimitive)
            key.asPrimitive.get.typeId.name == "String"
          else
            false

        if (isNativeMap)
          new DynamoDBCodec[Map[Key, Value]] {
            override def encoder: Encoder[Map[Key, Value]] =
              (m: Map[Key, Value]) => {
                val mapBuilder = JMapView.hash.builder[AttributeValue.String, AttributeValue]
                val it         = deconstructor.deconstruct(m)
                while (it.hasNext) {
                  val kv           = it.next()
                  val key          = deconstructor.getKey(kv)
                  val value: Value = deconstructor.getValue(kv)
                  val keyVal       = keyEncoder.asInstanceOf[Key => AttributeValue.String](key)
                  mapBuilder.addOne(keyVal, valueEncoder(value))
                }
                AttributeValue.Map(mapBuilder.result)
              }

            override def decoder: Decoder[Map[Key, Value]] =
              (av: AttributeValue) =>
                if (!av.isInstanceOf[AttributeValue.Map])
                  Left(
                    ItemError.DecodingError(
                      s"Expected AttributeValue.Map, found ${if (av == null) "null" else av.showType}"
                    )
                  )
                else {
                  val errors  = new ArrayBuffer[DecodingError.Cause]
                  val map     = av.asInstanceOf[AttributeValue.Map]
                  val builder = constructor.newObjectBuilder[Key, Value](8)
                  val it      = map.value.iterator
                  while (it.hasNext) {
                    val (k, v) = it.next()
                    (keyDecoder(k), valueDecoder(v)) match {
                      case (Right(key), Right(value)) =>
                        constructor.addObject(builder, key, value)
                      case (Left(errL), Left(errR))   =>
                        errors.addAll(errL.causes)
                        errors.addAll(errR.causes)
                      case (_, Left(err))             => errors.addAll(err.causes)
                      case (Left(err), _)             => errors.addAll(err.causes)
                    }
                  }
                  if (errors.isEmpty) {
                    val m = constructor.resultObject[Key, Value](builder)
                    Right(m)
                  } else Left(ItemError.DecodingError(errors.toArray))
                }
          }
        else // non native Map encoding - Sequence of tuple2
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
                val errors  = new ArrayBuffer[DecodingError.Cause]
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
                          errors.addAll(errL.causes)
                          errors.addAll(errR.causes)
                        case (_, Left(err))             => errors.addAll(err.causes)
                        case (Left(err), _)             => errors.addAll(err.causes)
                      }
                    case other                                             =>
                      errors.addOne(
                        DecodingError.Failure(
                          s"Expected AttributeValue.List of size 2 for Map entry, found: ${other.showType}"
                        )
                      )
                  }
                }

                if (errors.isEmpty) {
                  val m = constructor.resultObject[Key, Value](builder)
                  Right(m)
                } else Left(ItemError.DecodingError(errors.toArray))

              case av => Left(ItemError.DecodingError(s"Expected AttributeValue.List, found ${av.showType}"))

            }

          }
      }
    } else binding.asInstanceOf[BindingInstance[TC, ?, ?]].instance
  }.asInstanceOf[Lazy[DynamoDBCodec[M[K, V]]]]

  override def deriveDynamic[F[_, _]](
    binding: Binding.Dynamic,
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[DynamicValue],
    examples: Seq[DynamicValue]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[DynamicValue]] = {
    if (binding.isInstanceOf[Binding[?, ?]]) Lazy(dynamicValueCodec)
    else binding.asInstanceOf[BindingInstance[TC, ?, ?]].instance
  }.asInstanceOf[Lazy[DynamoDBCodec[DynamicValue]]]

  override def deriveWrapper[F[_, _], A, B](
    wrapped: Reflect[F, B],
    typeId: TypeId[A],
    binding: Binding.Wrapper[A, B],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[DynamoDBCodec[A]] = {
    if (binding.isInstanceOf[Binding[?, ?]]) {
      val wrapperBinding = binding.asInstanceOf[Binding.Wrapper[A, Wrapped]]
      D.instance(wrapped.metadata).map { codec =>
        // valueType must match the wrapped codec's register kind (int/long/object/etc), not
        // the objectType default — a record field of this wrapper type is stored in whichever
        // register registerOffset()/ZB's own Binding.Record decided for the wrapped primitive
        // (see unwrapToPrimitiveTypeOption above), and the record-level encoder/decoder use
        // this valueType to pick regs.getInt/getLong/getObject/etc at that same offset.
        new DynamoDBCodec[A](codec.valueType) {
          private[this] val unwrap                               = wrapperBinding.unwrap
          private[this] val wrap                                 = wrapperBinding.wrap
          private[this] val wrappedCodec: DynamoDBCodec[Wrapped] = codec.asInstanceOf[DynamoDBCodec[Wrapped]]

          override def encoder: Encoder[A] = (a: A) => wrappedCodec.encoder(unwrap(a))

          override def decoder: Decoder[A] =
            (av: AttributeValue) => {
              val x: Either[ItemError.DecodingError, Wrapped] = wrappedCodec.decoder(av)
              x match {
                case Right(w) =>
                  val a: A = wrap(w)
                  Right(a)
                case Left(e)  => Left(e)
              }
            }
        }
      }
    } else binding.asInstanceOf[BindingInstance[TC, ?, A]].instance
  }.asInstanceOf[Lazy[DynamoDBCodec[A]]]

  override def instanceOverrides: IndexedSeq[InstanceOverride] = {
    recursiveRecordCache.remove()
    super.instanceOverrides :+ InstanceOverrideByType[DynamoDBCodec, Json](TypeId.of[Json], Lazy(jsonAVCodec))
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

  private[this] val unitCodec: DynamoDBCodec[Unit] =
    new DynamoDBCodec[Unit](valueType = DynamoDBCodec.unitType) {
      override def encoder: Encoder[Unit] = _ => AttributeValue.Map.empty
      override def decoder: Decoder[Unit] = {
        case m: AttributeValue.Map if m.value.isEmpty => Right(())
        case av                                       => Left(DecodingError(s"Expected empty Map for Unit but got: ${av.showType}"))
      }
    }

  private[this] val booleanCodec: DynamoDBCodec[Boolean] =
    new DynamoDBCodec[Boolean](valueType = DynamoDBCodec.booleanType) {
      override def encoder: Encoder[Boolean] = b => AttributeValue.Bool(b)
      override def decoder: Decoder[Boolean] = {
        case AttributeValue.Bool(b) => Right(b)
        case av                     => Left(DecodingError(s"Expected Bool attribute value but got: ${av.showType}"))
      }
    }

  private[this] val byteCodec: DynamoDBCodec[Byte] =
    new DynamoDBCodec[Byte](valueType = DynamoDBCodec.byteType) {
      override def encoder: Encoder[Byte] = b => AttributeValue.Number(BigDecimal.valueOf(b.toLong))
      override def decoder: Decoder[Byte] = {
        case AttributeValue.Number(bd) => Right(bd.byteValue)
        case av                        =>
          Left(DecodingError(s"Error getting byte value. Expected AttributeValue.Number but found ${av.showType}"))
      }
    }

  private[this] val shortCodec: DynamoDBCodec[Short] =
    new DynamoDBCodec[Short](valueType = DynamoDBCodec.shortType) {
      override def encoder: Encoder[Short] = s => AttributeValue.Number(BigDecimal.valueOf(s.toLong))
      override def decoder: Decoder[Short] = {
        case AttributeValue.Number(bd) => Right(bd.shortValue)
        case av                        =>
          Left(DecodingError(s"Error getting short value. Expected AttributeValue.Number but found ${av.showType}"))
      }
    }

  private[this] val floatCodec: DynamoDBCodec[Float] =
    new DynamoDBCodec[Float](valueType = DynamoDBCodec.floatType) {
      override def encoder: Encoder[Float] = f => AttributeValue.Number(BigDecimal.valueOf(f.toDouble))
      override def decoder: Decoder[Float] = {
        case AttributeValue.Number(bd) => Right(bd.floatValue)
        case av                        =>
          Left(DecodingError(s"Error getting float value. Expected AttributeValue.Number but found ${av.showType}"))
      }
    }

  private[this] val doubleCodec: DynamoDBCodec[Double] =
    new DynamoDBCodec[Double](valueType = DynamoDBCodec.doubleType) {
      override def encoder: Encoder[Double] = d => AttributeValue.Number(BigDecimal.valueOf(d))
      override def decoder: Decoder[Double] = {
        case AttributeValue.Number(bd) => Right(bd.doubleValue)
        case av                        =>
          Left(DecodingError(s"Error getting double value. Expected AttributeValue.Number but found ${av.showType}"))
      }
    }

  private[this] val charCodec: DynamoDBCodec[Char] =
    new DynamoDBCodec[Char](valueType = DynamoDBCodec.charType) {
      override def encoder: Encoder[Char] = c => AttributeValue.String(c.toString)
      override def decoder: Decoder[Char] = {
        case AttributeValue.String(s) if s.length == 1 => Right(s.charAt(0))
        case AttributeValue.String(s)                  =>
          Left(DecodingError(s"Expected single-character String but got length ${s.length}"))
        case av                                        => Left(DecodingError(s"Expected String attribute value but got: ${av.showType}"))
      }
    }

  private[this] val bigIntCodec: DynamoDBCodec[BigInt] =
    new DynamoDBCodec[BigInt](valueType = DynamoDBCodec.objectType) {
      override def encoder: Encoder[BigInt] = bi => AttributeValue.Number(BigDecimal(bi))
      override def decoder: Decoder[BigInt] = {
        case AttributeValue.Number(bd) => Right(bd.toBigInt)
        case av                        =>
          Left(DecodingError(s"Error getting BigInt value. Expected AttributeValue.Number but found ${av.showType}"))
      }
    }

  private[this] val bigDecimalCodec: DynamoDBCodec[BigDecimal] =
    new DynamoDBCodec[BigDecimal](valueType = DynamoDBCodec.objectType) {
      override def encoder: Encoder[BigDecimal] = bd => AttributeValue.Number(bd)
      override def decoder: Decoder[BigDecimal] = {
        case AttributeValue.Number(bd) => Right(bd)
        case av                        =>
          Left(
            DecodingError(s"Error getting BigDecimal value. Expected AttributeValue.Number but found ${av.showType}")
          )
      }
    }

  private[this] def temporalStringCodec[A](encode: A => String, decode: String => A): DynamoDBCodec[A] =
    new DynamoDBCodec[A](valueType = DynamoDBCodec.objectType) {
      override def encoder: Encoder[A] = a => AttributeValue.String(encode(a))
      override def decoder: Decoder[A] = {
        case AttributeValue.String(s) =>
          try Right(decode(s))
          catch { case e: Exception => Left(DecodingError(e.getMessage)) }
        case av                       => Left(DecodingError(s"Expected String attribute value but got: ${av.showType}"))
      }
    }

  private[this] val dayOfWeekCodec: DynamoDBCodec[DayOfWeek] =
    temporalStringCodec(_.name, DayOfWeek.valueOf)

  private[this] val durationCodec: DynamoDBCodec[Duration] =
    temporalStringCodec(_.toString, Duration.parse)

  private[this] val instantCodec: DynamoDBCodec[Instant] =
    temporalStringCodec(_.toString, Instant.parse)

  private[this] val localDateCodec: DynamoDBCodec[LocalDate] =
    temporalStringCodec(_.toString, LocalDate.parse)

  private[this] val localDateTimeCodec: DynamoDBCodec[LocalDateTime] =
    temporalStringCodec(_.toString, LocalDateTime.parse)

  private[this] val localTimeCodec: DynamoDBCodec[LocalTime] =
    temporalStringCodec(_.toString, LocalTime.parse)

  private[this] val monthCodec: DynamoDBCodec[Month] =
    temporalStringCodec(_.name, Month.valueOf)

  private[this] val monthDayCodec: DynamoDBCodec[MonthDay] =
    temporalStringCodec(_.toString, MonthDay.parse)

  private[this] val offsetDateTimeCodec: DynamoDBCodec[OffsetDateTime] =
    temporalStringCodec(_.toString, OffsetDateTime.parse)

  private[this] val offsetTimeCodec: DynamoDBCodec[OffsetTime] =
    temporalStringCodec(_.toString, OffsetTime.parse)

  private[this] val periodCodec: DynamoDBCodec[Period] =
    temporalStringCodec(_.toString, Period.parse)

  private[this] val yearCodec: DynamoDBCodec[Year] =
    schema1YearCompat match {
      case Schema1Compat.ReadNewWriteNew =>
        // Default: encode as Number, decode Number only.
        new DynamoDBCodec[Year](valueType = DynamoDBCodec.objectType) {
          override def encoder: Encoder[Year] = y => AttributeValue.Number(BigDecimal.valueOf(y.getValue.toLong))
          override def decoder: Decoder[Year] = {
            case AttributeValue.Number(bd) => Right(Year.of(bd.intValue))
            case av                        =>
              Left(DecodingError(s"Expected Number for Year but found ${av.showType}"))
          }
        }
      case _                             =>
        // ReadBothWriteOld / ReadBothWriteNew — legacy stores Year as a zero-padded String
        // with EXCEEDS_PAD sign style: "0001", "2023", "+10000", "-0001".
        val fmt         = new DateTimeFormatterBuilder()
          .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
          .toFormatter()
        val writeLegacy = schema1YearCompat == Schema1Compat.ReadBothWriteOld
        new DynamoDBCodec[Year](valueType = DynamoDBCodec.objectType) {
          override def encoder: Encoder[Year] =
            if (writeLegacy) y => AttributeValue.String(fmt.format(y))
            else y => AttributeValue.Number(BigDecimal.valueOf(y.getValue.toLong))
          override def decoder: Decoder[Year] = {
            case AttributeValue.Number(bd) => Right(Year.of(bd.intValue))
            case AttributeValue.String(s)  =>
              try Right(Year.parse(s, fmt))
              catch { case e: Exception => Left(DecodingError(e.getMessage)) }
            case av                        =>
              Left(DecodingError(s"Expected Number or String for Year but found ${av.showType}"))
          }
        }
    }

  private[this] val yearMonthCodec: DynamoDBCodec[YearMonth] =
    temporalStringCodec(_.toString, YearMonth.parse)

  private[this] val zoneIdCodec: DynamoDBCodec[ZoneId] =
    temporalStringCodec(_.getId, ZoneId.of)

  private[this] val zoneOffsetCodec: DynamoDBCodec[ZoneOffset] =
    temporalStringCodec(_.getId, ZoneOffset.of)

  private[this] val zonedDateTimeCodec: DynamoDBCodec[ZonedDateTime] =
    temporalStringCodec(_.toString, ZonedDateTime.parse)

  private[this] val currencyCodec: DynamoDBCodec[Currency] =
    temporalStringCodec(_.getCurrencyCode, Currency.getInstance)

  private[this] val uuidCodec: DynamoDBCodec[UUID] =
    temporalStringCodec(_.toString, UUID.fromString)

  private[this] val recursiveRecordCache = new ThreadLocal[java.util.HashMap[TypeId[?], Array[FieldInfo]]] {
    override def initialValue: java.util.HashMap[TypeId[?], Array[FieldInfo]] = new java.util.HashMap
  }

  private[this] val discriminatorFields = new ThreadLocal[List[DiscriminatorFieldInfo]] {
    override def initialValue: List[DiscriminatorFieldInfo] = Nil
  }

  private[this] def isOptional[F[_, _], A](reflect: Reflect[F, A]): Boolean =
    !requireOptionFields && reflect.isVariant && {
      val variant = reflect.asVariant.get
      val typeId  = reflect.typeId
      val cases   = variant.cases
      (typeId.owner == Owner.fromPackagePath("scala") && typeId.name == "Option" &&
        cases.length == 2 && cases(1).name == "Some") ||
      typeId.isMaybe
    }

  private[this] def isEnumeration[F[_, _], A](cases: IndexedSeq[Term[F, A, ?]]): Boolean =
    enumValuesAsStrings && cases.forall { case_ =>
      val caseReflect = case_.value
      caseReflect.asRecord.exists(_.fields.isEmpty) ||
      caseReflect.isVariant && caseReflect.asVariant.map(_.cases).forall(isEnumeration)
    }

  private[this] def getDefaultValue[F[_, _], A](fieldReflect: Reflect[F, A]): Option[?] =
    if (requireDefaultValueFields) None
    else fieldReflect.asInstanceOf[Reflect[Binding, A]].getDefaultValue

  private[this] def emptyCollectionConstructorFor[F[_, _], A](reflect: Reflect[F, A]): () => AnyRef =
    if (requireCollectionFields) null
    else if (reflect.isSequence) {
      val seq         = reflect.asSequenceUnknown.get.sequence
      implicit val ct = seq.elemClassTag.asInstanceOf[scala.reflect.ClassTag[Elem]]
      val constructor =
        (if (seq.seqBinding.isInstanceOf[Binding[?, ?]]) seq.seqBinding
         else seq.seqBinding.asInstanceOf[BindingInstance[TC, ?, A]].binding)
          .asInstanceOf[Binding.Seq[Col, Elem]]
          .constructor
      () => constructor.empty.asInstanceOf[AnyRef]
    } else if (reflect.isMap) {
      val map         = reflect.asMapUnknown.get.map
      val constructor =
        (if (map.mapBinding.isInstanceOf[Binding[?, ?]]) map.mapBinding
         else map.mapBinding.asInstanceOf[BindingInstance[TC, ?, A]].binding)
          .asInstanceOf[Binding.Map[Map, Key, Value]]
          .constructor
      () => constructor.emptyObject.asInstanceOf[AnyRef]
    } else null

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

  private[this] def hasOnlyRecordAndVariantCases[F[_, _], A](cases: IndexedSeq[Term[F, A, ?]]): Boolean =
    cases.forall { case_ =>
      val caseReflect = case_.value
      caseReflect.isRecord || caseReflect.asVariant.map(_.cases).forall(hasOnlyRecordAndVariantCases)
    }

  // copied out of zio-blocks as scope is private inside Reflect
  //
  // wrapper.underlyingPrimitiveType (ZB's Reflect.Wrapper method) infers primitiveness
  // from the wrapper's TypeId.representation alone (PrimitiveType.fromTypeId), which is
  // None whenever no explicit TypeId.opaque(..., representation = ...) was given for the
  // wrapper type — e.g. zio-prelude Newtype/Subtype fields with no custom assertion, which
  // rely on ZB's Schema.derived to auto-synthesize the wrapper without any given TypeId.
  // registerOffset (below) uses this result to decide which register kind (int/long/object/
  // etc.) a field occupies when sizing the Registers buffer and computing field offsets —
  // ZB's own macro-generated Binding.Record constructor/deconstructor instead determines
  // register kind directly from the wrapped Reflect's actual structure, independent of any
  // TypeId hint, so a None here desyncs our register layout from ZB's real one and corrupts
  // encode/decode (wrong values, or an out-of-bounds Registers access). Recursing into the
  // wrapped Reflect first (structural check, matches what ZB's macro does) before falling
  // back to the TypeId-based hint keeps the two in agreement regardless of TypeId presence.
  private def unwrapToPrimitiveTypeOption[F[_, _], A](reflect: Reflect[F, A]): Option[PrimitiveType[A]] =
    if (reflect.isWrapper) {
      val wrapper = reflect.asWrapperUnknown.get.wrapper
      unwrapToPrimitiveTypeOption(wrapper.wrapped)
        .orElse(wrapper.underlyingPrimitiveType)
        .asInstanceOf[Option[PrimitiveType[A]]]
    } else reflect.asPrimitive.map(_.primitiveType)

  private def registerOffset[F[_, _], A](reflect: Reflect[F, A]): RegisterOffset =
    unwrapToPrimitiveTypeOption(reflect) match {
      case Some(primitiveType) =>
        primitiveType match {
          case _: PrimitiveType.Unit.type => 0L
          case _: PrimitiveType.Boolean   => RegisterOffset.incrementBooleansAndBytes(0L)
          case _: PrimitiveType.Byte      => RegisterOffset.incrementBooleansAndBytes(0L)
          case _: PrimitiveType.Char      => RegisterOffset.incrementCharsAndShorts(0L)
          case _: PrimitiveType.Short     => RegisterOffset.incrementCharsAndShorts(0L)
          case _: PrimitiveType.Float     => RegisterOffset.incrementFloatsAndInts(0L)
          case _: PrimitiveType.Int       => RegisterOffset.incrementFloatsAndInts(0L)
          case _: PrimitiveType.Double    => RegisterOffset.incrementDoublesAndLongs(0L)
          case _: PrimitiveType.Long      => RegisterOffset.incrementDoublesAndLongs(0L)
          case _                          => RegisterOffset.incrementObjects(0L)
        }
      case _                   => RegisterOffset.incrementObjects(0L)
    }

} // end class DynamoDBCodecDeriver

private final class FieldInfo(
  val span: DynamicOptic.Node.Field,            // original field name — used in decode error paths
  private val defaultValue: Option[?],          // schema-derived default; None if requireDefaultValueFields
  val emptyCollectionConstructor: () => AnyRef, // null when the field is not a collection
  val offset: RegisterOffset,
  val isOptional: Boolean,
  val usesNullSentinel: Boolean = false
) {
  var nonTransient: Boolean                 = true
  var nonEncodeTransient: Boolean           = true
  private[this] var name: String            = null
  private[this] var valueType: Int          = 0
  private[this] var codec: DynamoDBCodec[?] = null

  def setName(name: String): Unit = this.name = name

  def getName: String = name

  def setCodec(codec: DynamoDBCodec[?]): Unit = {
    this.codec = codec
    valueType = codec.valueType
  }

  def getCodec: DynamoDBCodec[?] = codec

  def getValueType: Int = valueType

  def hasDefault: Boolean = defaultValue.isDefined

  def isCollection: Boolean = emptyCollectionConstructor ne null

  def defaultEqualsValue(regs: Registers): Boolean =
    defaultValue match {
      case Some(dv) =>
        getValueType match {
          case DynamoDBCodec.intType     => dv.asInstanceOf[Int] == regs.getInt(offset)
          case DynamoDBCodec.longType    => dv.asInstanceOf[Long] == regs.getLong(offset)
          case DynamoDBCodec.booleanType => dv.asInstanceOf[Boolean] == regs.getBoolean(offset)
          case DynamoDBCodec.byteType    => dv.asInstanceOf[Byte] == regs.getByte(offset)
          case DynamoDBCodec.charType    => dv.asInstanceOf[Char] == regs.getChar(offset)
          case DynamoDBCodec.shortType   => dv.asInstanceOf[Short] == regs.getShort(offset)
          case DynamoDBCodec.floatType   => dv.asInstanceOf[Float] == regs.getFloat(offset)
          case DynamoDBCodec.doubleType  => dv.asInstanceOf[Double] == regs.getDouble(offset)
          case _                         => dv == regs.getObject(offset)
        }
      case None     => false
    }

  def setMissingValueOrDefault(regs: Registers, accumulate: DecodingError => Unit): Unit =
    if (defaultValue.isDefined) {
      val dv = defaultValue.get
      getValueType match {
        case DynamoDBCodec.intType     => regs.setInt(offset, dv.asInstanceOf[Int])
        case DynamoDBCodec.longType    => regs.setLong(offset, dv.asInstanceOf[Long])
        case DynamoDBCodec.booleanType => regs.setBoolean(offset, dv.asInstanceOf[Boolean])
        case DynamoDBCodec.byteType    => regs.setByte(offset, dv.asInstanceOf[Byte])
        case DynamoDBCodec.charType    => regs.setChar(offset, dv.asInstanceOf[Char])
        case DynamoDBCodec.shortType   => regs.setShort(offset, dv.asInstanceOf[Short])
        case DynamoDBCodec.floatType   => regs.setFloat(offset, dv.asInstanceOf[Float])
        case DynamoDBCodec.doubleType  => regs.setDouble(offset, dv.asInstanceOf[Double])
        case _                         => regs.setObject(offset, dv.asInstanceOf[AnyRef])
      }
    } else if (isOptional) {
      regs.setObject(offset, if (usesNullSentinel) MaybeSupport.absent else None)
    } else if (emptyCollectionConstructor ne null) {
      regs.setObject(offset, emptyCollectionConstructor())
    } else {
      accumulate(DecodingError.missingField(getName))
    }

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
}
