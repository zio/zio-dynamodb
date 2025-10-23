package zio.dynamodb.blocks

import zio.dynamodb.AttributeValue
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.FromAttributeValue
import zio.dynamodb.Encoder
import zio.dynamodb.Decoder
import zio.blocks.schema._
import zio.blocks.schema.binding.{ Binding, Constructor, Register, RegisterOffset, Registers }
import zio.Chunk

import java.time._
import java.time.format.{ DateTimeFormatterBuilder, SignStyle }
import java.time.temporal.ChronoField.YEAR
import scala.util.Try
import java.util.UUID
import zio.dynamodb.DynamoDBError
import zio.dynamodb.blocks.BlocksCodecViaDynamic.dynamicEncoder
import zio.dynamodb.blocks.BlocksCodecViaDynamic.dynamicDecoder

/*
Reflect
TODO
- Remaining Dynamic in Reflect$ (zio.blocks.schema)
- caching
DONE
- Wrapper in Reflect$ (zio.blocks.schema)
- Seq all primitive types
- Primitive in Reflect$ (zio.blocks.schema)
- Sequence in Reflect$ (zio.blocks.schema)
- Record in Reflect$ (zio.blocks.schema)
- Variant in Reflect$ (zio.blocks.schema)
- Map in Reflect$ (zio.blocks.schema)
- Deferred in Reflect$ (zio.blocks.schema)
- Primitive in Reflect$ (zio.blocks.schema)
- Dynamic Primitive + Recordin Reflect$ (zio.blocks.schema)
 */
object BlocksCodec {
  // type Encoder[A]  = A => AttributeValue
  // type Decoder[+A] = AttributeValue => Either[ItemError, A]

  private val stringEncoder = encoder(Schema[String])
  private val yearFormatter =
    new DateTimeFormatterBuilder().appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD).toFormatter

  def maybeDiscriminatorNameModifier(
    modifiers: Seq[Modifier]
  ): Option[String] =
    modifiers.collectFirst {
      case Modifier.config("discriminatorName", value) => value
    }

  /**
   * Note that Some, Left and Right use value classes with a single field named "value" which equates to a schema Record.
   * This function searches all cases in the Variant for the `caseLabel` and returns the binding for that field as a Some,
   * else returns a None.
   */
  def reflectBindingForCaseValueField[A](
    caseLabel: String,
    v: Reflect.Variant.Bound[A]
  ): Option[Reflect.Bound[A]] = {
    // Find the case for the given label
    val case_ : Option[Term[Binding, A, _ <: A]] = v.cases.find(_.name == caseLabel)

    // dig into the structure of the found case to get the binding for the value field
    case_ match {
      case Some(recordForValue) =>
        recordForValue.value match {
          case Reflect.Record(fields, _, _, _, _) if fields.size == 1 && fields(0).name == "value" =>
            fields(0) match {
              case Term(_, value, _, _) =>
                Some(value.asInstanceOf[Reflect.Bound[A]])
            }
          case _                                                                                   => None
        }
      case None                 => None
    }
  }

  private def yearEncoder[A]: Encoder[A] =
    (a: A) => {
      val year      = a.asInstanceOf[Year]
      val formatted = year.format(yearFormatter)
      AttributeValue.String(formatted)
    }

  def primitiveEncoder[A](primitiveType: PrimitiveType[A]): Encoder[A] =
    primitiveType match {
      case PrimitiveType.Unit              => _ => AttributeValue.Null
      case PrimitiveType.Char(_)           => (a: A) => AttributeValue.String(Character.toString(a))
      case PrimitiveType.String(_)         => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.Boolean(_)        => (a: A) => AttributeValue.Bool(a.asInstanceOf[Boolean])
      case PrimitiveType.Byte(_)           => (a: A) => AttributeValue.Binary(Chunk(a))
      case PrimitiveType.Short(_)          => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case PrimitiveType.Int(_)            => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case PrimitiveType.Long(_)           => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case PrimitiveType.Float(_)          => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case PrimitiveType.Double(_)         => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case PrimitiveType.BigDecimal(_)     => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case PrimitiveType.BigInt(_)         => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case PrimitiveType.Currency(_)       => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.DayOfWeek(_)      => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.Duration(_)       => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.Instant(_)        => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.LocalDate(_)      => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.LocalDateTime(_)  => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.LocalTime(_)      => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.Month(_)          => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.MonthDay(_)       => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.OffsetDateTime(_) => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.OffsetTime(_)     => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.Period(_)         => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.UUID(_)           => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.Year(_)           => yearEncoder
      case PrimitiveType.YearMonth(_)      => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.ZonedDateTime(_)  => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.ZoneId(_)         => (a: A) => AttributeValue.String(a.toString)
      case PrimitiveType.ZoneOffset(_)     => (a: A) => AttributeValue.String(a.toString)
    }

  private def nativeMapEncoder[A, V](encoderV: Encoder[V]) =
    (a: A) => {
      val m  = a.asInstanceOf[Map[String, V]]
      val av = AttributeValue.Map(m.map {
        case (k, v) =>
          (stringEncoder(k), encoderV(v))
      }.asInstanceOf[Map[AttributeValue.String, AttributeValue]])
      av
    }

  def mapEncoder[K, V](ks: Reflect.Bound[K], vs: Reflect.Bound[V]): Encoder[Map[K, V]] =
    ks match {
      case Reflect.Primitive(_: PrimitiveType.String, _, _, _, _) =>
        nativeMapEncoder(reflectEncoder(vs))
      case Reflect.Deferred(value)                                =>
        mapEncoder(value(), vs)
      case _                                                      =>
        throw new Exception(
          "TODO: nonNativeMapEncoder(encoder(ks), encoder(vs))"
        ) // Non native Map encoder relies on Sequence encoder
    }

  /*
    def toDynamicValue(value: C[A])(implicit F: HasBinding[F]): DynamicValue = {
      val iterator = seqDeconstructor.deconstruct(value)
      val builder  = Vector.newBuilder[DynamicValue]
      while (iterator.hasNext) builder.addOne(element.toDynamicValue(iterator.next()))
      new DynamicValue.Sequence(builder.result())
    }
   */
  //def foo[Col[_], A](col: Col[A])(implicit F: HasBinding[F])
  def sequenceEncoder[Col[_], A](enc: Encoder[A]): Encoder[Col[A]] =
    (a: Col[A]) => {
      val av: Iterable[AttributeValue] = a match {
        case a: Iterable[_] =>
          a.map(v => enc(v.asInstanceOf[A])) // TODO: use Binding functionality to deconstruct a sequence
        case a: Array[_]    => a.map(v => enc(v.asInstanceOf[A])).toSeq
        case c              => throw new Exception(s"Expected a collection type but found: ${c.getClass.getSimpleName}")
      }
      AttributeValue.List(av)
    }

  def optionEncoder[A](v: Reflect.Variant.Bound[A]): Encoder[A] = {
    case Some(a) =>
      reflectBindingForCaseValueField("Some", v) match {
        case Some(value) =>
          val enc = reflectEncoder(value)
          enc(a.asInstanceOf[value.Structure])
        case None        =>
          throw new Exception(s"Unexpected Schema shape for Some") // this should never happen
      }
    case None    => AttributeValue.Null                              // gets removed at the Record level
    case _       => throw new Exception(s"Input type not an Option") // TODO: tighten up types
  }

  def eitherEncoder[A](v: Reflect.Variant.Bound[A]): Encoder[A] = {
    case Right(r) => encodeCase("Right", r, v)
    case Left(l)  => encodeCase("Left", l, v)
    case _        => throw new Exception(s"Input type not an Either") // TODO: tighten types
  }

  private def encodeCase[A](caseLabel: String, value: Any, v: Reflect.Variant.Bound[A]): AttributeValue =
    reflectBindingForCaseValueField(caseLabel, v) match {
      case Some(binding) =>
        val enc = reflectEncoder(binding)
        AttributeValue.Map.empty + (caseLabel -> enc(value.asInstanceOf[binding.Structure]))
      case None          =>
        throw new Exception(s"Unexpected Schema shape for $caseLabel") // should never happen
    }

  def isOption[A](variant: Reflect.Variant.Bound[A]): Boolean =
    variant.typeName.name == "Option" && variant.typeName.namespace.packages.mkString(".") == "scala"

  def isEither[A](variant: Reflect.Variant.Bound[A]): Boolean =
    variant.typeName.name == "Either" && variant.typeName.namespace.packages.mkString(".") == "scala.util"

  def reflectEncoder[A](reflect: Reflect.Bound[A]): Encoder[A] =
    reflect match {
      case Reflect.Primitive(primitiveType, _, _, _, _)             =>
        println(s"XXXXXX BlocksCodec.reflectEncoder Reflect.Primitive")
        primitiveEncoder(primitiveType)
      case Reflect.Map(key, value, _, _, _, _)                      =>
        mapEncoder(key, value).asInstanceOf[Encoder[A]] // TODO: handle non-native maps

      /*
  case class Sequence[F[_, _], A, C[_]](
    element: Reflect[F, A],
    typeName: TypeName[C[A]],
    seqBinding: F[BindingType.Seq[C], C[A]],
    doc: Doc = Doc.Empty,
    modifiers: Seq[Modifier.Reflect] = Nil
  ) extends Reflect[F, C[A]] { self =>

    def toDynamicValue(value: C[A])(implicit F: HasBinding[F]): DynamicValue = {
      val iterator = seqDeconstructor.deconstruct(value)
      val builder  = Vector.newBuilder[DynamicValue]
      while (iterator.hasNext) builder.addOne(element.toDynamicValue(iterator.next()))
      new DynamicValue.Sequence(builder.result())
    }

       */
      case Reflect.Sequence(element, _, _, _, _)                    => {
        case (a: Iterable[_]) =>
          println(s"XXXXXX BlocksCodec.reflectEncoder Reflect.Sequence Iterable")
//          val x   = s.seqDeconstructor(seqBinding) // try to use Blocks Binding functionality for deconstruction
          val enc = reflectEncoder(element)
          val av  = AttributeValue.List(a.map {
            case v => enc(v.asInstanceOf[element.Structure])
          })
          av
        case (a: Array[_])    =>
          val enc = reflectEncoder(element)
          val av  = AttributeValue.List(a.map {
            case v => enc(v.asInstanceOf[element.Structure])
          }.toSeq) // Array is not Iterable so convert to Seq
          av
        case c                => throw new Exception(s"Expected a collection type but found: ${c.getClass.getSimpleName}")
      }

      case Reflect.Wrapper(wrapped, typeName, wrapperBinding, _, _) =>
        wrapperBinding match {
          case Binding.Wrapper(_, unwrap, _, _) =>
            (a: A) => {
              val b   = unwrap(a)
              val enc = reflectEncoder(wrapped)
              val av  = enc(b.asInstanceOf[wrapped.Structure])
              av
            }

          case _                                => throw new Exception("Unknown wrapper binding")
        }

      case r @ Reflect.Record(fields, _, _, _, _)                   =>
        // TODO: Extract recordEncoder
        (a: A) => {
          // TODO: replace foldLeft with imperative loop
          val avMap = fields.foldLeft[AttributeValue.Map](AttributeValue.Map.empty) {
            case (acc: AttributeValue.Map, field) =>
              val fieldName                     = field.name
              val maybeLens: Option[Lens[A, _]] =
                r.lensByName(fieldName) // TODO: should we use a lower level deconstructor rather than a lens?
              if (maybeLens.isDefined) {
                val lens       = maybeLens.get
                val fieldValue = lens.get(a)
                val enc        = reflectEncoder(field.value)
                val av         = enc(fieldValue.asInstanceOf[field.value.Structure])

                field.value match {
                  case v: Reflect.Variant.Bound[_] if isOption(v) && av == AttributeValue.Null =>
                    acc
                  case _                                                                       =>
                    acc + (fieldName -> av)
                }
              } else
                throw new Exception(s"Field $fieldName not found in record") // this should not happen
          }
          avMap
        }

      case v @ Reflect.Variant(cases, _, _, _, variantModifiers)    => // encoder
        (a: A) =>
          val idx   = v.discriminator.discriminate(a)
          val case_ = cases(idx)
          if (isOption(v))
            optionEncoder(v)(a)
          else if (isEither(v))
            eitherEncoder(v)(a)
          else {
            //TODO: extract to Term level Variant encoder
            val enc: Encoder[A] = case_.value match {
              case r: Reflect.Record.Bound[aa] => // "default" vs "compact" encoding. Variant instance is a Record
                if (r.fields.isEmpty)
                  // empty fields implies a case object
                  _ => AttributeValue.String(case_.name)
                else {
                  // TODO: Consider a NoDiscriminator modifier as well
                  val disc: Option[String] = maybeDiscriminatorNameModifier(variantModifiers)
                  val av: AttributeValue   = reflectEncoder(case_.value)(a.asInstanceOf[case_.value.Structure])
                  disc match {
                    case Some(discName) =>
                      val newMap = av match {
                        case AttributeValue.Map(map) =>
                          map + (AttributeValue.String(discName) -> AttributeValue.String(case_.name))
                        case _                       =>
                          throw new Exception(s"Could not encode $a with discriminator $disc")
                      }
                      _ => AttributeValue.Map(newMap)
                    case None           =>
                      // tagged Variant encoding
                      _ => AttributeValue.Map(case_.name, av)
                  }
                }
              case r                           =>
                throw new Exception(s"Did not expect Reflect $r - only Record is valid")
            }
            enc(a)
          }
      case Reflect.Deferred(value)                                  =>
        reflectEncoder(value())
      /*
        case class Dynamic[F[_, _]](
    dynamicBinding: F[BindingType.Dynamic, DynamicValue],
    doc: Doc = Doc.Empty,
    modifiers: Seq[Modifier.Dynamic] = Nil
  ) extends Reflect[F, DynamicValue] {
       */
      case d @ Reflect.Dynamic(_, _, _, _)                          =>
        (a: A) => {
          val dv = d.toDynamicValue(a)
          println(s"XXXXXXXXX dv: $dv")
          dynamicEncoder(dv)
        }
      case r                                                        => throw new Exception(s"Could not encode ${r.getClass.getSimpleName} just yet")
    }

  def encoder[A](implicit schema: Schema[A]): Encoder[A] = reflectEncoder(schema.reflect)

  // ================================================================================================

  def fromAttributeValueList[A, C[_]](
    r: Reflect.Sequence[Binding, A, C],
    avList: AttributeValue.List
  ): Either[List[DynamoDBError.ItemError], C[A]] = {
    var errors: List[DynamoDBError.ItemError] = Nil

    def addError(e: DynamoDBError.ItemError): Unit = errors = errors :+ e

    val elements: Iterable[AttributeValue] = avList.value
    val constructor                        = r.seqConstructor
    r.element.asPrimitive match {
      case Some(primitive) =>
        primitive.primitiveType match {
          case _: PrimitiveType.Boolean =>
            val builder = constructor.newBooleanBuilder(elements.size)
            elements.foreach { elem =>
              val dec = primitiveDecoder(primitive.primitiveType)
              dec(elem) match {
                case Right(value) =>
                  constructor.addBoolean(builder, value.asInstanceOf[Boolean])
                case Left(error)  =>
                  addError(error)
              }
            }
            if (errors.isEmpty) new Right(constructor.resultBoolean(builder))
            else new Left(errors)
          case _: PrimitiveType.Byte    =>
            val builder = constructor.newByteBuilder(elements.size)
            elements.foreach { elem =>
              val dec = primitiveDecoder(primitive.primitiveType)
              dec(elem) match {
                case Right(value) =>
                  constructor.addByte(builder, value.asInstanceOf[Byte])
                case Left(error)  =>
                  addError(error)
              }
            }
            if (errors.isEmpty) new Right(constructor.resultByte(builder))
            else new Left(errors)
          case _: PrimitiveType.Char    =>
            val builder = constructor.newCharBuilder(elements.size)
            elements.foreach { elem =>
              val dec = primitiveDecoder(primitive.primitiveType)
              dec(elem) match {
                case Right(value) =>
                  constructor.addChar(builder, value.asInstanceOf[Char])
                case Left(error)  =>
                  addError(error)
              }
            }
            if (errors.isEmpty) new Right(constructor.resultChar(builder))
            else new Left(errors)
          case _: PrimitiveType.Int     =>
            val builder = constructor.newIntBuilder(elements.size)
            elements.foreach { elem =>
              val dec = primitiveDecoder(primitive.primitiveType)
              dec(elem) match {
                case Right(value) =>
                  constructor.addInt(builder, value.asInstanceOf[Int])
                case Left(error)  =>
                  addError(error)
              }
            }
            if (errors.isEmpty) new Right(constructor.resultInt(builder))
            else new Left(errors)
          case _: PrimitiveType.Short   =>
            val builder = constructor.newShortBuilder(elements.size)
            elements.foreach { elem =>
              val dec = primitiveDecoder(primitive.primitiveType)
              dec(elem) match {
                case Right(value) =>
                  constructor.addShort(builder, value.asInstanceOf[Short])
                case Left(error)  =>
                  addError(error)
              }
            }
            if (errors.isEmpty) new Right(constructor.resultShort(builder))
            else new Left(errors)
          case _: PrimitiveType.Long    =>
            val builder = constructor.newLongBuilder(elements.size)
            elements.foreach { elem =>
              val dec = primitiveDecoder(primitive.primitiveType)
              dec(elem) match {
                case Right(value) =>
                  constructor.addLong(builder, value.asInstanceOf[Long])
                case Left(error)  =>
                  addError(error)
              }
            }
            if (errors.isEmpty) new Right(constructor.resultLong(builder))
            else new Left(errors)
          case _: PrimitiveType.Float   =>
            val builder = constructor.newFloatBuilder(elements.size)
            elements.foreach { elem =>
              val dec = primitiveDecoder(primitive.primitiveType)
              dec(elem) match {
                case Right(value) =>
                  constructor.addFloat(builder, value.asInstanceOf[Float])
                case Left(error)  =>
                  addError(error)
              }
            }
            if (errors.isEmpty) new Right(constructor.resultFloat(builder))
            else new Left(errors)
          case _: PrimitiveType.Double  =>
            val builder = constructor.newDoubleBuilder(elements.size)
            elements.foreach { elem =>
              val dec = primitiveDecoder(primitive.primitiveType)
              dec(elem) match {
                case Right(value) =>
                  constructor.addDouble(builder, value.asInstanceOf[Double])
                case Left(error)  =>
                  addError(error)
              }
            }
            if (errors.isEmpty) new Right(constructor.resultDouble(builder))
            else new Left(errors)
          case _                        =>
            val builder = constructor.newObjectBuilder[A](elements.size)
            elements.foreach { elem =>
              val dec = reflectDecoder(r.element)
              dec(elem) match {
                case Right(value) => constructor.addObject(builder, value)
                case Left(error)  => addError(error)
              }
            }
            if (errors.isEmpty) new Right(constructor.resultObject(builder))
            else new Left(errors)
        }
      case _               =>
        val builder = constructor.newObjectBuilder[A](elements.size)
        elements.foreach { elem =>
          val dec = reflectDecoder(r.element)
          dec(elem) match {
            case Right(value) => constructor.addObject(builder, value)
            case Left(error)  => addError(error)
          }
        }
        if (errors.isEmpty) new Right(constructor.resultObject(builder))
        else new Left(errors)
    }
  }

  private def javaTimeStringParser[A](
    av: AttributeValue
  )(unsafeParse: String => A): Either[DynamoDBError.ItemError, A] =
    FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av).flatMap { s =>
      val stringOrA = Try(unsafeParse(s)).toEither.left
        .map(e => DecodingError(s"error parsing string '$s': ${e.getMessage}"))
      stringOrA
    }

  def eitherDecoder[A](v: Reflect.Variant.Bound[A]): Decoder[A] = {
    def decodeEitherValue[A](label: String, v: Reflect.Variant.Bound[A]): Decoder[A] =
      // dig into the structure of the found case to get the decoder for the value field
      reflectBindingForCaseValueField(label, v) match {
        case Some(value) =>
          reflectDecoder(value).asInstanceOf[Decoder[A]]
        case None        =>
          (_: AttributeValue) =>
            Left(
              DecodingError(s"Unexpected Schema shape for $label")
            ) // this should never happen
      }

    {
      case AttributeValue.Map(map) if map.size == 1 =>
        val iter  = map.iterator
        val entry = iter.next() // Map.Entry[_, _] under the hood, no extra tuple
        entry._1 match {
          case AttributeValue.String("Right") =>
            decodeEitherValue("Right", v)(entry._2).map(Right(_)).asInstanceOf[Either[DecodingError, A]]
          case AttributeValue.String("Left")  =>
            decodeEitherValue("Left", v)(entry._2).map(Left(_)).asInstanceOf[Either[DecodingError, A]]
          case other                          =>
            Left(DecodingError(s"Unexpected key in Either decoder: $other"))
        }

      case AttributeValue.Map(map)                  =>
        Left(DecodingError(s"Expected single-element map, got keys: ${map.keys}"))

      case av                                       =>
        Left(DecodingError(s"Expected AttributeValue.Map but found ${av.showType}"))
    }
  }

  // Note that None decoding (AttributeValue.Null or missing field value) is done upstream
  // so we only focus on the Some case here
  def someDecoder[A](v: Reflect.Variant.Bound[A]): Decoder[A] = { (av: AttributeValue) =>
    // we are dealing with the Some case of Option Variant
    // so we can short cut decoding of Option Variant to decoding of the value field of the Some case
    reflectBindingForCaseValueField("Some", v) match {
      case Some(value) => reflectDecoder(value).apply(av).map(Some(_)).asInstanceOf[Either[DecodingError, A]]
      case None        => Left(DecodingError(s"Unexpected Schema shape for Some")) // this should never happen
    }

  }

  def reflectDecoder[A](reflect: Reflect.Bound[A]): Decoder[A] =
    reflect match {
      case Reflect.Primitive(primitiveType, _, _, _, _)                 =>
        primitiveDecoder(primitiveType)
      case s @ Reflect.Sequence(element, _, seqBinding, _, _)           => {
        case l: AttributeValue.List => // DynamoDBExecutor uses Chunk for avIterable
          val errorsOrCol: Either[List[DynamoDBError.ItemError], Any] = fromAttributeValueList(s, l)
          errorsOrCol.left.map(xs => DecodingError(xs.mkString(", ")))

        case av                     => Left(DecodingError(s"Expected AttributeValue.List but found ${av.showType}"))
      }
      case Reflect.Wrapper(wrapped, _, wrapperBinding, _, _)            =>
        wrapperBinding match {
          case Binding.Wrapper(wrap, _, _, _) =>
            val dec = reflectDecoder(wrapped)
            (av: AttributeValue) =>
              dec(av) match {
                case Left(e)  => Left(e)
                case Right(b) =>
                  val w = wrap(b)
                  w.left.map(s => DecodingError(s"Error unwrapping $s"))
              }
          case _                              => (_: AttributeValue) => Left(DecodingError("Unknown wrapper binding"))
        }
      case r @ Reflect.Record(fields, _, _, _, _)                       =>
        // TODO: extract recordDecoder
        (av: AttributeValue) =>
          if (fields.isEmpty) {
            // empty fields implies a case object
            val constructor: Constructor[A] = r.constructor
            val registers                   = Registers(constructor.usedRegisters)
            Right(r.constructor.construct(registers, RegisterOffset.Zero))
          } else
            av match {
              case AttributeValue.Map(map) =>
                var errors: Option[Chunk[String]] = None
                def addError(e: String): Unit     = errors = errors.map(_ :+ e).orElse(Some(Chunk(e)))
                val constructor: Constructor[A]   = r.constructor
                val registers                     = Registers(constructor.usedRegisters)

                fields.foreach {
                  var idx = 0
                  field =>
                    val (isOpt, cases)                     = field.value match {
                      case Reflect.Variant(cases, typeName, _, _, _) if typeName.name == "Option" => (true, cases)
                      case _                                                                      => (false, Vector.empty)
                    }
                    val fieldName                          = field.name
                    val fieldValue: Option[AttributeValue] = map.get(AttributeValue.String(fieldName))

                    // TODO: generalise missing field handling for Option and other container types
                    val isNone = isOpt && (fieldValue.isEmpty || fieldValue == Some(AttributeValue.Null))

                    if (isNone)
                      Right(None)
                    else if (fieldValue.isEmpty)
                      addError(s"Field $fieldName not found")
                    else {
                      val dec = reflectDecoder(field.value)
                      dec(fieldValue.get) match { // naked get on Option is safe
                        case Left(e)      =>
                          addError(s"Field $fieldName: ${e.getMessage}")
                        case Right(value) =>
                          r.registers(idx).asInstanceOf[Register[Any]].set(registers, RegisterOffset.Zero, value)
                      }
                      idx += 1
                    }
                }
                if (errors.isEmpty) {
                  val x = constructor.construct(registers, RegisterOffset.Zero)
                  Right(x)
                } else
                  Left(DecodingError(errors.mkString(", ")))

              case av                      =>
                Left(DecodingError(s"Could not decode $av just yet"))
            }
      case v @ Reflect.Variant(cases, typeName, _, _, variantModifiers) => // decoder
        if (isOption(v))
          someDecoder(v)
        else if (isEither(v))
          eitherDecoder(v)
        else
          maybeDiscriminatorNameModifier(variantModifiers) match { // TODO: Consider a NoDiscriminator modifier as well
            case Some(discName) =>
              (av: AttributeValue) =>
                av match {
                  case m @ AttributeValue.Map(_) => // We only handle records
                    m.get(discName) match {
                      case Some(AttributeValue.String(name)) => // extract discriminator name
                        v.caseByName(name) match {
                          case None        =>
                            Left(DecodingError(s"Could not find case $name"))
                          case Some(case_) => // extract case so we can get case decoder
                            val dec = reflectDecoder(case_.value)
                            dec(av) match {
                              case Left(e)  => Left(e)
                              case Right(r) => Right(r.asInstanceOf[A])
                            }
                        }
                      case _                                 =>
                        Left(DecodingError(s"Could not find discriminator $discName"))
                    }
                  case av                        =>
                    Left(
                      DecodingError(
                        s"Expected an AttributeValue.Map but found ${av.getClass.getSimpleName}"
                      )
                    )
                }
            case None           => // no DiscriminatorName modifier
              (av: AttributeValue) =>
                av match {
                  case AttributeValue.Map(map)          => // We only expect map of discriminator name
                    // map must have single entry only of AttributeValue.String(discriminatorName) -> AttributeValue
                    if (map.size != 1)
                      Left(DecodingError(s"Expected a single entry map but found ${map.size}"))
                    else {
                      val (AttributeValue.String(discriminatorName), av) = map.iterator.next()
                      v.caseByName(discriminatorName) match {
                        case None        =>
                          Left(DecodingError(s"Could not find case $discriminatorName"))
                        case Some(case_) => // extract case so we can get case decoder
                          val dec = reflectDecoder(case_.value)
                          dec(av) match {
                            case Left(e)  => Left(e)
                            case Right(r) => Right(r.asInstanceOf[A])
                          }
                      }

                    }
                  case AttributeValue.String(enumValue) =>
                    v.caseByName(enumValue) match {
                      case None        =>
                        Left(DecodingError(s"Could not find case $enumValue"))
                      case Some(case_) => // extract case so we can get case decoder
                        val dec = reflectDecoder(case_.value)
                        dec(av) match {
                          case Left(e)  => Left(e)
                          case Right(r) => Right(r.asInstanceOf[A])
                        }
                    }
                  case av                               =>
                    Left(
                      DecodingError(
                        s"Expected an AttributeValue.Map but found ${av.getClass.getSimpleName}"
                      )
                    )
                }
          }
      case Reflect.Deferred(value)                                      =>
        val dec = reflectDecoder(value())
        (av: AttributeValue) => dec(av)
      case Reflect.Dynamic(dynamicBinding, _, _, _)                     =>
        (av: AttributeValue) =>
          println(s"XXXXXXXXX DynamicValue decoder")
          val x: Either[DynamoDBError.ItemError, DynamicValue] = dynamicDecoder(av) match {
            case Left(e)   => Left(e)
            case Right(dv) =>
              reflect.fromDynamicValue(dv) match {
                case Left(e)  => Left(DecodingError(e.toString))
                case Right(r) =>
                  println(s"XXXXXXXXX DynamicValue decoder r: ${r.getClass.getName} $r")
                  Right(r).asInstanceOf[Either[DynamoDBError.ItemError, DynamicValue]]
              }
          }
          x

      case r                                                            =>
        (_: AttributeValue) => Left(DecodingError(s"Could not decode Reflect $r just yet"))
    }

  def primitiveDecoder[A](primitiveType: PrimitiveType[A]): Decoder[A] =
    primitiveType match {
      case PrimitiveType.Unit              => _ => Right(())
      case PrimitiveType.String(_)         =>
        (av: AttributeValue) => FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av)
      case PrimitiveType.Boolean(_)        =>
        (av: AttributeValue) => FromAttributeValue.booleanFromAttributeValue.fromAttributeValue(av)
      case PrimitiveType.Short(_)          =>
        (av: AttributeValue) => FromAttributeValue.shortFromAttributeValue.fromAttributeValue(av)
      case PrimitiveType.Int(_)            =>
        (av: AttributeValue) => FromAttributeValue.intFromAttributeValue.fromAttributeValue(av)
      case PrimitiveType.Long(_)           =>
        (av: AttributeValue) => FromAttributeValue.longFromAttributeValue.fromAttributeValue(av)
      case PrimitiveType.Float(_)          =>
        (av: AttributeValue) => FromAttributeValue.floatFromAttributeValue.fromAttributeValue(av)
      case PrimitiveType.Double(_)         =>
        (av: AttributeValue) => FromAttributeValue.doubleFromAttributeValue.fromAttributeValue(av)
      case PrimitiveType.BigDecimal(_)     =>
        (av: AttributeValue) =>
          FromAttributeValue.bigDecimalFromAttributeValue
            .fromAttributeValue(av)
            .map(_.bigDecimal)
      case PrimitiveType.BigInt(_)         =>
        (av: AttributeValue) =>
          FromAttributeValue.bigDecimalFromAttributeValue
            .fromAttributeValue(av)
            .map(_.toBigInt.bigInteger)
      case PrimitiveType.Byte(_)           =>
        (av: AttributeValue) =>
          FromAttributeValue.byteFromAttributeValue
            .fromAttributeValue(av)
      case PrimitiveType.Char(_)           =>
        (av: AttributeValue) =>
          FromAttributeValue.stringFromAttributeValue
            .fromAttributeValue(av)
            .map { s =>
              val array = s.toCharArray
              array(0)
            }
      case PrimitiveType.UUID(_)           =>
        (av: AttributeValue) =>
          FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av).flatMap { s =>
            Try(UUID.fromString(s)).toEither.left.map(iae => DecodingError(s"Invalid UUID: ${iae.getMessage}"))
          }
      case PrimitiveType.Currency(_)       =>
        (av: AttributeValue) =>
          FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av).flatMap { s =>
            Try(java.util.Currency.getInstance(s)).toEither.left.map(e =>
              DecodingError(s"Invalid Currency: ${e.getMessage}")
            )
          }
      case PrimitiveType.DayOfWeek(_)      =>
        (av: AttributeValue) => javaTimeStringParser(av)(DayOfWeek.valueOf(_))
      case PrimitiveType.Duration(_)       =>
        (av: AttributeValue) => javaTimeStringParser(av)(Duration.parse(_))
      case PrimitiveType.Instant(_)        =>
        (av: AttributeValue) => javaTimeStringParser(av)(Instant.parse)
      case PrimitiveType.LocalDate(_)      =>
        (av: AttributeValue) => javaTimeStringParser(av)(LocalDate.parse)
      case PrimitiveType.LocalDateTime(_)  =>
        (av: AttributeValue) => javaTimeStringParser(av)(LocalDateTime.parse)
      case PrimitiveType.LocalTime(_)      =>
        (av: AttributeValue) => javaTimeStringParser(av)(LocalTime.parse)
      case PrimitiveType.Month(_)          =>
        (av: AttributeValue) => javaTimeStringParser(av)(Month.valueOf(_))
      case PrimitiveType.MonthDay(_)       =>
        (av: AttributeValue) => javaTimeStringParser(av)(MonthDay.parse(_))
      case PrimitiveType.OffsetDateTime(_) =>
        (av: AttributeValue) => javaTimeStringParser(av)(OffsetDateTime.parse)
      case PrimitiveType.OffsetTime(_)     =>
        (av: AttributeValue) => javaTimeStringParser(av)(OffsetTime.parse)
      case PrimitiveType.Period(_)         =>
        (av: AttributeValue) => javaTimeStringParser(av)(Period.parse(_))
      case PrimitiveType.Year(_)           =>
        (av: AttributeValue) => javaTimeStringParser(av)(Year.parse(_))
      case PrimitiveType.YearMonth(_)      =>
        (av: AttributeValue) => javaTimeStringParser(av)(YearMonth.parse(_))
      case PrimitiveType.ZonedDateTime(_)  =>
        (av: AttributeValue) => javaTimeStringParser(av)(ZonedDateTime.parse)
      case PrimitiveType.ZoneId(_)         =>
        (av: AttributeValue) => javaTimeStringParser(av)(ZoneId.of(_))
      case PrimitiveType.ZoneOffset(_)     =>
        (av: AttributeValue) => javaTimeStringParser(av)(ZoneOffset.of(_))
    }

  def decoder[A](implicit schema: Schema[A]): Decoder[A] = reflectDecoder(schema.reflect)

}
