package zio.dynamodb.blocks

import zio.dynamodb.AttributeValue
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.FromAttributeValue
import zio.dynamodb.Encoder
import zio.dynamodb.Decoder
import zio.blocks.schema._
import zio.blocks.schema.binding.Constructor
import zio.blocks.schema.binding.Register
import zio.blocks.schema.binding.RegisterOffset
import zio.blocks.schema.binding.Registers
import zio.Chunk

object BlocksCodec {
  // type Encoder[A]  = A => AttributeValue
  // type Decoder[+A] = AttributeValue => Either[ItemError, A]

  private val stringEncoder = encoder(Schema[String])

  def maybeDiscriminatorNameModifier(
    modifiers: Seq[Modifier.Variant]
  ): Option[String] =
    modifiers.collectFirst {
      case Modifier.config("discriminatorName", value) => value
    }

  /**
   * Assumes case field is a Record with a single field named "value", and returns the binding for that field
   * Applies to Some, Left and Right
   */
  def reflectBindingForCaseValueField[A](
    caseLabel: String,
    v: Reflect.Variant.Bound[A]
  ): Option[Reflect.Bound[A]] = {
    // Find the case for the given label
    val case_ = v.cases.find(_.name == caseLabel)

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

  // TODO: handle all primitive types
  def primitiveEncoder[A](primitiveType: PrimitiveType[A]): Encoder[A] =
    (a: A) => {
      primitiveType match {
        case PrimitiveType.String(_) => AttributeValue.String(a.toString)
        case PrimitiveType.Int(_)    => AttributeValue.Number(BigDecimal(a.toString))
        case _                       => throw new Exception(s"Could not encode $a of type $primitiveType")
      }
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
        throw new Exception("TODO: nonNativeMapEncoder(encoder(ks), encoder(vs))")
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
    case None    => AttributeValue.Null                              // gets redacted at the Record level
    case _       => throw new Exception(s"Input type not an Option") // TODO: tighten up types
  }

  def eitherEncoderOld[A](record: Reflect.Record.Bound[A]): Encoder[A] = {
    def encodeBranch[B](label: String, extract: PartialFunction[A, B]): Encoder[A] =
      (a: A) => {
        val valueFieldTerm = record.fields(0)
        extract.lift(a) match {
          case Some(value) =>
            valueFieldTerm match {
              case Term(_, valueFieldValue, _, _) =>
                val enc = reflectEncoder(valueFieldValue)
                val x   = enc(value.asInstanceOf[valueFieldValue.Structure])
                AttributeValue.Map.empty + (label -> x)
            }
          case None        =>
            throw new Exception(s"Expected $label")
        }
      }

    (record.typeName.name, record.fields.length) match {
      case ("Right", 1)  => encodeBranch("Right", { case Right(v) => v })
      case ("Left", 1)   => encodeBranch("Left", { case Left(v) => v })
      case (typeName, n) =>
        throw new Exception(s"Could not encode Either for type $typeName with $n fields")
    }
  }

  def eitherEncoder[A](v: Reflect.Variant.Bound[A]): Encoder[A] = {
    case Right(r) => encodeCase("Right", r, v)
    case Left(l)  => encodeCase("Left", l, v)
    case _        => throw new Exception(s"Input type not an Either") // TODO: tighten types
  }

  private def encodeCase[A](tag: String, value: Any, v: Reflect.Variant.Bound[A]): AttributeValue =
    reflectBindingForCaseValueField(tag, v) match {
      case Some(binding) =>
        val enc = reflectEncoder(binding)
        AttributeValue.Map.empty + (tag -> enc(value.asInstanceOf[binding.Structure]))
      case None          =>
        throw new Exception(s"Unexpected Schema shape for $tag") // should never happen
    }

  def isOption[A](variant: Reflect.Variant.Bound[A]): Boolean =
    variant.typeName.name == "Option" && variant.typeName.namespace.packages.mkString(".") == "scala"

  def isEither[A](variant: Reflect.Variant.Bound[A]): Boolean =
    variant.typeName.name == "Either" && variant.typeName.namespace.packages.mkString(".") == "scala.util"

  def reflectEncoder[A](reflect: Reflect.Bound[A]): Encoder[A] =
    reflect match {
      case Reflect.Primitive(primitiveType, _, _, _, _)          =>
        primitiveEncoder(primitiveType)
      case Reflect.Map(key, value, _, _, _, _)                   =>
        mapEncoder(key, value).asInstanceOf[Encoder[A]] // TODO: handle non-native maps

      case r @ Reflect.Record(fields, _, _, _, _)                =>
        // TODO: Extract recordEncoder
        (a: A) => {
          // TODO: replace foldLeft with imperative loop
          val avMap = fields.foldLeft[AttributeValue.Map](AttributeValue.Map.empty) {
            case (acc: AttributeValue.Map, field) =>
              val fieldName                     = field.name
              val maybeLens: Option[Lens[A, _]] = r.lensByName(fieldName)
              if (maybeLens.isDefined) {
                val lens       = maybeLens.get
                val fieldValue = lens.get(a)
                val enc        = reflectEncoder(field.value)
                val av         = enc(fieldValue.asInstanceOf[field.value.Structure])

                field.value match {
                  // TODO: use type matching
                  case v @ Reflect.Variant(_, _, _, _, _) if isOption(v) && av == AttributeValue.Null =>
                    acc
                  case _                                                                              =>
                    acc + (fieldName -> av)
                }
              } else
                throw new Exception(s"Field $fieldName not found in record") // this should not happen
          }
          avMap
        }
      case v @ Reflect.Variant(cases, _, _, _, variantModifiers) =>
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
      case Reflect.Deferred(value)                               =>
        reflectEncoder(value())
      case r                                                     => throw new Exception(s"Could not encode $r just yet")
    }

  def encoder[A](implicit schema: Schema[A]): Encoder[A] = reflectEncoder(schema.reflect)

  // ================================================================================================

  private def decodeEitherValue[A](label: String, v: Reflect.Variant.Bound[A]): Decoder[A] =
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

  def eitherDecoder[A](v: Reflect.Variant.Bound[A]): Decoder[A] = {
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

  // Note that None decoding (AttributeValue.Null or missing field value) is done upstream
  // so we only focus on the Some case here
  def optionDecoder[A](v: Reflect.Variant.Bound[A]): Decoder[A] = { (av: AttributeValue) =>
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
                if (errors.isEmpty)
                  Right(constructor.construct(registers, RegisterOffset.Zero))
                else
                  Left(DecodingError(errors.mkString(", ")))

              case av                      =>
                Left(DecodingError(s"Could not decode $av just yet"))
            }
      case v @ Reflect.Variant(cases, typeName, _, _, variantModifiers) =>
        if (isOption(v))
          optionDecoder(v)
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
      case r                                                            =>
        (_: AttributeValue) => Left(DecodingError(s"Could not decode Reflect $r just yet"))
    }

  // TODO: handle all primitive types
  def primitiveDecoder[A](primitiveType: PrimitiveType[A]): Decoder[A] =
    (av: AttributeValue) => {
      primitiveType match {
        case PrimitiveType.String(_) => FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av)
        case PrimitiveType.Int(_)    => FromAttributeValue.intFromAttributeValue.fromAttributeValue(av)
        case _                       => Left(DecodingError("Could not decode"))
      }
    }

  def decoder[A](implicit schema: Schema[A]): Decoder[A] = reflectDecoder(schema.reflect)

}
