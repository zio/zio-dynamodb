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

  def optionEncoderOld[A](encoder: Encoder[A]): Encoder[Option[A]] =
    (a: Option[A]) =>
      a match {
        case None        => AttributeValue.Null
        case Some(value) => encoder(value)
      }

  def optionEncoder[S, A](a: A, term: Term.Bound[S, A]): AttributeValue =
    term.value match {
      case r @ Reflect.Record(
            fields,
            _,
            _,
            _,
            _
          ) => // "default" vs "compact" encoding. Variant instance is a Record
        if (r.typeName.name == "Some" && fields.length == 1) { // TODO: do more checks for Some here like package name
          val valueField = fields(0)
          // there is no native DDB Option type, so we need to dig into the schema of the "value" field in "Some"
          a match {
            case Some(value) =>
              valueField match {
                case Term(name, value2, _, _) =>
                  val enc = reflectEncoder(value2)
                  enc(value.asInstanceOf[value2.Structure])
              }
            case _           =>
              throw new Exception(s"Expected Some but found None")
          }
        } else
          AttributeValue.Null // TODO
      case _ =>
        throw new Exception(s"Unsupported term.value for optionEncoder: ${term.value}")
    }
  def optionEncoder2[A](a: A, record: Reflect.Record.Bound[A]): AttributeValue = {
    val av = (record.typeName.name, record.fields.length) match {
      case ("Some", 1)   =>
        val valueField = record.fields(0)
        a match {
          case Some(value) =>
            valueField match {
              case Term(name, value2, _, _) =>
                val enc = reflectEncoder(value2)
                enc(value.asInstanceOf[value2.Structure])
            }
          case _           =>
            throw new Exception(s"Expected Some but found None")
        }
      case ("None", 0)   =>
        AttributeValue.Null
      case (typeName, _) =>
        throw new Exception(s"Could not encode Option for type $typeName")
    }
    av
  }

  def optionEncoder3[A](record: Reflect.Record.Bound[A]): Encoder[A] =
    (record.typeName.name, record.fields.length) match {
      case ("Some", 1)   =>
        (a: A) => {
          val valueField = record.fields(0)
          a match {
            case Some(value) =>
              valueField match {
                case Term(name, value2, _, _) =>
                  val enc = reflectEncoder(value2)
                  enc(value.asInstanceOf[value2.Structure])
              }
            case _           =>
              throw new Exception(s"Expected Some but found None")
          }
        }
      case ("None", 0)   => _ => AttributeValue.Null
      case (typeName, _) => throw new Exception(s"Could not encode Option for type $typeName")
    }

  // SCHEMA V1
  // private def optionalEncoder[A](encoder: Encoder[A]): Encoder[Option[A]] = {
  //   case None        => AttributeValue.Null
  //   case Some(value) => encoder(value)
  // }

  def reflectEncoder[A](reflect: Reflect.Bound[A]): Encoder[A] =
    reflect match {
      case Reflect.Primitive(primitiveType, _, _, _, _)                 =>
        primitiveEncoder(primitiveType)
      case Reflect.Map(key, value, _, _, _, _)                          =>
        mapEncoder(key, value).asInstanceOf[Encoder[A]] // TODO: handle non-native maps

      case r @ Reflect.Record(fields, _, _, _, _)                       =>
//        recordEncoder(r) // TODO: handle empty records (case objects)
        // TODO: Extract recordEncoder
        (a: A) => {
          val avMap = fields.foldLeft[AttributeValue.Map](AttributeValue.Map.empty) {
            case (acc: AttributeValue.Map, field) =>
              val fieldName        = field.name
              val lens: Lens[A, _] = r.lensByName(fieldName).get // TODO: handle error
              val fieldValue       = lens.get(a)                 // if "a" is a Some(x) we need to deal with x and schema of x somehow
              val enc              = reflectEncoder(field.value)
              val av               = enc(fieldValue.asInstanceOf[field.value.Structure])

              /*
          @tailrec
          def appendToMap[B](schema: Schema[B]): AttributeValue.Map =
            schema match {
              case l @ Schema.Lazy(_)                                                 =>
                appendToMap(l.schema)
              case _: Schema.Optional[_] if av.isInstanceOf[AttributeValue.Null.type] =>
                AttributeValue.Map(s._2.value)
              case _                                                                  =>
                AttributeValue.Map(s._2.value + (AttributeValue.String(k) -> av))
            }

          appendToMap(s._1.schema)
               */

              field.value match {
                case Reflect.Variant(_, typeName, _, _, _) if typeName.name == "Option" && av == AttributeValue.Null =>
                  acc
                case _                                                                                               =>
                  acc + (fieldName -> av)
              }
          }
          avMap
        }
      case v @ Reflect.Variant(cases, typeName, _, _, variantModifiers) =>
        (a: A) =>
          val idx                = v.discriminator.discriminate(a)
          val case_              = cases(idx)
          val isOption           = typeName.name == "Option"
          val av: AttributeValue = case_.value match {
            case r: Reflect.Record.Bound[aa] => // "default" vs "compact" encoding. Variant instance is a Record
              // TODO: Note "None" is a case object and is dealt with upstream so not expected here
              if (r.fields.isEmpty && !isOption) {
                // empty fields implies a case object
                AttributeValue.String(case_.name)
              } else if (isOption) { // TODO: do more checks for Some here like package name
                optionEncoder2[aa](a.asInstanceOf[aa], r)
              } else {
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
                    AttributeValue.Map(newMap)
                  case None           =>
                    // tagged Variant encoding
                    AttributeValue.Map(case_.name, av)
                }
              }
            case r                           =>
              throw new Exception(s"Did not expect Reflect $r - only Record is valid")
          }
          av
      case Reflect.Deferred(value)                                      =>
        reflectEncoder(value())
      case r                                                            => throw new Exception(s"Could not encode $r just yet")
    }

  def encoder[A](implicit schema: Schema[A]): Encoder[A] = reflectEncoder(schema.reflect)

  // ================================================================================================

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
                    val (isOption, cases)                  = field.value match {
                      case Reflect.Variant(cases, typeName, _, _, _) if typeName.name == "Option" => (true, cases)
                      case _                                                                      => (false, Vector.empty)
                    }
                    val fieldName                          = field.name
                    val fieldValue: Option[AttributeValue] = map.get(AttributeValue.String(fieldName))

                    val isNone = isOption && (fieldValue.isEmpty || fieldValue == Some(AttributeValue.Null))

                    if (isNone)
                      Right(None)
                    else if (!fieldValue.isEmpty && isOption) {
                      // we dealing with the Some case of Option Variant
                      // so we can sort cut decoding of Option Variant to decoding of the value field of the Some case
                      val case_ = cases.find(_.name == "Some").get // TODO - is there a better way to do this?
                      case_.value match {
                        case Reflect.Record(fields, _, _, _, _) if fields.size == 1 =>
                          fields(0) match {
                            case Term(_, value, _, _) =>
                              val dec = reflectDecoder(value)
                              dec(fieldValue.get) match { // TODO: Naked get on Option
                                case Left(e)      =>
                                  addError(s"Field $fieldName: ${e.getMessage}")
                                case Right(value) =>
                                  r.registers(idx)
                                    .asInstanceOf[Register[Any]]
                                    .set(registers, RegisterOffset.Zero, Some(value))
                              }
                          }
                        case _                                                      => addError(s"Expected a record with a single field for Some")
                      }
                    } else if (fieldValue.isEmpty)
                      addError(s"Field $fieldName not found")
                    else {
                      val dec = reflectDecoder(field.value)
                      dec(fieldValue.get) match { // TODO: Naked get on Option
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
