package zio.dynamodb.blocks

import zio.dynamodb.AttributeValue
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.blocks.schema._
import zio.Chunk

/*
  case class Primitive[F[_, _], A](
    primitiveType: PrimitiveType[A],
    primitiveBinding: F[BindingType.Primitive, A],
    typeName: TypeName[A],
    doc: Doc = Doc.Empty,
    modifiers: Seq[Modifier.Primitive] = Vector()
  ) extends Reflect[F, A] { self =>

  case class Record[F[_, _], A](
    fields: Seq[Term[F, A, ?]],
    typeName: TypeName[A],
    recordBinding: F[BindingType.Record, A],
    doc: Doc = Doc.Empty,
    modifiers: Seq[Modifier.Record] = Vector()
  ) extends Reflect[F, A] { self =>

  final case class Term[F[_, _], S, A](name: String, value: Reflect[F, A], doc: Doc, modifiers: Seq[Modifier.Term])
 */
object BlocksCodecViaDynamic extends App {

  @Modifier.config("discriminator", "disc")
  sealed trait Variant1

  object Variant1 extends CompanionOptics[Variant1] {
    implicit val schema: Schema[Variant1]         = Schema.derived
    lazy val variant1: Prism[Variant1, Variant1A] = optic(_.when[Variant1A])
    lazy val variant2: Prism[Variant1, Variant1B] = optic(_.when[Variant1B])
  }

  final case class Variant1A(a: Int, b: String) extends Variant1
  object Variant1A {
    implicit val schema: Schema[Variant1A] = Schema.derived
  }
  final case class Variant1B(b: String) extends Variant1
  object Variant1B {
    implicit val schema: Schema[Variant1B] = Schema.derived
  }
  /*
  Record(Vector((id,Primitive(Int(1))), (variant1,Variant(Variant1.Variant1A,Record(Vector((a,Primitive(Int(1))), (b,Primitive(String(John Doe)))))))))
   */
  final case class PersonWithVariant(id: Int, variant1: Variant1)
  object PersonWithVariant extends CompanionOptics[PersonWithVariant] {
    implicit val schema: Schema[PersonWithVariant] = Schema.derived[PersonWithVariant]
    val id: Lens[PersonWithVariant, Int]           = optic(_.id)
    val name: Lens[PersonWithVariant, Variant1]    = optic(_.variant1)
  }

  final case class Person(id: Int, name: String)
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived
    val id: Lens[Person, Int]           = optic(_.id)
    val name: Lens[Person, String]      = optic(_.name)
  }

  type Encoder[A]  = A => AttributeValue
  type Decoder[+A] = AttributeValue => Either[ItemError, A]

  /*
  problem with using Dynamic is we lose access to Modifiers
   */
  def reflectEncoder[A](schema: Schema[A]): Encoder[A] =
    (a: A) => {
      val dv                 = schema.toDynamicValue(a)
      val av: AttributeValue = dynamicEncoder(dv)
      av
    }

  def encoder[A](implicit schema: Schema[A]): Encoder[A] = reflectEncoder(schema)

  /*
  // ========================================================
  DynamicValue.Primitive(value: PrimitiveValue)
  // ========================================================
  sealed trait PrimitiveValue {
    type Type

    def primitiveType: PrimitiveType[Type]
  }
  sealed trait Val extends PrimitiveValue {
    type Type <: AnyVal
  }
  sealed trait Ref extends PrimitiveValue {
    type Type <: AnyRef
  }
  case class String(value: Predef.String) extends Ref {
    type Type = Predef.String

    def primitiveType: PrimitiveType[Predef.String] = PrimitiveType.String(Validation.None)
  }
  case class Int(value: scala.Int) extends Val {
    type Type = scala.Int

    def primitiveType: PrimitiveType[scala.Int] = PrimitiveType.Int(Validation.None)
  }
  // ========================================================

   */
  def dvPrimitiveEncoder(dvPrimitive: DynamicValue.Primitive): Encoder[DynamicValue] =
    (a: DynamicValue) => {
      println(s"Encoding DynamicValue.Primitive $a $dvPrimitive")
      dvPrimitive.value match {
        case PrimitiveValue.String(s) => AttributeValue.String(s)
        case PrimitiveValue.Int(i)    => AttributeValue.Number(BigDecimal(i.toString))
        case pv                       => throw new Exception(s"Could not encode PrimitiveValue $pv")
      }
    }

  def primitiveEncoder(primitive: PrimitiveValue): AttributeValue =
    primitive match {
      case PrimitiveValue.String(s) => AttributeValue.String(s)
      case PrimitiveValue.Int(i)    => AttributeValue.Number(BigDecimal(i.toString))
      case pv                       => throw new Exception(s"Could not encode PrimitiveValue $pv")
    }

  // Record(Vector((id,Primitive(Int(1))), (variant1,Variant(Variant1.Variant1A,Record(Vector((a,Primitive(Int(1))), (b,Primitive(String(John Doe)))))))))
  def dynamicEncoder[A]: Encoder[DynamicValue] = {
    val directDynamic = true
    println(s"EEEEEEEEEEE directDynamic: $directDynamic")

    if (directDynamic) { (dv: DynamicValue) =>
      dv match {
        case DynamicValue.Primitive(value)     =>
          primitiveEncoder(value)
        case DynamicValue.Record(fields)       =>
          val avs = fields.map {
            case (k, v) =>
              val av = dynamicEncoder(v)
              AttributeValue.String(k) -> av
          }
          AttributeValue.Map(avs.toMap)
        case DynamicValue.Variant(variant, dv) =>
          println(s"EEEEEEEEEEEE variant: $variant")
          variantEncoder(dv)
        case dv                                =>
          throw new Exception(s"Unsupported DynamicValue $dv")
      }
    } else
      throw new Exception("Can not currently process directDynamic is false")

  }

  /*
  TODO: how do we encode a user defined variant discriminator?
  Top level Variants in DDB always need a discriminator
   */
  // Record(Vector((id,Primitive(Int(1))), (variant1,Variant(Variant1.Variant1A,Record(Vector((a,Primitive(Int(1))), (b,Primitive(String(John Doe)))))))))
  def variantEncoder(dv: DynamicValue): AttributeValue = {
    println(dv)
    AttributeValue.String("TODO")
  }

  // ================================================================================================
  def reflectDecoder[A](schema: Schema[A]): Decoder[A]   =
    (av: AttributeValue) => {
      dynamicDecoder(av) match {
        case Left(e)   => Left(e)
        case Right(dv) =>
          schema.fromDynamicValue(dv) match {
            case Left(e)  => Left(DecodingError(e.toString))
            case Right(r) => Right(r)
          }
      }
    }
  def decoder[A](implicit schema: Schema[A]): Decoder[A] = reflectDecoder(schema)

  def primitiveDecoder[A](primitive: PrimitiveValue): Either[DecodingError, A] =
    primitive match {
      case PrimitiveValue.String(s) => Right(s.asInstanceOf[A])
      case PrimitiveValue.Int(i)    => Right(i.asInstanceOf[A])
      case pv                       => Left(DecodingError(s"Could not decode PrimitiveValue $pv"))
    }

  def dynamicDecoder: Decoder[DynamicValue] =
    (av: AttributeValue) => {
      av match {
        case AttributeValue.String(s) =>
          Right(DynamicValue.Primitive(PrimitiveValue.String(s)))
        case AttributeValue.Number(n) =>
          Right(DynamicValue.Primitive(PrimitiveValue.Int(n.toInt)))
        case AttributeValue.Map(m)    =>
          //val fields = m.map { case (k, v) => k.value -> dynamicDecoder(v) }
          val (errors, fields) = m.foldLeft[(Chunk[String], Vector[(String, DynamicValue)])](
            (Chunk.empty[String], Vector.empty[(String, DynamicValue)])
          ) {
            case ((acc, x), (k, v)) =>
              val (k2, v2) = (k.value, dynamicDecoder(v))
              v2 match {
                case Left(e)   => (acc :+ s"$k2: ${e.getMessage}", x)
                case Right(vv) => (acc, x :+ (k2 -> vv))
              }
          }
          if (errors.isEmpty)
            Right(DynamicValue.Record(fields))
          else
            Left(ItemError.DecodingError(errors.mkString(", ")))
        case av                       =>
          Left(ItemError.DecodingError(s"Unsupported AttributeValue $av"))
      }
    }

  // ================================================================================================

  val person        = Person(1, "John Doe")
  val enc           = encoder[Person]
  val av            = enc(person)
  println(s"Person $person encoded: $av")
  val dec           = decoder[Person]
  val personDecoded = dec(av)
  println(s"Person $person decoded: $personDecoded")

  val variant1A = Variant1A(1, "John Doe")
  val person2   = PersonWithVariant(1, variant1A)
  val dv        = PersonWithVariant.schema.toDynamicValue(person2)
  println(s"person2 $person2 encoded to a DynamicValue : $dv")

  // TODO: Monitor library fixes for this
  // [error] Exception in thread "main" java.util.NoSuchElementException: None.
  //println(s"person2 - Variant1.variant1.getOption ${Variant1.variant1.getOption(person2.variant1)}")
}
