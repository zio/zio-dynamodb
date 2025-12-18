package zio.dynamodb.blocks

import zio.Chunk
import zio.blocks.schema.Modifier.config
import zio.blocks.schema.binding.{ Binding, SeqConstructor, SeqDeconstructor }
import zio.blocks.schema.{ CompanionOptics, Doc, Lens, Namespace, PrimitiveType, Reflect, Schema, TypeName, Validation }
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.{ AttributeValue, Decoder, Encoder, Item }
import zio.test.{ assertTrue, ZIOSpecDefault }

object BlocksCodecSpec extends ZIOSpecDefault {
  sealed trait TrafficLight
  object TrafficLight {
    case object Red    extends TrafficLight
    case object Yellow extends TrafficLight
    case object Green  extends TrafficLight

    implicit val schema: Schema[TrafficLight] = Schema.derived
  }

  sealed trait PaymentMethod
  object PaymentMethod           {
    final case class CreditCard(number: String, cvv: String) extends PaymentMethod
    object CreditCard {
      implicit val schema: Schema[CreditCard] = Schema.derived
    }
    final case class PayPal(email: String) extends PaymentMethod
    object PayPal     {
      implicit val schema: Schema[PayPal] = Schema.derived
    }

    implicit val schema: Schema[PaymentMethod] = Schema.derived
  }
  final case class RecordWithPaymentMethod(method: PaymentMethod)
  object RecordWithPaymentMethod {
    implicit val schema: Schema[RecordWithPaymentMethod] = Schema.derived
  }

  sealed trait PaymentMethod2
  object PaymentMethod2           {
    final case class CreditCard(number: String, cvv: String) extends PaymentMethod2
    object CreditCard {
      implicit val schema: Schema[CreditCard] = Schema.derived
    }
    final case class PayPal(email: String) extends PaymentMethod2
    object PayPal     {
      implicit val schema: Schema[PayPal] = Schema.derived
    }

    implicit val schema: Schema[PaymentMethod2] = Schema.derived.modifier(config("discriminatorName", "discriminator"))
  }
  final case class RecordWithPaymentMethod2(method: PaymentMethod2)
  object RecordWithPaymentMethod2 {
    implicit val schema: Schema[RecordWithPaymentMethod2] = Schema.derived
  }

  final case class RecordWithEnum(light: TrafficLight)
  object RecordWithEnum {
    implicit val schema: Schema[RecordWithEnum] = Schema.derived
  }

  final case class RecordWithCollections(
    numbers: List[Int] = Nil,
    map: Map[String, Int] = Map.empty
  )
  object RecordWithCollections  {
    implicit val schema: Schema[RecordWithCollections] = Schema.derived
  }
  final case class RecordWithNonNativeMap(
    map: Map[Int, Int] = Map.empty
  )
  object RecordWithNonNativeMap {
    implicit val schema: Schema[RecordWithNonNativeMap] = Schema.derived
  }
  final case class RecordWithArray(
    // TODO: Avi - bottom out Array support in AttrMap/To/FromAttributeValue and equality checks
    names: Array[String] = Array.empty
  )                             {
    override def equals(obj: Any): Boolean =
      obj match {
        case that: RecordWithArray =>
          this.names.toSeq == that.names.toSeq
        case _                     => false
      }

    override def hashCode(): Int =
      names.toSeq.hashCode()
  }
  object RecordWithArray        {
    implicit val schema: Schema[RecordWithArray] = Schema.derived
  }
  final case class RecordWithEither(either: Either[String, Int])
  object RecordWithEither extends CompanionOptics[RecordWithEither] {
    implicit val schema: Schema[RecordWithEither]                = Schema.derived
    val either /*: Lens[RecordWithEither, Either[String, Int]]*/ = $(_.either)
  }

  final case class RecordWithOption(id: String, option: Option[Int])
  object RecordWithOption {
    implicit val schema: Schema[RecordWithOption] = Schema.derived
  }

  final case class RecordWithTuple(tuple: (Int, Int, Int))
  object RecordWithTuple {
    implicit val schema: Schema[RecordWithTuple] = Schema.derived
  }

  final case class RecordWithOptionalPerson(option: Option[Person])
  object RecordWithOptionalPerson {
    implicit val schema: Schema[RecordWithOptionalPerson] = Schema.derived
  }

  final case class RecordWithStringSet(set: Set[String])
  object RecordWithStringSet {
    implicit val schema: Schema[RecordWithStringSet] = Schema.derived
  }
  final case class RecordWithNumberSet(set: Set[Int])
  object RecordWithNumberSet {
    implicit val schema: Schema[RecordWithNumberSet] = Schema.derived
  }

  final case class RecordWithNonNativeSet(set: Set[Person])
  object RecordWithNonNativeSet {
    implicit val schema: Schema[RecordWithNonNativeSet] = Schema.derived
  }

  // Blocks has zero dependency so we have to derive schema for Chunk
  // Code taken from comment by ghostdogpr on issue https://github.com/zio/zio-blocks/issues/447
  final case class RecordWithNativeBinarySet(set: Set[Chunk[Byte]])
  object RecordWithNativeBinarySet {
    val chunkConstructor: SeqConstructor[Chunk] = new SeqConstructor.Boxed[Chunk] {
      type ObjectBuilder[A] = zio.ChunkBuilder[A]
      def newObjectBuilder[A](sizeHint: Int): ObjectBuilder[A] = zio.ChunkBuilder.make(sizeHint)
      def addObject[A](builder: ObjectBuilder[A], a: A): Unit  = builder.addOne(a)
      def resultObject[A](builder: ObjectBuilder[A]): Chunk[A] = builder.result()
    }

    val chunkDeconstructor: SeqDeconstructor[Chunk] = new SeqDeconstructor[Chunk] {
      def deconstruct[A](c: Chunk[A]): Iterator[A] = c.iterator
      def size[A](c: Chunk[A]): Int                = c.length
    }

    implicit def schemaChunk[V](implicit ev: Schema[V]): Schema[Chunk[V]] =
      new Schema(
        new Reflect.Sequence[Binding, V, Chunk](
          ev.reflect,
          TypeName(Namespace("zio" :: Nil, Nil), "Chunk"),
          new Binding.Seq(chunkConstructor, chunkDeconstructor)
        )
      )

    implicit val schema: Schema[RecordWithNativeBinarySet] = Schema.derived
  }

  final case class Person2(id: String, age: Int, count: Long)
  object Person2 extends CompanionOptics[Person2] {
    implicit val schema: Schema[Person2] = Schema.derived

    val id: Lens[Person2, String] = $(_.id)
  }

  val stringSchema = new Schema(
    Reflect.Primitive(
      primitiveType = PrimitiveType.String(Validation.None),
      typeName = TypeName(Namespace("scala" :: Nil, Nil), "String"),
      primitiveBinding = Binding.Primitive.string,
      doc = Doc.Empty,
      modifiers = Seq.empty
    )
  )

  final case class Person(id: String, age: Long)
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived

    val id: Lens[Person, String] = $(_.id)
  }

  val spec = suite("BlocksSpec")(
    test("investigate field codec override") {
      val codecToUpper: DynamoDBCodec[String] = new DynamoDBCodec[String] {
        override def encoder: Encoder[String] =
          s => {
            AttributeValue.String(s.toUpperCase)
          }

        override def decoder: Decoder[String] = {
          case AttributeValue.String(s) => Right(s + "_decoded")
          case other                    => Left(DecodingError(s"Expected String attribute value but got: $other"))
        }
      }
      val expectedAv                          = Item("id" -> "ONE", "age" -> 21L, "count" -> 100).toAttributeValue
      def codec: DynamoDBCodec[Person2]       =
        Person2.schema
          .deriving(DynamoDBCodecDeriver)
          .instance(Person2.id, codecToUpper)
          .derive
      val person                              = Person2("one", 21, 100L)
      val _                                   = codec.encoder(person)
      val enc                                 = codec.encoder(person)
      val dec                                 = codec.decoder(enc)
      assertTrue(enc == expectedAv && dec == Right(person.copy(id = "ONE_decoded")))
    },
    test("round trip Person2") {
      val expectedItem                  = Item("id" -> "1", "age" -> 42, "count" -> 100)
      val codec: DynamoDBCodec[Person2] = Person2.schema.derive(DynamoDBCodecDeriver)
      val expectedPerson                = Person2("1", 42, 100)
      val enc                           = codec.encoder(expectedPerson)
      val dec                           = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with Option[Int] Some(42)") {
      val expectedItem                           =
        Item("id" -> "1", "option" -> 42)
      val codec: DynamoDBCodec[RecordWithOption] = RecordWithOption.schema.derive(DynamoDBCodecDeriver)
      val expectedPerson                         = RecordWithOption("1", option = Some(42))
      val enc                                    = codec.encoder(expectedPerson)
      val dec                                    = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with Option[Int] None") {
      val expectedItem                           =
        Item("id" -> "1")
      val codec: DynamoDBCodec[RecordWithOption] = RecordWithOption.schema.derive(DynamoDBCodecDeriver)
      val expectedPerson                         = RecordWithOption("1", option = None)
      val enc                                    = codec.encoder(expectedPerson)
      val dec                                    = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with Option[Int] None with required None") {
      val expectedItem                           =
        Item("id" -> "1", "option" -> null)
      val codec: DynamoDBCodec[RecordWithOption] = RecordWithOption.schema.derive(DynamoDBCodecDeriver.withTransientNone(false))
      val expectedPerson                         = RecordWithOption("1", option = None)
      val enc                                    = codec.encoder(expectedPerson)
      val dec                                    = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    }
  )

}
