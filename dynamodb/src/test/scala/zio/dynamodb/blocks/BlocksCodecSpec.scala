package zio.dynamodb.blocks

import zio.Chunk
import zio.blocks.schema.Modifier.config
import zio.blocks.schema.binding.{ Binding, SeqConstructor, SeqDeconstructor }
import zio.blocks.schema.{ CompanionOptics, Doc, Lens, Namespace, PrimitiveType, Reflect, Schema, TypeName, Validation }
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb._
import zio.test.{ assertTrue, Spec, TestResult, ZIOSpecDefault }

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

  final case class Address(postcode: String, number: Int)
  object Address extends CompanionOptics[Address] {
    implicit val schema: Schema[Address] = Schema.derived

    val postcode: Lens[Address, String] = $(_.postcode)
    val number: Lens[Address, Int]      = $(_.number)
  }

  final case class RecordWithEnum(light: TrafficLight)
  object RecordWithEnum {
    implicit val schema: Schema[RecordWithEnum] = Schema.derived
  }

  final case class RecordWithNativeMap(
    map: Map[String, Int] = Map.empty
  )
  object RecordWithNativeMap            {
    implicit val schema: Schema[RecordWithNativeMap]               = Schema.derived
    implicit val zioSchema: zio.schema.Schema[RecordWithNativeMap] =
      zio.schema.DeriveSchema.gen[RecordWithNativeMap]
  }
  final case class RecordWithNonNativeMapOfInt(
    map: Map[Int, Int] = Map.empty
  )
  object RecordWithNonNativeMapOfInt    {
    implicit val zioSchema: zio.schema.Schema[RecordWithNonNativeMapOfInt] =
      zio.schema.DeriveSchema.gen[RecordWithNonNativeMapOfInt]
    implicit val schema: Schema[RecordWithNonNativeMapOfInt]               = Schema.derived
  }
  final case class RecordWithNonNativeMapOfPerson(
    map: Map[Int, Person] = Map.empty
  )
  object RecordWithNonNativeMapOfPerson {
    implicit val zioSchema: zio.schema.Schema[RecordWithNonNativeMapOfPerson] =
      zio.schema.DeriveSchema.gen[RecordWithNonNativeMapOfPerson]
    implicit val schema: Schema[RecordWithNonNativeMapOfPerson]               = Schema.derived
  }
  final case class RecordWithArray(
    // TODO: Avi - bottom out Array support in AttrMap/To/FromAttributeValue and equality checks
    names: Array[String] = Array.empty
  )                                     {
    override def equals(obj: Any): Boolean =
      obj match {
        case that: RecordWithArray =>
          this.names.toSeq == that.names.toSeq
        case _                     => false
      }

    override def hashCode(): Int =
      names.toSeq.hashCode()
  }
  object RecordWithArray                {
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

  final case class RecordWithTuple(tuple: (Int, Long, String, String))
  object RecordWithTuple {
    implicit val schema: Schema[RecordWithTuple]               = Schema.derived
    implicit val zioSchema: zio.schema.Schema[RecordWithTuple] =
      zio.schema.DeriveSchema.gen[RecordWithTuple]
  }

  final case class RecordWithListAsFirstInTuple(tuple: (List[Int], Long, String))
  object RecordWithListAsFirstInTuple {
    implicit val schema: Schema[RecordWithListAsFirstInTuple]               = Schema.derived
    implicit val zioSchema: zio.schema.Schema[RecordWithListAsFirstInTuple] =
      zio.schema.DeriveSchema.gen[RecordWithListAsFirstInTuple]
  }

  final case class RecordWithListAsSecondInTuple(tuple: (Int, List[Long], String))
  object RecordWithListAsSecondInTuple {
    implicit val schema: Schema[RecordWithListAsSecondInTuple]               = Schema.derived
    implicit val zioSchema: zio.schema.Schema[RecordWithListAsSecondInTuple] =
      zio.schema.DeriveSchema.gen[RecordWithListAsSecondInTuple]
  }

  final case class RecordWithTuple1(tuple: (Int))
  object RecordWithTuple1 {
    implicit val schema: Schema[RecordWithTuple1]               = Schema.derived
    implicit val zioSchema: zio.schema.Schema[RecordWithTuple1] =
      zio.schema.DeriveSchema.gen[RecordWithTuple1]
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
    implicit val schema: Schema[Person2]               = Schema.derived
    implicit val zioSchema: zio.schema.Schema[Person2] =
      zio.schema.DeriveSchema.gen[Person2]

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
    implicit val zioSchema: zio.schema.Schema[Person] =
      zio.schema.DeriveSchema.gen[Person]
    implicit val schema: Schema[Person]               = Schema.derived

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
    testWithCodecs("round trip Person2")(Person2.zioSchema, Person2.schema) { codec =>
      val expectedItem   = Item("id" -> "1", "age" -> 42, "count" -> 100)
      val expectedPerson = Person2("1", 42, 100)
      val enc            = codec.encoder(expectedPerson)
      val dec            = codec.decoder(enc)
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
    test("Record with Option[Int] None with required None via config") {
      final case class RecordWithOption2(id: String, option: Option[Int])
      object RecordWithOption2 {
        implicit val cfg: DynamoDBCodecConfig[RecordWithOption2] =
          (d: DynamoDBCodecDeriver) => d.withTransientNone(false)

        implicit val schema: Schema[RecordWithOption2] = Schema.derived
      }

      val expectedItem                          =
        Item("id" -> "1", "option" -> null)
      val codec: SchemaCodec[RecordWithOption2] = implicitly[SchemaCodec[RecordWithOption2]]
      val expectedPerson                        = RecordWithOption2("1", option = None)
      val enc                                   = codec.encoder(expectedPerson)
      val dec                                   = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with Option[Int] None with required None") {
      val expectedItem                           =
        Item("id" -> "1", "option" -> null)
      val codec: DynamoDBCodec[RecordWithOption] =
        RecordWithOption.schema.derive(DynamoDBCodecDeriver.withTransientNone(false))
      val expectedPerson                         = RecordWithOption("1", option = None)
      val enc                                    = codec.encoder(expectedPerson)
      val dec                                    = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with Option of record") {
      val expectedItem                                   =
        Item("option" -> Item("id" -> "id", "age" -> 21))
      val codec: DynamoDBCodec[RecordWithOptionalPerson] = RecordWithOptionalPerson.schema.derive(DynamoDBCodecDeriver)
      val person                                         = RecordWithOptionalPerson(option = Some(Person("id", 21)))
      val enc                                            = codec.encoder(person)
      val dec                                            = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(person))
    },
    suite("Native Map")(
      testWithCodecs("Record with native Map[String, Int]")(
        RecordWithNativeMap.zioSchema,
        RecordWithNativeMap.schema
      ) { codec =>
        val expectedItem   =
          Item("map" -> Map("a" -> 1, "b" -> 2))
        val expectedPerson = RecordWithNativeMap(map = Map("a" -> 1, "b" -> 2))
        val enc            = codec.encoder(expectedPerson)
        val dec            = codec.decoder(enc)
        assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
      },
      test("Record with native map of record - Map[String, Address]") {
        final case class RecordWithAddressMap(
          map: Map[String, Address]
        )
        object RecordWithAddressMap {
          implicit val schema: Schema[RecordWithAddressMap] = Schema.derived
        }

        val expectedItem                               =
          Item("map" -> Map("home" -> Item("postcode" -> "12345", "number" -> 10)))
        val codec: DynamoDBCodec[RecordWithAddressMap] = RecordWithAddressMap.schema.derive(DynamoDBCodecDeriver)
        val expectedPerson                             = RecordWithAddressMap(map = Map("home" -> Address("12345", 10)))
        val enc                                        = codec.encoder(expectedPerson)
        val dec                                        = codec.decoder(enc)
        assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
      }
    ),
    suite("non native Map")(
      testWithCodecs("Record with Map[Int, Int]")(
        RecordWithNonNativeMapOfInt.zioSchema,
        RecordWithNonNativeMapOfInt.schema
      ) { codec =>
        val expectedItem   = Item("map" -> List(List(1, 1), List(2, 2)))
        val expectedRecord = RecordWithNonNativeMapOfInt(map = Map(1 -> 1, 2 -> 2))
        val enc            = codec.encoder(expectedRecord)
        val dec            = codec.decoder(enc)
        assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedRecord))
      },
      testWithCodecs("Record with Map[Int, Person]")(
        RecordWithNonNativeMapOfPerson.zioSchema,
        RecordWithNonNativeMapOfPerson.schema
      ) { codec =>
        val expectedItem   = AttributeValue.Map(
          Map(
            AttributeValue.String("map") -> AttributeValue.List(
              Chunk(
                AttributeValue.List(
                  Chunk(
                    AttributeValue.Number(BigDecimal(1)),
                    AttributeValue.Map(
                      Map(
                        AttributeValue.String("id")  -> AttributeValue.String("id"),
                        AttributeValue.String("age") -> AttributeValue.Number(
                          BigDecimal(21)
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
        val expectedRecord = RecordWithNonNativeMapOfPerson(Map(1 -> Person("id", 21)))
        val enc            = codec.encoder(expectedRecord)
        val dec            = codec.decoder(enc)
        assertTrue(enc == expectedItem && dec == Right(expectedRecord))
      }
    ),
    suite("tuple")(
      // Schema2 encoding will never be symmetric with Schema1
      test("record with tuple (Int, Long, String)") {
        val codec        = SchemaCodec.schema2ToSchemaCodec(RecordWithTuple.schema, DynamoDBCodecConfig.identity)
        val expectedItem =
          AttributeValue.Map(
            Map(
              AttributeValue.String("tuple") -> AttributeValue.List(
                Chunk(
                  AttributeValue.Number(BigDecimal(1)),
                  AttributeValue.Number(BigDecimal(2L)),
                  AttributeValue.String("3"),
                  AttributeValue.String("4")
                )
              )
            )
          )

        val expectedPerson = RecordWithTuple(tuple = (1, 2, "3", "4"))
        val enc            = codec.encoder(expectedPerson)
        val dec            = codec.decoder(enc)
        assertTrue(enc == expectedItem && dec == Right(expectedPerson))
      },
      test("tuple compatibility - nested lists") {
        val blocksCodec    = SchemaCodec.schema2ToSchemaCodec(RecordWithTuple.schema, DynamoDBCodecConfig.identity)
        val zioSchemaCodec = SchemaCodec.schema1ToSchemaCodec(RecordWithTuple.zioSchema)

        val recordWithTuple = RecordWithTuple(tuple = (1, 2, "3", "4"))
        val av              = zioSchemaCodec.encoder(recordWithTuple)
        val a               = blocksCodec.decoder(av)
        assertTrue(a == Right(recordWithTuple)) // Blocks codec can decode a tuple encoded by a ZIO Schema codec
      },
      test("tuple compatibility - single scalar value for Tuple1") {
        val blocksCodec    = SchemaCodec.schema2ToSchemaCodec(RecordWithTuple1.schema, DynamoDBCodecConfig.identity)
        val zioSchemaCodec = SchemaCodec.schema1ToSchemaCodec(RecordWithTuple1.zioSchema)

        val recordWithTuple = RecordWithTuple1(tuple = (1))
        val av              = zioSchemaCodec.encoder(recordWithTuple)
        val a               = blocksCodec.decoder(av)
        assertTrue(a == Right(recordWithTuple)) // Blocks codec can decode a tuple encoded by a ZIO Schema codec
      },
      test("tuple compatibility - tuple with first element as List") {
        val blocksCodec    =
          SchemaCodec.schema2ToSchemaCodec(RecordWithListAsFirstInTuple.schema, DynamoDBCodecConfig.identity)
        val zioSchemaCodec = SchemaCodec.schema1ToSchemaCodec(RecordWithListAsFirstInTuple.zioSchema)

        val recordWithTuple = RecordWithListAsFirstInTuple(tuple = (List(1, 2), 2L, "3"))
        val av1             = zioSchemaCodec.encoder(recordWithTuple)
        val dec2            = blocksCodec.decoder(av1)

        assertTrue(dec2 == Right(recordWithTuple))
      },
      test("tuple compatibility - tuple with second element as List") {
        val blocksCodec    =
          SchemaCodec.schema2ToSchemaCodec(RecordWithListAsSecondInTuple.schema, DynamoDBCodecConfig.identity)
        val zioSchemaCodec = SchemaCodec.schema1ToSchemaCodec(RecordWithListAsSecondInTuple.zioSchema)

        val recordWithTuple = RecordWithListAsSecondInTuple(tuple = (1, List(1L, 2L), "3"))
        val av1             = zioSchemaCodec.encoder(recordWithTuple)
        val dec2            = blocksCodec.decoder(av1)

        assertTrue(dec2 == Right(recordWithTuple))
      }
    ),
    suite("sequence")(
      // Note ZIO Schema does not work with Arrays
      test("record with Array[String]") {
        val expectedItem                          =
          AttributeValue.Map(
            Map(
              AttributeValue.String("names") -> AttributeValue.List(
                Chunk(
                  AttributeValue.String("Alice"),
                  AttributeValue.String("Bob"),
                  AttributeValue.String("Tharloachan")
                )
              )
            )
          )
        val codec: DynamoDBCodec[RecordWithArray] = RecordWithArray.schema.derive(DynamoDBCodecDeriver)
        val expectedPerson                        = RecordWithArray(names = Array("Alice", "Bob", "Tharloachan"))
        val enc                                   = codec.encoder(expectedPerson)
        val dec                                   = codec.decoder(enc)
        assertTrue(enc == expectedItem && dec == Right(expectedPerson))
      }
    )
  )

  def testWithCodecs[A](
    name: String
  )(
    zioSchema: zio.schema.Schema[A],
    blocks: Schema[A],
    cfg: DynamoDBCodecConfig[A] = DynamoDBCodecConfig.identity[A]
  )(
    testBody: SchemaCodec[A] => TestResult
  ): Spec[Any, Nothing] = {

    val scBlocks = SchemaCodec.schema2ToSchemaCodec(blocks, cfg)
    val scZio    = SchemaCodec.schema1ToSchemaCodec(zioSchema)

    suite(name)(
      test("zio-schema") {
        testBody(scZio)
      },
      test("blocks-schema") {
        testBody(scBlocks)
      }
    )
  }
}
