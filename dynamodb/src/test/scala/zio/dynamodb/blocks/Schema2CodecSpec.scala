package zio.dynamodb.blocks

import zio.{ schema, Chunk }
import zio.blocks.schema.binding.{ Binding, SeqConstructor, SeqDeconstructor }
import zio.blocks.schema.derive.DerivationBuilder
import zio.blocks.schema.{
  CompanionOptics,
  Doc,
  Lens,
  Modifier,
  Namespace,
  PrimitiveType,
  Reflect,
  Schema,
  TypeName,
  Validation
}
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb._
import zio.schema.annotation.{ discriminatorName, noDiscriminator }
import zio.test.Assertion.{ equalTo, isLeft }
import zio.test.{ assert, assertTrue, Spec, TestResult, ZIOSpecDefault }

object Schema2CodecSpec extends ZIOSpecDefault {
  case class RecordWithUnit(unit: Unit)
  object RecordWithUnit {
    implicit val schema1: schema.Schema[RecordWithUnit] = zio.schema.DeriveSchema.gen[RecordWithUnit]
    implicit val schema2: Schema[RecordWithUnit]        = Schema.derived
  }

  sealed trait TrafficLight
  object TrafficLight {
    case object Red    extends TrafficLight
    case object Yellow extends TrafficLight
    case object Green  extends TrafficLight

    implicit val schema2: Schema[TrafficLight] = Schema.derived
  }

  /** uses DiscriminatorKind.Key in tests */
  sealed trait PaymentMethod
  object PaymentMethod {
    final case class CreditCard(number: String, cvv: String) extends PaymentMethod
    final case class PayPal(email: String)                   extends PaymentMethod
  }

  /** uses DiscriminatorKind.Field in tests */
  @discriminatorName("foo")
  sealed trait PaymentMethod2
  object PaymentMethod2 {
    final case class CreditCard(number: String, cvv: String) extends PaymentMethod2
    final case class PayPal(email: String)                   extends PaymentMethod2
  }

  /** uses DiscriminatorKind.None in tests */
  @noDiscriminator
  sealed trait PaymentMethod3
  object PaymentMethod3 {
    final case class CreditCard(number: String, cvv: String) extends PaymentMethod3
    final case class PayPal(email: String)                   extends PaymentMethod3
  }

  /** uses DiscriminatorKind.Key in tests */
  final case class RecordWithPaymentMethodUsingKey(method: PaymentMethod)
  object RecordWithPaymentMethodUsingKey {
    implicit val schema2: Schema[RecordWithPaymentMethodUsingKey]            = Schema.derived
    implicit val schema1: zio.schema.Schema[RecordWithPaymentMethodUsingKey] =
      zio.schema.DeriveSchema.gen[RecordWithPaymentMethodUsingKey]
  }

  /** uses DiscriminatorKind.Field in tests */
  final case class RecordWithPaymentMethodUsingField(method: PaymentMethod2)
  object RecordWithPaymentMethodUsingField {
    implicit val schema2: Schema[RecordWithPaymentMethodUsingField]            = Schema.derived
    implicit val schema1: zio.schema.Schema[RecordWithPaymentMethodUsingField] =
      zio.schema.DeriveSchema.gen[RecordWithPaymentMethodUsingField]
  }

  /** uses DiscriminatorKind.None in tests */
  final case class RecordWithPaymentMethodUsingNone(method: PaymentMethod3)
  object RecordWithPaymentMethodUsingNone {
    implicit val schema2: Schema[RecordWithPaymentMethodUsingNone]            = Schema.derived
    implicit val schema1: zio.schema.Schema[RecordWithPaymentMethodUsingNone] =
      zio.schema.DeriveSchema.gen[RecordWithPaymentMethodUsingNone]
  }

  final case class Address(postcode: String, number: Int)
  object Address extends CompanionOptics[Address] {
    implicit val schema2: Schema[Address] = Schema.derived

    val postcode: Lens[Address, String] = $(_.postcode)
    val number: Lens[Address, Int]      = $(_.number)
  }

  final case class RecordWithEnum(light: TrafficLight)
  object RecordWithEnum {
    implicit val schema2: Schema[RecordWithEnum]            = Schema.derived
    implicit val schema1: zio.schema.Schema[RecordWithEnum] =
      zio.schema.DeriveSchema.gen[RecordWithEnum]
  }

  final case class RecordWithNativeMap(
    map: Map[String, Int] = Map.empty
  )
  object RecordWithNativeMap            {
    implicit val schema2: Schema[RecordWithNativeMap]            = Schema.derived
    implicit val schema1: zio.schema.Schema[RecordWithNativeMap] =
      zio.schema.DeriveSchema.gen[RecordWithNativeMap]
  }
  final case class RecordWithNonNativeMapOfInt(
    map: Map[Int, Int] = Map.empty
  )
  object RecordWithNonNativeMapOfInt    {
    implicit val schema1: zio.schema.Schema[RecordWithNonNativeMapOfInt] =
      zio.schema.DeriveSchema.gen[RecordWithNonNativeMapOfInt]
    implicit val schema2: Schema[RecordWithNonNativeMapOfInt]            = Schema.derived
  }
  final case class RecordWithNonNativeMapOfPerson(
    map: Map[Int, Person] = Map.empty
  )
  object RecordWithNonNativeMapOfPerson {
    implicit val schema1: zio.schema.Schema[RecordWithNonNativeMapOfPerson] =
      zio.schema.DeriveSchema.gen[RecordWithNonNativeMapOfPerson]
    implicit val schema2: Schema[RecordWithNonNativeMapOfPerson]            = Schema.derived
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
    implicit val schema2: Schema[RecordWithArray] = Schema.derived
  }
  final case class RecordWithEither(either: Either[String, Int])
  object RecordWithEither extends CompanionOptics[RecordWithEither] {
    implicit val schema2: Schema[RecordWithEither]               = Schema.derived
    val either /*: Lens[RecordWithEither, Either[String, Int]]*/ = $(_.either)
  }

  final case class RecordWithOption(option: Option[Int])
  object RecordWithOption {
    implicit val schema2: Schema[RecordWithOption]            = Schema.derived
    implicit val schema1: zio.schema.Schema[RecordWithOption] =
      zio.schema.DeriveSchema.gen[RecordWithOption]
  }

  final case class RecordWithListOfInt(list: List[Int])
  object RecordWithListOfInt {
    implicit val schema2: Schema[RecordWithListOfInt]            = Schema.derived
    implicit val schema1: zio.schema.Schema[RecordWithListOfInt] =
      zio.schema.DeriveSchema.gen[RecordWithListOfInt]
  }

// TODO: Avi - enable when Schema2 implicit for Blocks Chunk is available
//  final case class RecordWithBlocksChunkOfInt(chunk: zio.blocks.chunk.Chunk[Int])
//  object RecordWithBlocksChunkOfInt {
//    implicit val schema2: Schema[RecordWithBlocksChunkOfInt] = Schema.derived
//  }

  final case class RecordWithTuple(tuple: (Int, Long, String, String))
  object RecordWithTuple {
    implicit val schema2: Schema[RecordWithTuple]            = Schema.derived
    implicit val schema1: zio.schema.Schema[RecordWithTuple] =
      zio.schema.DeriveSchema.gen[RecordWithTuple]
  }

  final case class RecordWithListAsFirstInTuple(tuple: (List[Int], Long, String))
  object RecordWithListAsFirstInTuple {
    implicit val schema2: Schema[RecordWithListAsFirstInTuple]            = Schema.derived
    implicit val schema1: zio.schema.Schema[RecordWithListAsFirstInTuple] =
      zio.schema.DeriveSchema.gen[RecordWithListAsFirstInTuple]
  }

  final case class RecordWithListAsSecondInTuple(tuple: (Int, List[Long], String))
  object RecordWithListAsSecondInTuple {
    implicit val schema2: Schema[RecordWithListAsSecondInTuple]            = Schema.derived
    implicit val schema1: zio.schema.Schema[RecordWithListAsSecondInTuple] =
      zio.schema.DeriveSchema.gen[RecordWithListAsSecondInTuple]
  }

  final case class RecordWithTuple1(tuple: (Int))
  object RecordWithTuple1 {
    implicit val schema2: Schema[RecordWithTuple1]            = Schema.derived
    implicit val schema1: zio.schema.Schema[RecordWithTuple1] =
      zio.schema.DeriveSchema.gen[RecordWithTuple1]
  }

  final case class RecordWithOptionalPerson(option: Option[Person])
  object RecordWithOptionalPerson {
    implicit val schema2: Schema[RecordWithOptionalPerson] = Schema.derived
  }

  final case class RecordWithStringSet(set: Set[String])
  object RecordWithStringSet {
    implicit val schema2: Schema[RecordWithStringSet] = Schema.derived
  }
  final case class RecordWithNumberSet(set: Set[Int])
  object RecordWithNumberSet {
    implicit val schema2: Schema[RecordWithNumberSet] = Schema.derived
  }

  final case class RecordWithNonNativeSet(set: Set[Person])
  object RecordWithNonNativeSet {
    implicit val schema2: Schema[RecordWithNonNativeSet] = Schema.derived
  }

  // Schema2 has zero dependency so we have to derive schema for Chunk
  // Code taken from comment by ghostdogpr on issue https://github.com/zio/zio-blocks/issues/447
  final case class RecordWithNativeBinarySet(set: Set[Chunk[Byte]])
  object RecordWithNativeBinarySet {
    val chunkConstructor: SeqConstructor[Chunk] = new SeqConstructor.Boxed[Chunk] {
      type ObjectBuilder[A] = zio.ChunkBuilder[A]
      def newObjectBuilder[A](sizeHint: Int): ObjectBuilder[A] = zio.ChunkBuilder.make(sizeHint)
      def addObject[A](builder: ObjectBuilder[A], a: A): Unit  = builder.addOne(a)
      def resultObject[A](builder: ObjectBuilder[A]): Chunk[A] = builder.result()

      override def emptyObject[A]: Chunk[A] = Chunk.empty // TODO: Avi
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

    implicit val schema2: Schema[RecordWithNativeBinarySet] = Schema.derived
  }

  final case class Person2(id: String, age: Int, count: Long)
  object Person2 extends CompanionOptics[Person2] {
    implicit val schema2: Schema[Person2]            = Schema.derived
    implicit val schema1: zio.schema.Schema[Person2] =
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
    implicit val schema1: zio.schema.Schema[Person] =
      zio.schema.DeriveSchema.gen[Person]
    implicit val schema2: Schema[Person]            = Schema.derived

    val id: Lens[Person, String] = $(_.id)
  }

  final case class Person3(foreName: String)
  object Person3 extends CompanionOptics[Person3] {
    implicit val schema1: zio.schema.Schema[Person3] =
      zio.schema.DeriveSchema.gen[Person3]
    implicit val schema2: Schema[Person3]            = Schema.derived

    val foreName: Lens[Person3, String] = $(_.foreName)
  }

  final case class Email(value: String)
  object Email {
    val derivedSchema: Reflect.Record[Binding, Email] = Schema.derived[Email].reflect.asRecord.get

    implicit val schema2: Schema[Email] =
      Schema(
        Reflect.Wrapper(
          Schema[String].reflect,
          derivedSchema.typeName,
          None,
          Binding.Wrapper[Email, String](s => Right(Email(s)), _.value)
        )
      )
  }
  final case class RecordWithWrapped(id: String, email: Email)
  object RecordWithWrapped extends CompanionOptics[RecordWithWrapped] {
    implicit val schema2: Schema[RecordWithWrapped] = Schema.derived
    val id: Lens[RecordWithWrapped, String]         = optic(_.id)
  }

  final case class RecordWithAddressMap(
    map: Map[String, Address]
  )
  object RecordWithAddressMap {
    implicit val schema2: Schema[RecordWithAddressMap] = Schema.derived
  }

  val spec = suite("Schema2Spec")(
    // TODO: Avi - Schema2 Unit support
    testRoundTripWithSchema1Codec("schema1 Record with Unit")(RecordWithUnit.schema1)(
      expectedItem = Item("unit" -> null).toAttributeValue
    )(
      expectedValue = RecordWithUnit(())
    ),
    suite("sequences")(
      // TODO: Avi - add test for record with Schema2 Chunk (with implicit built in implicit for Chunk, when available)
//      testRoundTripWithSchema2Codec("Record with Blocks Chunk[Int]")(
//        RecordWithBlocksChunkOfInt.schema2
//      )(expectedItem = Item("list" -> List(1, 2, 3)).toAttributeValue)(
//        expectedValue = RecordWithBlocksChunkOfInt(chunk = zio.blocks.chunk.Chunk(1, 2, 3))
//      ),
      testRoundTripWithCodecs("Record with List[Int]")(
        RecordWithListOfInt.schema1,
        RecordWithListOfInt.schema2
      )(expectedItem = Item("list" -> List(1, 2, 3)).toAttributeValue)(
        expectedValue = RecordWithListOfInt(list = List(1, 2, 3))
      ),
      testRoundTripWithCodecs(
        "Record with empty List[Int], transientEmptyCollection = false, requiredCollectionFields = true"
      )(
        RecordWithListOfInt.schema1,
        RecordWithListOfInt.schema2
        // Note default is:
        //_.withTransientEmptyCollection(true).withRequiredCollectionFields(true)
      )(expectedItem = AttributeValue.Map(Map(AttributeValue.String("list") -> AttributeValue.List.empty)))(
        expectedValue = RecordWithListOfInt(list = Nil)
      ),
      testRoundTripWithSchema2Codec(
        "Record of List[Int] with empty AttributeValue, transientEmptyCollection = true, requiredCollectionFields = false"
      )(
        RecordWithListOfInt.schema2,
        _.withTransientEmptyCollection(true).withRequiredCollectionFields(false)
      )(expectedItem = Item.empty.toAttributeValue)(
        expectedValue = RecordWithListOfInt(list = Nil)
      ),
      testDecodeErrorWithCodecs("returns error when decoding invalid AttributeValue for Record with List[Int]")(
        RecordWithListOfInt.schema1,
        RecordWithListOfInt.schema2,
        _.withTransientEmptyCollection(true).withRequiredCollectionFields(false)
      )(item = Item("list" -> 1).toAttributeValue)(
        errorMessage = "unable to decode AttributeValue.Number as a list"
      ),
      testDecodeErrorWithSchema2Codec(
        "Record of List[Int] with empty AttributeValue, transientEmptyCollection = false, requiredCollectionFields = true"
      )(
        RecordWithListOfInt.schema2,
        _.withTransientEmptyCollection(false).withRequiredCollectionFields(true)
      )(item = Item.empty.toAttributeValue)(
        errorMessage = "Missing attribute value for field: list"
      ),
      // Note Schema1 does not work with Arrays
      testRoundTripWithSchema2Codec("record with Array[String]")(
        RecordWithArray.schema2
      )(
        expectedItem = AttributeValue.Map(
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
      )(
        expectedValue = RecordWithArray(names = Array("Alice", "Bob", "Tharloachan"))
      )
    ),
    suite("wrapped")(
      testRoundTripWithSchema2Codec("round trip record with wrapped Email")(RecordWithWrapped.schema2)(
        expectedItem = Item("id" -> "1", "email" -> "test@example.com").toAttributeValue
      )(
        expectedValue = RecordWithWrapped("1", Email("test@example.com"))
      )
      // TODO: Avi - new types
    ),
    suite("variant suite")(
      suite("Option Suite")(
        testRoundTripWithCodecs("Record with Option[Int] Some(42) - transientNone = true")(
          RecordWithOption.schema1,
          RecordWithOption.schema2
        )(expectedItem = Item("option" -> 42).toAttributeValue)(
          expectedValue = RecordWithOption(option = Some(42))
        ),
        testRoundTripWithCodecs("Record with Option[Int] None - transientNone = true")(
          RecordWithOption.schema1,
          RecordWithOption.schema2
        )(expectedItem = Item.empty.toAttributeValue)(
          expectedValue = RecordWithOption(option = None)
        ),
        testRoundTripWithSchema2Codec("Record with Option[Int] None - transientNone = true")(
          RecordWithOption.schema2,
          _.withTransientNone(false)
        )(expectedItem = Item("option" -> null).toAttributeValue)(
          expectedValue = RecordWithOption(option = None)
        ),
        testRoundTripWithSchema2Codec("Record with Option of record")(RecordWithOptionalPerson.schema2)(
          expectedItem = Item("option" -> Item("id" -> "id", "age" -> 21)).toAttributeValue
        )(expectedValue = RecordWithOptionalPerson(option = Some(Person("id", 21))))
        // TODO: Avi - compatibility decode error tests
        // TODO: Avi - full coverage for Schema2 flag combinations
      ),
      suite("simple enumerations")(
        testRoundTripWithCodecs("enum round trip")(RecordWithEnum.schema1, RecordWithEnum.schema2)(
          expectedItem = Item("light" -> "Green").toAttributeValue
        )(expectedValue = RecordWithEnum(TrafficLight.Green)),
        testRoundTripWithSchema2Codec("enum round trip with enumValuesAsStrings=false")(
          RecordWithEnum.schema2,
          _.withEnumValuesAsStrings(false)
        )(
          expectedItem = Item("light" -> Item("Green" -> Item.empty)).toAttributeValue
        )(expectedValue = RecordWithEnum(TrafficLight.Green))
      ),
      suite("Variants that are records")(
        suite("case name mappers")(
          testRoundTripWithSchema2Codec("custom case name mapper for DiscriminatorKind.Key")(
            RecordWithPaymentMethodUsingKey.schema2,
            _.withCaseNameMapper(NameMapper.Custom(_.toLowerCase))
          )(
            expectedItem = Item("method" -> Item("paypal" -> Item("email" -> "a@b.com"))).toAttributeValue
          )(
            expectedValue = RecordWithPaymentMethodUsingKey(PaymentMethod.PayPal("a@b.com"))
          ),
          testRoundTripWithSchema2Codec("snake_case name mapper for DiscriminatorKind.Key")(
            RecordWithPaymentMethodUsingKey.schema2,
            _.withCaseNameMapper(NameMapper.SnakeCase)
          )(
            expectedItem = Item("method" -> Item("pay_pal" -> Item("email" -> "a@b.com"))).toAttributeValue
          )(
            expectedValue = RecordWithPaymentMethodUsingKey(PaymentMethod.PayPal("a@b.com"))
          ),
          testRoundTripWithSchema2Codec("camelCase name mapper for DiscriminatorKind.Key")(
            RecordWithPaymentMethodUsingKey.schema2,
            _.withCaseNameMapper(NameMapper.CamelCase)
          )(
            expectedItem = Item("method" -> Item("payPal" -> Item("email" -> "a@b.com"))).toAttributeValue
          )(
            expectedValue = RecordWithPaymentMethodUsingKey(PaymentMethod.PayPal("a@b.com"))
          ),
          testRoundTripWithSchema2Codec("kebab-case name mapper for DiscriminatorKind.Key")(
            RecordWithPaymentMethodUsingKey.schema2,
            _.withCaseNameMapper(NameMapper.KebabCase)
          )(
            expectedItem = Item("method" -> Item("pay-pal" -> Item("email" -> "a@b.com"))).toAttributeValue
          )(
            expectedValue = RecordWithPaymentMethodUsingKey(PaymentMethod.PayPal("a@b.com"))
          )
        ),
        testRoundTripWithCodecs("Record of variant with leaf record cases using DiscriminatorKind.Key")(
          RecordWithPaymentMethodUsingKey.schema1,
          RecordWithPaymentMethodUsingKey.schema2
        )(expectedItem = Item("method" -> Item("PayPal" -> Item("email" -> "a@b.com"))).toAttributeValue)(
          expectedValue = RecordWithPaymentMethodUsingKey(PaymentMethod.PayPal("a@b.com"))
        ),
        testRoundTripWithCodecs("Record of variant with leaf record cases using DiscriminatorKind.Field")(
          RecordWithPaymentMethodUsingField.schema1,
          RecordWithPaymentMethodUsingField.schema2,
          _.withDiscriminatorKind(DiscriminatorKind.Field("foo"))
        )(expectedItem = Item("method" -> Item("foo" -> "PayPal", "email" -> "a@b.com")).toAttributeValue)(
          expectedValue = RecordWithPaymentMethodUsingField(PaymentMethod2.PayPal("a@b.com"))
        ),
        testRoundTripWithCodecs("Record of variant with leaf record cases using DiscriminatorKind.None")(
          RecordWithPaymentMethodUsingNone.schema1,
          RecordWithPaymentMethodUsingNone.schema2,
          _.withDiscriminatorKind(DiscriminatorKind.None)
        )(
          expectedItem = Item("method" -> Item("email" -> "a@b.com")).toAttributeValue
        )(
          expectedValue = RecordWithPaymentMethodUsingNone(PaymentMethod3.PayPal("a@b.com"))
        )
      )
    ),
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
        Person2.schema2
          .deriving(DynamoDBCodecDeriver)
          .instance(Person2.id, codecToUpper)
          .derive
      val person                              = Person2("one", 21, 100L)
      val _                                   = codec.encoder(person)
      val enc                                 = codec.encoder(person)
      val dec                                 = codec.decoder(enc)
      assertTrue(enc == expectedAv && dec == Right(person.copy(id = "ONE_decoded")))
    },
    testRoundTripWithCodecs("round trip Person2")(Person2.schema1, Person2.schema2)(
      expectedItem = Item("id" -> "1", "age" -> 42, "count" -> 100).toAttributeValue
    )(
      expectedValue = Person2("1", 42, 100)
    ),
    suite("Native Map")(
      testRoundTripWithCodecs("Record with native Map[String, Int]")(
        RecordWithNativeMap.schema1,
        RecordWithNativeMap.schema2
      )(expectedItem = Item("map" -> Map("a" -> 1, "b" -> 2)).toAttributeValue)(
        expectedValue = RecordWithNativeMap(map = Map("a" -> 1, "b" -> 2))
      ),
      testRoundTripWithSchema2Codec("Record with native map of record - Map[String, Address]")(
        RecordWithAddressMap.schema2
      )(
        expectedItem = Item("map" -> Map("home" -> Item("postcode" -> "12345", "number" -> 10))).toAttributeValue
      )(
        expectedValue = RecordWithAddressMap(map = Map("home" -> Address("12345", 10)))
      )
    ),
    suite("non native Map")(
      testRoundTripWithCodecs("Record with Map[Int, Int]")(
        RecordWithNonNativeMapOfInt.schema1,
        RecordWithNonNativeMapOfInt.schema2
      )(expectedItem = Item("map" -> List(List(1, 1), List(2, 2))).toAttributeValue)(
        expectedValue = RecordWithNonNativeMapOfInt(map = Map(1 -> 1, 2 -> 2))
      ),
      testRoundTripWithCodecs("Record with non native Map[Int, Person]")(
        RecordWithNonNativeMapOfPerson.schema1,
        RecordWithNonNativeMapOfPerson.schema2
      )(expectedItem =
        AttributeValue.Map(
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
      )(
        expectedValue = RecordWithNonNativeMapOfPerson(Map(1 -> Person("id", 21)))
      )
    ),
    suite("tuple")(
      // Schema2 encoding will never be symmetric with Schema1
      testRoundTripWithSchema2Codec("record with tuple (Int, Long, String)")(
        RecordWithTuple.schema2
      )(
        expectedItem = AttributeValue.Map(
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
      )(
        expectedValue = RecordWithTuple(tuple = (1, 2, "3", "4"))
      ),
      test("tuple compatibility - nested lists") {
        val schema2Codec =
          SchemaCodec.schema2ToSchemaCodec(RecordWithTuple.schema2, DynamoDBCodecDeriverConfigure.identity)
        val schema1Codec = SchemaCodec.schema1ToSchemaCodec(RecordWithTuple.schema1)

        val recordWithTuple = RecordWithTuple(tuple = (1, 2, "3", "4"))
        val av              = schema1Codec.encoder(recordWithTuple)
        val a               = schema2Codec.decoder(av)
        assertTrue(a == Right(recordWithTuple)) // Schema2 codec can decode a tuple encoded by a ZIO Schema codec
      },
      test("tuple compatibility - single scalar value for Tuple1") {
        val schema2Codec =
          SchemaCodec.schema2ToSchemaCodec(RecordWithTuple1.schema2, DynamoDBCodecDeriverConfigure.identity)
        val schema1Codec = SchemaCodec.schema1ToSchemaCodec(RecordWithTuple1.schema1)

        val recordWithTuple = RecordWithTuple1(tuple = (1))
        val av              = schema1Codec.encoder(recordWithTuple)
        val a               = schema2Codec.decoder(av)
        assertTrue(a == Right(recordWithTuple)) // Schema2 codec can decode a tuple encoded by a ZIO Schema codec
      },
      test("tuple compatibility - tuple with first element as List") {
        val schema2Codec =
          SchemaCodec.schema2ToSchemaCodec(RecordWithListAsFirstInTuple.schema2, DynamoDBCodecDeriverConfigure.identity)
        val schema1Codec = SchemaCodec.schema1ToSchemaCodec(RecordWithListAsFirstInTuple.schema1)

        val recordWithTuple = RecordWithListAsFirstInTuple(tuple = (List(1, 2), 2L, "3"))
        val av1             = schema1Codec.encoder(recordWithTuple)
        val dec2            = schema2Codec.decoder(av1)

        assertTrue(dec2 == Right(recordWithTuple))
      },
      test("tuple compatibility - tuple with second element as List") {
        val schema2Codec =
          SchemaCodec.schema2ToSchemaCodec(
            RecordWithListAsSecondInTuple.schema2,
            DynamoDBCodecDeriverConfigure.identity
          )
        val schema1Codec = SchemaCodec.schema1ToSchemaCodec(RecordWithListAsSecondInTuple.schema1)

        val recordWithTuple = RecordWithListAsSecondInTuple(tuple = (1, List(1L, 2L), "3"))
        val av1             = schema1Codec.encoder(recordWithTuple)
        val dec2            = schema2Codec.decoder(av1)

        assertTrue(dec2 == Right(recordWithTuple))
      }
    ),
    suite("field name mapper applied at record level")(
      testRoundTripWithSchema2Codec("snake_case name mapper for Person")(
        Person3.schema2,
        _.withFieldNameMapper(NameMapper.SnakeCase)
      )(
        expectedItem = Item("fore_name" -> "John").toAttributeValue
      )(
        expectedValue = Person3(foreName = "John")
      ),
      testRoundTripWithSchema2Codec("kebab-case name mapper for Person")(
        Person3.schema2,
        _.withFieldNameMapper(NameMapper.KebabCase)
      )(
        expectedItem = Item("fore-name" -> "John").toAttributeValue
      )(
        expectedValue = Person3(foreName = "John")
      ),
      testRoundTripWithSchema2Codec("camelCase name mapper for Person")(
        Person3.schema2,
        _.withFieldNameMapper(NameMapper.KebabCase).withFieldNameMapper(NameMapper.CamelCase)
      )(
        expectedItem = Item("foreName" -> "John").toAttributeValue
      )(
        expectedValue = Person3(foreName = "John")
      ),
      testRoundTripWithSchema2Codec("PascalCase name mapper for Person")(
        Person3.schema2,
        _.withFieldNameMapper(NameMapper.PascalCase)
      )(
        expectedItem = Item("ForeName" -> "John").toAttributeValue
      )(
        expectedValue = Person3(foreName = "John")
      ),
      testRoundTripWithSchema2Codec("custom name mapper for Person")(
        Person3.schema2,
        _.withFieldNameMapper(NameMapper.Custom(_.toLowerCase))
      )(
        expectedItem = Item("forename" -> "John").toAttributeValue
      )(
        expectedValue = Person3(foreName = "John")
      )
    ),
    testRoundTripWithSchema2Codec2("modify a field name")(
      Person3.schema2,
      builderConfigure =
        (x: DerivationBuilder[DynamoDBCodec, Person3]) => x.modifier(Person3.foreName, Modifier.rename("forename"))
    )(
      expectedItem = Item("forename" -> "John").toAttributeValue
    )(
      expectedValue = Person3(foreName = "John")
    )
  )

  private def testRoundTripWithCodecs[A](
    name: String
  )(
    schema1: zio.schema.Schema[A],
    schema2: Schema[A],
    cfg: DynamoDBCodecDeriverConfigure[A] = DynamoDBCodecDeriverConfigure.identity[A]
  )(
    expectedItem: AttributeValue
  )(
    expectedValue: A
  ): Spec[Any, Nothing] = {
    val schema2Codec = SchemaCodec.schema2ToSchemaCodec(schema2, cfg)
    val schema1Codec = SchemaCodec.schema1ToSchemaCodec(schema1)

    val testBody: SchemaCodec[A] => TestResult = { codec =>
      val enc = codec.encoder(expectedValue)
      val dec = codec.decoder(enc)
      assertTrue(enc == expectedItem && dec == Right(expectedValue))
    }

    suite(name + " [compatibility]")(
      test("schema1") {
        testBody(schema1Codec)
      },
      test("schema2") {
        testBody(schema2Codec)
      }
    )
  }

  def testRoundTripWithSchema2Codec2[A](
    name: String
  )(
    schema2: Schema[A],
    deriverConfigure: DynamoDBCodecDeriverConfigure[A] = DynamoDBCodecDeriverConfigure.identity[A],
    builderConfigure: DerivationBuilderConfigure[A] = DerivationBuilderConfigure.identity[A]
  )(
    expectedItem: AttributeValue
  )(
    expectedValue: A
  ): Spec[Any, Nothing] = {

    val testBody: SchemaCodec[A] => TestResult = { codec =>
      val enc = codec.encoder(expectedValue)
      val dec = codec.decoder(enc)
      assertTrue(enc == expectedItem && dec == Right(expectedValue))
    }

    val schema2Codec = SchemaCodec.schema2ToSchemaCodec2(schema2, deriverConfigure, builderConfigure)

    suite(name)(
      test("schema2") {
        testBody(schema2Codec)
      }
    )
  }

  def testRoundTripWithSchema2Codec[A](
    name: String
  )(
    schema2: Schema[A],
    cfg: DynamoDBCodecDeriverConfigure[A] = DynamoDBCodecDeriverConfigure.identity[A]
  )(
    expectedItem: AttributeValue
  )(
    expectedValue: A
  ): Spec[Any, Nothing] = {

    val testBody: SchemaCodec[A] => TestResult = { codec =>
      val enc = codec.encoder(expectedValue)
      val dec = codec.decoder(enc)
      assertTrue(enc == expectedItem && dec == Right(expectedValue))
    }

    val schema2Codec = SchemaCodec.schema2ToSchemaCodec(schema2, cfg)

    suite(name)(
      test("schema2") {
        testBody(schema2Codec)
      }
    )
  }

  private def testRoundTripWithSchema1Codec[A](
    name: String
  )(
    schema1: zio.schema.Schema[A]
  )(
    expectedItem: AttributeValue
  )(
    expectedValue: A
  ): Spec[Any, Nothing] = {
    val schema1Codec = SchemaCodec.schema1ToSchemaCodec(schema1)

    val testBody: SchemaCodec[A] => TestResult = { codec =>
      val enc = codec.encoder(expectedValue)
      val dec = codec.decoder(enc)
      assertTrue(enc == expectedItem && dec == Right(expectedValue))
    }

    suite(name)(
      test("schema1") {
        testBody(schema1Codec)
      }
    )
  }

  private def testDecodeErrorWithCodecs[A](
    name: String
  )(
    schema1: zio.schema.Schema[A],
    schema2: Schema[A],
    cfg: DynamoDBCodecDeriverConfigure[A] // = DynamoDBCodecConfigure.identity[A]
  )(
    item: AttributeValue
  )(
    errorMessage: String
  ): Spec[Any, Nothing] = {
    val schema2Codec = SchemaCodec.schema2ToSchemaCodec(schema2, cfg)
    val schema1Codec = SchemaCodec.schema1ToSchemaCodec(schema1)

    val testBody: SchemaCodec[A] => TestResult = { codec =>
      val dec = codec.decoder(item)
      assert(dec)(isLeft(equalTo(DecodingError(errorMessage))))
    }

    suite(name + " [compatibility]")(
      test("schema1") {
        testBody(schema1Codec)
      },
      test("schema2") {
        testBody(schema2Codec)
      }
    )
  }

  private def testDecodeErrorWithSchema2Codec[A](
    name: String
  )(
    schema2: Schema[A],
    cfg: DynamoDBCodecDeriverConfigure[A] // = DynamoDBCodecConfigure.identity[A]
  )(
    item: AttributeValue
  )(
    errorMessage: String
  ): Spec[Any, Nothing] = {
    val schema2Codec = SchemaCodec.schema2ToSchemaCodec(schema2, cfg)

    val testBody: SchemaCodec[A] => TestResult = { codec =>
      val dec = codec.decoder(item)
      assert(dec)(isLeft(equalTo(DecodingError(errorMessage))))
    }

    suite(name)(
      test("schema2") {
        testBody(schema2Codec)
      }
    )
  }

}
