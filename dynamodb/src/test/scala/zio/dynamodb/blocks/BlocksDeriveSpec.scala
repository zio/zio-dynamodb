package zio.dynamodb.blocks

import zio.Chunk
import zio.test._
import zio.blocks.schema.Modifier.config
import zio.blocks.schema.Schema
import zio.blocks.schema.Reflect
import zio.blocks.schema.binding.Binding
import zio.dynamodb.{ AttributeValue, Item }
import zio.dynamodb.blocks.BlocksDeriveSpec.PaymentMethod.CreditCard

object BlocksDeriveSpec extends ZIOSpecDefault {

  sealed trait TrafficLight
  object TrafficLight {
    final case object Red    extends TrafficLight
    final case object Yellow extends TrafficLight
    final case object Green  extends TrafficLight

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
  object RecordWithEither       {
    implicit val schema: Schema[RecordWithEither] = Schema.derived
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

  // Get compile time error when I use Chunk - raised issue https://github.com/zio/zio-blocks/issues/447
  final case class RecordWithNativeBinarySet(set: Set[List[Byte]])
  object RecordWithNativeBinarySet {
    implicit val schema: Schema[RecordWithNativeBinarySet] = Schema.derived
  }

  final case class Person(id: String, age: Long)
  object Person {
    implicit val schema: Schema[Person] = Schema.derived
  }

  val spec = suite("Used derived codecs in a round trip spec")(
    test("Record with Primitives") {
      val expectedItem            = Item("id" -> "1", "age" -> 42)
      val codec: DdbCodec[Person] = Person.schema.derive(BlocksDdbDerived)
      val expectedPerson          = Person("1", 42)
      val enc                     = codec.encoder(expectedPerson)
      val dec                     = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with List(1, 2) ") {
      val expectedItem                           =
        Item("numbers" -> List(1, 2), "map" -> Map.empty[String, Int])
      val codec: DdbCodec[RecordWithCollections] = RecordWithCollections.schema.derive(BlocksDdbDerived)
      val expectedPerson                         = RecordWithCollections(numbers = List(1, 2))
      val enc                                    = codec.encoder(expectedPerson)
      val dec                                    = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with List() ") {
      val expectedItem                           =
        Item("numbers" -> List.empty[String], "map" -> Map.empty[String, Int])
      val codec: DdbCodec[RecordWithCollections] = RecordWithCollections.schema.derive(BlocksDdbDerived)
      val expectedPerson                         = RecordWithCollections(numbers = List())
      val enc                                    = codec.encoder(expectedPerson)
      val dec                                    = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with Array('a', 'b')") {
      val expectedItem                     =
        Item("names" -> Array("a", "b"))
      val codec: DdbCodec[RecordWithArray] = RecordWithArray.schema.derive(BlocksDdbDerived)
      val expectedPerson                   = RecordWithArray(names = Array("a", "b"))
      val enc                              = codec.encoder(expectedPerson)
      val dec                              = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with Array()") {
      val expectedItem                     =
        Item("names" -> Array.empty[String])
      val codec: DdbCodec[RecordWithArray] = RecordWithArray.schema.derive(BlocksDdbDerived)
      val expectedPerson                   = RecordWithArray(names = Array())
      val enc                              = codec.encoder(expectedPerson)
      val dec                              = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with Native String Set") {
      val expectedItem                         =
        Item("set" -> Set("a", "b"))
      val codec: DdbCodec[RecordWithStringSet] = RecordWithStringSet.schema.derive(BlocksDdbDerived)
      val expected                             = RecordWithStringSet(set = Set("a", "b"))
      val enc                                  = codec.encoder(expected)
      val dec                                  = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expected))
    },
    test("Record with Native Number Set of Int") {
      val expectedItem                         = AttributeValue.Map("set", AttributeValue.NumberSet(Set(1, 2)))
      val codec: DdbCodec[RecordWithNumberSet] = RecordWithNumberSet.schema.derive(BlocksDdbDerived)
      val expected                             = RecordWithNumberSet(set = Set(1, 2))
      val enc                                  = codec.encoder(expected)
      val dec                                  = codec.decoder(enc)
      assertTrue(enc == expectedItem && dec == Right(expected))
    },
    test("Record with Native Binary Set") {
      val byte1: Byte                                = 0x01
      val byte2: Byte                                = 0x02
      val byte3: Byte                                = 0x03
      val byte4: Byte                                = 0x04
      val expectedItem                               =
        AttributeValue.Map("set", AttributeValue.BinarySet(Set(Chunk(byte1, byte2), Chunk(byte3, byte4))))
      val codec: DdbCodec[RecordWithNativeBinarySet] = RecordWithNativeBinarySet.schema.derive(BlocksDdbDerived)
      val expected                                   = RecordWithNativeBinarySet(set = Set(List(byte1, byte2), List(byte3, byte4)))
      val enc                                        = codec.encoder(expected)
      val dec                                        = codec.decoder(enc)
      assertTrue(enc == expectedItem && dec == Right(expected))
    },
    test("Record with Non Native Set of Person") {
      val expectedItem                            =
        Item("set" -> Set(Item("id" -> "1", "age" -> 21)))
      val codec: DdbCodec[RecordWithNonNativeSet] = RecordWithNonNativeSet.schema.derive(BlocksDdbDerived)
      val expected                                = RecordWithNonNativeSet(set = Set(Person("1", 21)))
      val enc                                     = codec.encoder(expected)
      val dec                                     = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expected))
    },
    test("Record with native Map[String, Int]") {
      val expectedItem                           =
        Item("numbers" -> List.empty[String], "map" -> Map("a" -> 1, "b" -> 2))
      val codec: DdbCodec[RecordWithCollections] = RecordWithCollections.schema.derive(BlocksDdbDerived)
      val expectedPerson                         = RecordWithCollections(map = Map("a" -> 1, "b" -> 2))
      val enc                                    = codec.encoder(expectedPerson)
      val dec                                    = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    // TODO: Avi - implement Tuple codec
    test("Record with tuple") {
      val expectedItem                     =
        Item("tuple" -> List(1, 2, 3))
      val codec: DdbCodec[RecordWithTuple] = RecordWithTuple.schema.derive(BlocksDdbDerived)
      val expected                         = RecordWithTuple((1, 2, 3))
      val enc                              = codec.encoder(expected)
//      val dec                              = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue /* && dec == Right(expected) */ )
    },
    test("Record with native Map()") {
      val expectedItem                           =
        Item("numbers" -> List.empty[String], "map" -> Map.empty[String, Int])
      val codec: DdbCodec[RecordWithCollections] = RecordWithCollections.schema.derive(BlocksDdbDerived)
      val expectedPerson                         = RecordWithCollections(map = Map())
      val enc                                    = codec.encoder(expectedPerson)
      val dec                                    = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with NON native Map(1 -> 1, 2 -> 2)") {
      val expectedItem                            = Item("map" -> List(List(1, 1), List(2, 2)))
      val codec: DdbCodec[RecordWithNonNativeMap] = RecordWithNonNativeMap.schema.derive(BlocksDdbDerived)
      val expectedRecord                          = RecordWithNonNativeMap(map = Map(1 -> 1, 2 -> 2))
      val enc                                     = codec.encoder(expectedRecord)
      val dec                                     = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedRecord))
    },
    test("Record with Either[String, Int] Right(42)") {
      val expectedItem                      =
        Item("either" -> Item("Right" -> 42))
      val codec: DdbCodec[RecordWithEither] = RecordWithEither.schema.derive(BlocksDdbDerived)
      val expectedPerson                    = RecordWithEither(either = Right(42))
      val enc                               = codec.encoder(expectedPerson)
      val dec                               = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with Either[String, Int] Left('error')") {
      val expectedItem                      =
        Item("either" -> Item("Left" -> "error"))
      val codec: DdbCodec[RecordWithEither] = RecordWithEither.schema.derive(BlocksDdbDerived)
      val expectedPerson                    = RecordWithEither(either = Left("error"))
      val enc                               = codec.encoder(expectedPerson)
      val dec                               = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with Option[Int] Some(42)") {
      val expectedItem                      =
        Item("id" -> "1", "option" -> 42)
      val codec: DdbCodec[RecordWithOption] = RecordWithOption.schema.derive(BlocksDdbDerived)
      val expectedPerson                    = RecordWithOption("1", option = Some(42))
      val enc                               = codec.encoder(expectedPerson)
      val dec                               = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with Option[Person]") {
      val expectedItem                              =
        Item("option" -> Item("id" -> "id", "age" -> 21))
      val codec: DdbCodec[RecordWithOptionalPerson] = RecordWithOptionalPerson.schema.derive(BlocksDdbDerived)
      val person                                    = RecordWithOptionalPerson(option = Some(Person("id", 21)))
      val enc                                       = codec.encoder(person)
      val dec                                       = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(person))
    },
    test("Record with Option[Int] Some(42)") {
      val expectedItem                      =
        Item("id" -> "1", "option" -> 42).toAttributeValue
      val codec: DdbCodec[RecordWithOption] = RecordWithOption.schema.derive(BlocksDdbDerived)
      val person                            = RecordWithOption("1", option = Some(42))
      val enc                               = codec.encoder(person)
      val dec                               = codec.decoder(enc)
      assertTrue(enc == expectedItem && dec == Right(person))
    },
    test("Record with Option[Int] None") {
      val expectedItem                      =
        Item("id" -> "1")
      val codec: DdbCodec[RecordWithOption] = RecordWithOption.schema.derive(BlocksDdbDerived)
      val expectedPerson                    = RecordWithOption("1", option = None)
      val enc                               = codec.encoder(expectedPerson)
      val dec                               = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("Record with a simple enum") {
      val expectedItem                    =
        Item("light" -> "Green")
      val codec: DdbCodec[RecordWithEnum] = RecordWithEnum.schema.derive(BlocksDdbDerived)
      val expectedRecord                  = RecordWithEnum(TrafficLight.Green)
      val enc                             = codec.encoder(expectedRecord)
      val dec                             = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedRecord))
    },
    test("Record with a complex enum using default discriminator policy") {
      val expectedItem                             =
        Item("method" -> Item("CreditCard" -> Item("number" -> "1234", "cvv" -> "567")))
      val codec: DdbCodec[RecordWithPaymentMethod] = RecordWithPaymentMethod.schema.derive(BlocksDdbDerived)
      val expectedRecord                           = RecordWithPaymentMethod(CreditCard("1234", "567"))
      val enc                                      = codec.encoder(expectedRecord)
      val dec                                      = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedRecord))
    },
    test("Record with a complex enum using field discriminator policy") {
      val expectedItem                              =
        Item("method" -> Item("number" -> "1234", "cvv" -> "567", "discriminator" -> "CreditCard"))
      val codec: DdbCodec[RecordWithPaymentMethod2] = RecordWithPaymentMethod2.schema.derive(BlocksDdbDerived)
      val expectedRecord                            = RecordWithPaymentMethod2(PaymentMethod2.CreditCard("1234", "567"))
      val enc                                       = codec.encoder(expectedRecord)
      val dec                                       = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedRecord))
    },
// TODO: Avi - test caching somehow
//    test("codec are cached") {
//      val codec: DdbCodec[RecordWithOption] = RecordWithOption.schema.derive(BlocksDdbDerived)
//      (1 to 100).foreach { i =>
//        val person = RecordWithOption(i.toString, Some(i))
//        val enc    = codec.encoder(person)
//        codec.decoder(enc)
//      }
//      assertTrue(BlocksDdbDerived.cacheMissCounter == 3)
//    },
    test("explore Wrapped") {
      case class Email(value: String)

      object Email {
        val derivedSchema: Reflect.Record[Binding, Email] = Schema.derived[Email].reflect.asRecord.get

        implicit val schema: Schema[Email] =
          Schema(
            Reflect.Wrapper(
              Schema[String].reflect,
              derivedSchema.typeName,
              None, // TODO: Avi investigate new wrapperPrimitiveType: Option[PrimitiveType[A]],
              Binding.Wrapper[Email, String](s => Right(Email(s)), _.value)
            )
          )
      }
      assertTrue(true)
    } @@ TestAspect.ignore
  )
}
