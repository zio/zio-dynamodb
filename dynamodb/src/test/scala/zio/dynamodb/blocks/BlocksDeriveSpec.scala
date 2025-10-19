package zio.dynamodb.blocks

import zio.test._
import zio.blocks.schema.CompanionOptics
import zio.blocks.schema.Schema
import zio.blocks.schema.Reflect
import zio.blocks.schema.binding.Binding
import zio.dynamodb.Item

object BlocksDeriveSpec extends ZIOSpecDefault {
  final case class RecordWithCollections(
    numbers: List[Int] = Nil,
    map: Map[String, Int] = Map.empty
  )
  object RecordWithCollections extends CompanionOptics[RecordWithCollections] {
    implicit val schema: Schema[RecordWithCollections] = Schema.derived
  }
  final case class PersonWithArray(
    id: String,
    // TODO: Avi - bottom out Array support in AttrMap/To/FromAttributeValue and equality checks
    names: Array[String] = Array.empty
  ) {
    override def equals(obj: Any): Boolean =
      obj match {
        case that: PersonWithArray =>
          this.id == that.id && this.names.toSeq == that.names.toSeq
        case _                     => false
      }

    override def hashCode(): Int = {
      val state = Seq(id, names.toSeq)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }
  object PersonWithArray       extends CompanionOptics[PersonWithArray]       {
    implicit val schema: Schema[PersonWithArray] = Schema.derived
  }
  final case class PersonWithEither(id: String, either: Either[String, Int])
  object PersonWithEither      extends CompanionOptics[PersonWithEither]      {
    implicit val schema: Schema[PersonWithEither] = Schema.derived
  }

  final case class PersonWithOption(id: String, option: Option[Int])
  object PersonWithOption extends CompanionOptics[PersonWithOption] {
    implicit val schema: Schema[PersonWithOption] = Schema.derived
  }

  final case class Person(id: String, age: Int)
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived
  }

  val spec = suite("BlocksDeriveSpec round trip spec")(
    test("use derived codec for Record with Primitives") {
      val expectedItem            = Item("id" -> "1", "age" -> 42)
      val codec: DdbCodec[Person] = Person.schema.derive(BlocksDdbDerived)
      val expectedPerson          = Person("1", 42)
      val enc                     = codec.encoder(expectedPerson)
      val dec                     = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("use derived codec for Record with List(1, 2) ") {
      val expectedItem                           =
        Item("numbers" -> List(1, 2), "map" -> Map.empty[String, Int])
      val codec: DdbCodec[RecordWithCollections] = RecordWithCollections.schema.derive(BlocksDdbDerived)
      val expectedPerson                         = RecordWithCollections(numbers = List(1, 2))
      val enc                                    = codec.encoder(expectedPerson)
      val dec                                    = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("use derived codec for Record with List() ") {
      val expectedItem                           =
        Item("numbers" -> List.empty[String], "map" -> Map.empty[String, Int])
      val codec: DdbCodec[RecordWithCollections] = RecordWithCollections.schema.derive(BlocksDdbDerived)
      val expectedPerson                         = RecordWithCollections(numbers = List())
      val enc                                    = codec.encoder(expectedPerson)
      val dec                                    = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("use derived codec for Record with Array('a', 'b')") {
      val expectedItem                     =
        Item("id" -> "1", "names" -> Array("a", "b"))
      val codec: DdbCodec[PersonWithArray] = PersonWithArray.schema.derive(BlocksDdbDerived)
      val expectedPerson                   = PersonWithArray("1", names = Array("a", "b"))
      val enc                              = codec.encoder(expectedPerson)
      val dec                              = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("use derived codec for Record with Array()") {
      val expectedItem                     =
        Item("id" -> "1", "names" -> Array.empty[String])
      val codec: DdbCodec[PersonWithArray] = PersonWithArray.schema.derive(BlocksDdbDerived)
      val expectedPerson                   = PersonWithArray("1", names = Array())
      val enc                              = codec.encoder(expectedPerson)
      val dec                              = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("use derived codec for Record with native Map[String, Int]") {
      val expectedItem                           =
        Item("numbers" -> List.empty[String], "map" -> Map("a" -> 1, "b" -> 2))
      val codec: DdbCodec[RecordWithCollections] = RecordWithCollections.schema.derive(BlocksDdbDerived)
      val expectedPerson                         = RecordWithCollections(map = Map("a" -> 1, "b" -> 2))
      val enc                                    = codec.encoder(expectedPerson)
      val dec                                    = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("use derived codec for Record with native Map()") {
      val expectedItem                           =
        Item("numbers" -> List.empty[String], "map" -> Map.empty[String, Int])
      val codec: DdbCodec[RecordWithCollections] = RecordWithCollections.schema.derive(BlocksDdbDerived)
      val expectedPerson                         = RecordWithCollections(map = Map())
      val enc                                    = codec.encoder(expectedPerson)
      val dec                                    = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("use derived codec for Record with Either[String, Int] Right(42)") {
      val expectedItem                      =
        Item("id" -> "1", "either" -> Item("Right" -> 42))
      val codec: DdbCodec[PersonWithEither] = PersonWithEither.schema.derive(BlocksDdbDerived)
      val expectedPerson                    = PersonWithEither("1", either = Right(42))
      val enc                               = codec.encoder(expectedPerson)
      val dec                               = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("use derived codec for Record with Either[String, Int] Left('error')") {
      val expectedItem                      =
        Item("id" -> "1", "either" -> Item("Left" -> "error"))
      val codec: DdbCodec[PersonWithEither] = PersonWithEither.schema.derive(BlocksDdbDerived)
      val expectedPerson                    = PersonWithEither("1", either = Left("error"))
      val enc                               = codec.encoder(expectedPerson)
      val dec                               = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("use derived codec for Record with Option[Int] Some(42)") {
      val expectedItem                      =
        Item("id" -> "1", "option" -> 42)
      val codec: DdbCodec[PersonWithOption] = PersonWithOption.schema.derive(BlocksDdbDerived)
      val expectedPerson                    = PersonWithOption("1", option = Some(42))
      val enc                               = codec.encoder(expectedPerson)
      val dec                               = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("use derived codec for Record with Option[Int] None") {
      val expectedItem                      =
        Item("id" -> "1")
      val codec: DdbCodec[PersonWithOption] = PersonWithOption.schema.derive(BlocksDdbDerived)
      val expectedPerson                    = PersonWithOption("1", option = None)
      val enc                               = codec.encoder(expectedPerson)
      val dec                               = codec.decoder(enc)
      assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
    },
    test("explore Wrapped") {
      case class Email(value: String)

      object Email {
        val derivedSchema: Reflect.Record[Binding, Email] = Schema.derived[Email].reflect.asRecord.get

        implicit val schema: Schema[Email] =
          Schema(
            Reflect.Wrapper(
              Schema[String].reflect,
              derivedSchema.typeName,
              Binding.Wrapper[Email, String](s => Right(Email(s)), _.value)
            )
          )
      }
      assertTrue(true)
    } @@ TestAspect.ignore
  )
}
