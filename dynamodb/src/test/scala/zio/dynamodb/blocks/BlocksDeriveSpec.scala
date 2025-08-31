package zio.dynamodb.blocks

import zio.test._
import zio.blocks.schema.CompanionOptics
import zio.blocks.schema.Schema
import zio.blocks.schema.Lens
import zio.blocks.schema.Reflect
import zio.blocks.schema.binding.Binding

object BlocksDeriveSpec extends ZIOSpecDefault {
  val spec = suite("BlocksDeriveSpec")(
    test("use derived codec") {
      final case class Person(id: String, count: Int)
      object Person extends CompanionOptics[Person] {
        implicit val schema: Schema[Person] = Schema.derived
        val id: Lens[Person, String]        = optic(_.id)
        val count: Lens[Person, Int]        = optic(_.count)
      }
      val codec: DdbCodec[Person] = Person.schema.derive(BlocksDdbDerived)
      val y = codec.encoder(Person("1", 42))
      println(s"Encoded Person: $y")
      assertTrue(true)
    } @@ TestAspect.ignore, // TODO: get Record derivation working
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
    }
  )
}
