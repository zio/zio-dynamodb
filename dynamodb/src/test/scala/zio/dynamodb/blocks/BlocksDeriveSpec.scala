package zio.dynamodb.blocks

import zio.test._
import zio.blocks.schema.CompanionOptics
import zio.blocks.schema.Schema
import zio.blocks.schema.Lens

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
    } @@ TestAspect.ignore // TODO: get Record derivation working
  )
}
