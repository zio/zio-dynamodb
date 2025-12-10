package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.test.{ assertTrue, ZIOSpecDefault }

object BlocksSpec extends ZIOSpecDefault {
  final case class Person(id: String, age: Long)
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived

    val id: Lens[Person, String] = $(_.id)
  }
  val spec = suite("BlocksSpec")(
    test("placeholder") {
      // TODO: Avi
      assertTrue(true)
    }
  )

}
