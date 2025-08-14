package zio.dynamodb.blocks

import zio.test._
import zio.blocks.schema._

object NewTypeSpec extends ZIOSpecDefault {
  val spec = suite("NewTypeSpec")(
    test("String new type uses string primitive") {
      val x = Planet.name.source
      println(s"XXXXXXXX x: $x")

      assertTrue(true)
    }
  )

  import zio.prelude._

  type Name = Name.Type

  object Name extends Subtype[String] {
    implicit val schema: Schema[Name] = derive(Schema[String])
  }

  type Kilogram = Kilogram.Type

  object Kilogram extends Subtype[Double] {
    implicit val schema: Schema[Kilogram] = derive(Schema[Double])
  }

  type Meter = Meter.Type

  object Meter extends Subtype[Double] {
    implicit val schema: Schema[Meter] = derive(Schema[Double])
  }

  case class Planet(name: Name, mass: Kilogram, radius: Meter)

  object Planet extends CompanionOptics[Planet] {
    implicit val schema: Schema[Planet] = Schema.derived
    val name: Lens[Planet, Name]        = $(_.name)
    val mass: Lens[Planet, Kilogram]    = $(_.mass)
    val radius: Lens[Planet, Meter]     = $(_.radius)
  }

}
