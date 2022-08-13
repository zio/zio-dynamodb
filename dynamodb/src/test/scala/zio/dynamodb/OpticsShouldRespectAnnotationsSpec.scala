package zio.dynamodb

import zio.dynamodb.Annotations.{ discriminator, id }
import zio.test.TestAspect
import zio.schema.{ DeriveSchema, Schema }
import zio.test.Assertion.equalTo
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object OpticsShouldRespectAnnotationsSpec extends DefaultRunnableSpec {

  sealed trait CaseObjectOnlyEnum
  final case class BoxOfCaseObjectOnlyEnum(enum: CaseObjectOnlyEnum)
  object BoxOfCaseObjectOnlyEnum {

    case object ONE extends CaseObjectOnlyEnum
    @id("2")
    case object TWO extends CaseObjectOnlyEnum
    implicit val schema: Schema[BoxOfCaseObjectOnlyEnum] = DeriveSchema.gen[BoxOfCaseObjectOnlyEnum]
    val sumType                                          = ProjectionExpression.accessors[BoxOfCaseObjectOnlyEnum]
  }

  sealed trait TrafficLight

  object TrafficLight {

    final case class Green(rgb: Int) extends TrafficLight

    object Green {
      implicit val schema = DeriveSchema.gen[Green]
      val rgb             = ProjectionExpression.accessors[Green]
    }

    final case class Red(rgb: Int) extends TrafficLight

    @id("red_traffic_light")
    object Red {
      implicit val schema = DeriveSchema.gen[Red]
      val rgb             = ProjectionExpression.accessors[Red]
    }

    final case class Amber( /*@id("red_green_blue")*/ rgb: Int) extends TrafficLight

    object Amber {
      implicit val schema = DeriveSchema.gen[Amber]
      val rgb             = ProjectionExpression.accessors[Amber]
    }

    final case class Box(trafficLightColour: TrafficLight)

    object Box {
      implicit val schema    = DeriveSchema.gen[Box]
      val trafficLightColour = ProjectionExpression.accessors[Box]
    }

    implicit val schema = DeriveSchema.gen[TrafficLight]

    // PROBLEM: zio-schema SORTS sum type members alphabetically, NOT in the order they are defined
    val (amber, green, red) = ProjectionExpression.accessors[TrafficLight]

  }

  @discriminator("light_type")
  sealed trait TrafficLightDiscriminated

  object TrafficLightDiscriminated {
    final case class Green(rgb: Int) extends TrafficLightDiscriminated

    object Green {
      implicit val schema = DeriveSchema.gen[Green]
      val rgb             = ProjectionExpression.accessors[Green]
    }

    @id("red_traffic_light")
    final case class Red(rgb: Int) extends TrafficLightDiscriminated

    object Red {
      implicit val schema = DeriveSchema.gen[Red]
      val rgb             = ProjectionExpression.accessors[Red]
    }

    final case class Amber(@id("red_green_blue") rgb: Int) extends TrafficLightDiscriminated

    object Amber {
      implicit val schema = DeriveSchema.gen[Amber]
      val rgb             = ProjectionExpression.accessors[Amber]
    }

    final case class Box(trafficLightColour: TrafficLightDiscriminated)

    object Box {
      implicit val schema    = DeriveSchema.gen[Box]
      val trafficLightColour = ProjectionExpression.accessors[Box]
    }

    implicit val schema     = DeriveSchema.gen[TrafficLightDiscriminated]
    // PROBLEM 1: zio-schema SORTS sum type members alphabetically, NOT in the order they are defined
    val (amber, green, red) = ProjectionExpression.accessors[TrafficLightDiscriminated]

  }

  /*
  TEST CASES
  - nested
    - with/without @discriminator
      - @id field level
      - @id at class level
  - non nested
    - with/without @discriminator
      - @id field level
      - @id at class level
   */

  override def spec: ZSpec[Environment, Failure] =
    suite("OpticsShouldRespectAnnotationsSpec")(nonDiscriminatedSuite, discriminatedSuite)

  val discriminatedSuite = {
    val conditionExpressionSuite =
      suite("ConditionExpression suite")(
        test("TrafficLight.Box.trafficLightColour >>> TrafficLight.red >>> TrafficLight.Red.rgb === 1") {
          val ce =
            TrafficLightDiscriminated.Box.trafficLightColour >>> TrafficLightDiscriminated.red >>> TrafficLightDiscriminated.Red.rgb === 1
          assert(ce.toString)(
            equalTo("Equals(ProjectionExpressionOperand(trafficLightColour.rgb),ValueOperand(Number(1)))")
          )
        }
      )

    suite("with @discriminated annotation")(
      test("composition using >>> should bypass intermediate Map") {
        val pe =
          TrafficLightDiscriminated.Box.trafficLightColour >>> TrafficLightDiscriminated.red >>> TrafficLightDiscriminated.Red.rgb
        assert(pe.toString)(equalTo("trafficLightColour.rgb"))
      },
      test("@id annotations at class level are ignored as they do not affect traversal") { // TODO
        // Map(String(rgb) -> Number(42), String(light_type) -> String(red_traffic_light)) // looks this this is never surfaced using RO API
        val pe =
          TrafficLightDiscriminated.Box.trafficLightColour >>> TrafficLightDiscriminated.red
        assert(pe.toString)(equalTo("rgb"))
      } @@ TestAspect.ignore,
      test("@id annotations at field level are honoured") {
        // Map(String(rgb) -> Number(42), String(light_type) -> String(red_traffic_light))
        val pe =
          TrafficLightDiscriminated.Box.trafficLightColour >>> TrafficLightDiscriminated.amber >>> TrafficLightDiscriminated.Amber.rgb
        assert(pe.toString)(equalTo("trafficLightColour.red_green_blue"))
      },
      conditionExpressionSuite
    )
  }

  val nonDiscriminatedSuite = {
    val conditionExpressionSuite =
      suite("ConditionExpression suite")(
        test("TrafficLight.Box.trafficLightColour >>> TrafficLight.red >>> TrafficLight.Red.rgb === 1") {
          val ce = TrafficLight.Box.trafficLightColour >>> TrafficLight.red >>> TrafficLight.Red.rgb === 1
          assert(ce.toString)(
            equalTo("Equals(ProjectionExpressionOperand(trafficLightColour.Red.rgb),ValueOperand(Number(1)))")
          )
        }
      )

    suite("without @discriminated annotation")(
      test("composition using >>> should use intermediate Map") {
        val pe = TrafficLight.Box.trafficLightColour >>> TrafficLight.red >>> TrafficLight.Red.rgb
        assert(pe.toString)(equalTo("trafficLightColour.Red.rgb"))
      },
      test("@id annotations at class level are ignored as they do not affect traversal") {
        // Map(String(rgb) -> Number(42), String(light_type) -> String(red_traffic_light))
        assert(TrafficLightDiscriminated.Red.rgb.toString)(equalTo("rgb"))
      },
      test("@id annotations at field level are honoured") {
        // Map(String(rgb) -> Number(42), String(light_type) -> String(red_traffic_light))
        val pe =
          TrafficLightDiscriminated.Box.trafficLightColour >>> TrafficLightDiscriminated.amber >>> TrafficLightDiscriminated.Amber.rgb
        assert(pe.toString)(equalTo("trafficLightColour.red_green_blue"))
      },
      conditionExpressionSuite
    )
  }

}
