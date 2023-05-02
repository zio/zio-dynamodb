package zio.dynamodb

import zio.schema.annotation.{ caseName, discriminatorName, fieldName }
import zio.schema.{ DeriveSchema, Schema }
import zio.test.Assertion.equalTo
import zio.test.{ assert, ZIOSpecDefault }

object OpticsShouldRespectAnnotationsSpec extends ZIOSpecDefault {

  private val trafficLightColour = "trafficLightColour"
  private val Green              = "Green"
  private val Amber              = "Amber"
  private val rgb                = "rgb"
  private val red_traffic_light  = "red_traffic_light"
  private val red_green_blue     = "red_green_blue"

  sealed trait CaseObjectOnlyEnum
  final case class BoxOfCaseObjectOnlyEnum(`enum`: CaseObjectOnlyEnum)
  object BoxOfCaseObjectOnlyEnum {

    case object ONE extends CaseObjectOnlyEnum
    @caseName("2")
    case object TWO extends CaseObjectOnlyEnum
    implicit val schema: Schema[BoxOfCaseObjectOnlyEnum] = DeriveSchema.gen[BoxOfCaseObjectOnlyEnum]
    val sumType                                          = ProjectionExpression.accessors[BoxOfCaseObjectOnlyEnum]
  }

  sealed trait TrafficLight

  object TrafficLight {

    final case class Green(rgb: Int) extends TrafficLight

    object Green {
      implicit val schema: Schema.CaseClass1[Int, Green] = DeriveSchema.gen[Green]
      val rgb: ProjectionExpression[Green, Int]          = ProjectionExpression.accessors[Green]
    }

    @caseName("red_traffic_light")
    final case class Red(rgb: Int) extends TrafficLight

    object Red {
      implicit val schema: Schema.CaseClass1[Int, Red] = DeriveSchema.gen[Red]
      val rgb: ProjectionExpression[Red, Int]          = ProjectionExpression.accessors[Red]
    }

    final case class Amber(@fieldName("red_green_blue") rgb: Int) extends TrafficLight

    object Amber {
      implicit val schema: Schema.CaseClass1[Int, Amber] = DeriveSchema.gen[Amber]
      val rgb                                            = ProjectionExpression.accessors[Amber]
    }

    final case class Box(trafficLightColour: TrafficLight)

    object Box {
      implicit val schema: Schema.CaseClass1[TrafficLight, Box] = DeriveSchema.gen[Box]
      val trafficLightColour                                    = ProjectionExpression.accessors[Box]
    }

    implicit val schema: Schema.Enum3[Green, Red, Amber, TrafficLight] = DeriveSchema.gen[TrafficLight]

    val (green, red, amber) = ProjectionExpression.accessors[TrafficLight]

  }

  @discriminatorName("light_type")
  sealed trait TrafficLightDiscriminated

  object TrafficLightDiscriminated {
    final case class Green(rgb: Int) extends TrafficLightDiscriminated

    object Green {
      implicit val schema: Schema.CaseClass1[Int, Green] = DeriveSchema.gen[Green]
      val rgb: ProjectionExpression[Green, Int]          = ProjectionExpression.accessors[Green]
    }

    @caseName("red_traffic_light")
    final case class Red(rgb: Int) extends TrafficLightDiscriminated

    object Red {
      implicit val schema: Schema.CaseClass1[Int, Red] = DeriveSchema.gen[Red]
      val rgb: ProjectionExpression[Red, Int]          = ProjectionExpression.accessors[Red]
    }

    final case class Amber(@fieldName("red_green_blue") rgb: Int) extends TrafficLightDiscriminated

    object Amber {
      implicit val schema: Schema.CaseClass1[Int, Amber] = DeriveSchema.gen[Amber]
      val rgb: ProjectionExpression[Amber, Int]          = ProjectionExpression.accessors[Amber]
    }

    final case class Box(trafficLightColour: TrafficLightDiscriminated)

    object Box {
      implicit val schema: Schema.CaseClass1[TrafficLightDiscriminated, Box]       =
        DeriveSchema.gen[Box]
      val trafficLightColour: ProjectionExpression[Box, TrafficLightDiscriminated] = ProjectionExpression.accessors[Box]
    }

    implicit val schema: Schema.Enum3[Green, Red, Amber, TrafficLightDiscriminated] =
      DeriveSchema.gen[TrafficLightDiscriminated]
    val (green, red, amber)                                                         = ProjectionExpression.accessors[TrafficLightDiscriminated]

  }

  override def spec =
    suite("OpticsShouldRespectAnnotationsSpec")(nonDiscriminatedSuite, discriminatedSuite)

  val discriminatedSuite = {
    val conditionExpressionSuite =
      suite("ConditionExpression suite")(
        test("TrafficLight.Box.trafficLightColour >>> TrafficLight.red >>> TrafficLight.Red.rgb === 1") {
          // Map(String(rgb) -> Number(42), String(light_type) -> String(red_traffic_light))
          val ce =
            TrafficLightDiscriminated.Box.trafficLightColour >>> TrafficLightDiscriminated.red >>> TrafficLightDiscriminated.Red.rgb === 1
          assert(ce.toString)(
            equalTo(s"Equals(ProjectionExpressionOperand($trafficLightColour.$rgb),ValueOperand(Number(1)))")
          )
        }
      )

    suite("with @discriminated annotation")(
      test("composition using >>> should bypass intermediate Map") {
        // Map(String(rgb) -> Number(42), String(light_type) -> String(Green))
        val pe =
          TrafficLightDiscriminated.Box.trafficLightColour >>> TrafficLightDiscriminated.green >>> TrafficLightDiscriminated.Green.rgb
        assert(pe.toString)(equalTo(s"$trafficLightColour.$rgb"))
      },
      test(
        "@caseName annotations at class level do not affect traversal as they are bypassed ie trafficLightColour.rgb"
      ) {
        // Map(String(rgb) -> Number(42), String(light_type) -> String(red_traffic_light))
        val pe =
          TrafficLightDiscriminated.Box.trafficLightColour >>> TrafficLightDiscriminated.red >>> TrafficLightDiscriminated.Red.rgb
        assert(pe.toString)(equalTo(s"$trafficLightColour.$rgb"))
      },
      test("@fieldName annotations are honoured") {
        // Map(String(rgb) -> Number(42), String(light_type) -> String(Amber))
        val pe =
          TrafficLightDiscriminated.Box.trafficLightColour >>> TrafficLightDiscriminated.amber >>> TrafficLightDiscriminated.Amber.rgb
        assert(pe.toString)(equalTo(s"$trafficLightColour.$red_green_blue"))
      },
      conditionExpressionSuite
    )
  }

  val nonDiscriminatedSuite = {
    val conditionExpressionSuite =
      suite("ConditionExpression suite")(
        test("Path with no @caseName or @fieldName annotations results in a PE of trafficLightColour.Green.rgb") {
          val ce = TrafficLight.Box.trafficLightColour >>> TrafficLight.green >>> TrafficLight.Green.rgb === 1
          assert(ce.toString)(
            equalTo(s"Equals(ProjectionExpressionOperand($trafficLightColour.$Green.$rgb),ValueOperand(Number(1)))")
          )
        },
        test("Path with @caseName but no @fieldName results in a PE of trafficLightColour.red_traffic_light.rgb") {
          val ce = TrafficLight.Box.trafficLightColour >>> TrafficLight.red >>> TrafficLight.Red.rgb === 1
          assert(ce.toString)(
            equalTo(
              s"Equals(ProjectionExpressionOperand($trafficLightColour.$red_traffic_light.$rgb),ValueOperand(Number(1)))"
            )
          )
        }
      )

    suite("without @discriminated annotation")(
      test("composition using >>> should use intermediate Map") {
        val pe = TrafficLight.Box.trafficLightColour >>> TrafficLight.green >>> TrafficLight.Green.rgb
        assert(pe.toString)(equalTo(s"$trafficLightColour.$Green.$rgb"))
      },
      test("@caseName annotations are honoured") {
        // Map(trafficLightColour -> Map(String(red_traffic_light) -> Map(String(rgb) -> Number(42))))
        val pe =
          TrafficLight.Box.trafficLightColour >>> TrafficLight.red >>> TrafficLight.Red.rgb
        assert(pe.toString)(equalTo(s"$trafficLightColour.$red_traffic_light.$rgb"))
      },
      test("@fieldName annotations are honoured") {
        val pe =
          TrafficLight.Box.trafficLightColour >>> TrafficLight.amber >>> TrafficLight.Amber.rgb
        assert(pe.toString)(equalTo(s"$trafficLightColour.$Amber.$red_green_blue"))
      },
      conditionExpressionSuite
    )
  }

}
