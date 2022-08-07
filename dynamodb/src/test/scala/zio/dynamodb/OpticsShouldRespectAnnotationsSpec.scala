package zio.dynamodb

import zio.dynamodb.Annotations.{ discriminator, id }
import zio.test.TestAspect
//import zio.dynamodb.OpticsShouldRespectAnnotationsSpec.TrafficLight
//import zio.dynamodb.ProjectionExpression.Typed
import zio.schema.{ DeriveSchema, Schema }
import zio.test.Assertion.equalTo
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object OpticsShouldRespectAnnotationsSpec extends DefaultRunnableSpec {

  // how do we address the case object sum type here - there are no members?
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

    //  @id("red_traffic_light")
    final case class Red(rgb: Int) extends TrafficLight

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

    // PROBLEM 1: zio-schema SORTS sum type members alphabetically, NOT in the order they are defined
    val (amber, green, red) = ProjectionExpression.accessors[TrafficLight]

    // PROBLEM 2: ATM when using A >>> B we can put anything for B ie it is not type safe
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
  PROBLEMS
  When sum type members are products themselves eg case classes with fields then they lose the @discriminator annotation
  as this is not propagated from the trait
  QUESTIONS
  - is it reasonable to expect zio-schema to take care of this?
  - if not could we change zio-schema schema structure to allow upwards traversal via a parent field?
  - parent annotations?
  - annotations stack ?

   */
  override def spec: ZSpec[Environment, Failure] =
    suite("OpticsShouldRespectAnnotationsSpec")(
      test(
        // Map(String(Green) -> Map(String(rgb) -> Number(42)))
        "when there is no @discriminator (default encoding) sum type should use intermediate Map when accessing a member"
      ) {
        // problem is that @discriminator annotation is at the trait level
        // however the sum type instance eg Green is itself a Product and does not inherit the annotation
        // so we cant use the absence of the annotation as a signal
        assert(TrafficLight.Green.rgb.toString)(equalTo("Green.rgb"))
      } @@ TestAspect.ignore,
      test("products should respect @discriminator annotation when accessing a member") {
        /* this works by pure fluke */
        // Map(String(rgb) -> Number(42), String(light_type) -> String(Green))
        assert(TrafficLightDiscriminated.Green.rgb.toString)(equalTo("rgb"))
      },
      test("@id annotations at class level are ignored as they do not affect traversal") {
        // Map(String(rgb) -> Number(42), String(light_type) -> String(red_traffic_light))
        assert(TrafficLightDiscriminated.Red.rgb.toString)(equalTo("rgb"))
      },
      test("products should respect @id annotation at field level when accessing a member") {
        // Map(String(red_green_blue) -> Number(42), String(light_type) -> String(Amber))
        assert(TrafficLightDiscriminated.Amber.rgb.toString)(equalTo("red_green_blue"))
      },
      test(
        "with simple sum type containing only case objects - we have no fields in the sum type to traverse - hence no entry point for ProjectionExpression.accessors"
      ) {
        // so I think there is nothing extra to do here WRT RO
        val x = BoxOfCaseObjectOnlyEnum.sumType
        println(x)
        assert(BoxOfCaseObjectOnlyEnum.sumType.toString)(equalTo("enum"))
      },
      test("composition using >>> without a discriminator") {
        // Box = Map( String(trafficLightColour) -> Map(String(Red) -> Map(String(rgb) -> Number(42))) )
        val x: ProjectionExpression[TrafficLight]     = TrafficLight.Box.trafficLightColour
        val y: ProjectionExpression[TrafficLight.Red] = TrafficLight.red
        val z: ProjectionExpression[Int]              = TrafficLight.Red.rgb
        val pe: Any                                   =
          TrafficLight.Box.trafficLightColour >>> TrafficLight.red >>> TrafficLight.Red.rgb
        println(s"XXXXXXXXXXX pe=$pe x=$x y=$y z=$z")
        assert(pe.toString)(equalTo("trafficLightColour.Red.rgb"))
      }
// This fails to compile which is good!
//      test("but >>> is not type safe WRT composition  - we do the above in reverse order!") {
//        // Box = Map( String(trafficLightColour) -> Map(String(Red) -> Map(String(rgb) -> Number(42))) )
//        val pe =
//          TrafficLight.Red.rgb >>> TrafficLight.red >>> TrafficLight.Box.trafficLightColour
//        println(s"XXXXXXXXXXX x=$pe")
//        assert(pe.toString)(equalTo("rgb.Red.trafficLightColour"))
//      }
    )

}
