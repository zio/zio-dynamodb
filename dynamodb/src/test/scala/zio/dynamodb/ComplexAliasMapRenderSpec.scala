package zio.dynamodb

import zio.test.ZIOSpecDefault
import zio.test.assertTrue
import zio.schema.DeriveSchema
import zio.schema.Schema

object ComplexAliasMapRenderSpec extends ZIOSpecDefault {

  sealed trait TrafficLight
  object TrafficLight {
    final case class Green(rgb: Int) extends TrafficLight
    object Green {
      implicit val schema: Schema.CaseClass1[Int, Green] = DeriveSchema.gen[Green]
      val rgb                                            = ProjectionExpression.accessors[Green]
    }
    final case class Red(rgb: Int) extends TrafficLight
    object Red   {
      implicit val schema: Schema.CaseClass1[Int, Red] = DeriveSchema.gen[Red]
      val rgb                                          = ProjectionExpression.accessors[Red]
    }
    final case class Amber(rgb: Int) extends TrafficLight
    object Amber {
      implicit val schema: Schema.CaseClass1[Int, Amber] = DeriveSchema.gen[Amber]
      val rgb                                            = ProjectionExpression.accessors[Amber]
    }
    final case class Box(id: Int, code: Int, trafficLightColour: TrafficLight)
    object Box   {
      implicit val schema: Schema.CaseClass3[Int, Int, TrafficLight, Box] =
        DeriveSchema.gen[Box]
      val (id, code, trafficLightColour)                                  = ProjectionExpression.accessors[Box]
    }

    implicit val schema: Schema.Enum3[Green, Red, Amber, TrafficLight] = DeriveSchema.gen[TrafficLight]

    val (green, red, amber) = ProjectionExpression.accessors[TrafficLight]
  }

  override def spec =
    suite("ComplexAliasMapRenderSpec")(
      test("aliasMap should contain all names and values that appear in the rendered expression") {

        val filterExpr       =
          TrafficLight.Box.trafficLightColour >>> TrafficLight.green >>> TrafficLight.Green.rgb === 1
        val keyConditionExpr = TrafficLight.Box.id.partitionKey === 1
        val projectionsExpr  = ProjectionExpression.projectionsFromSchema[TrafficLight.Box]

        // render a complex AliasMapRender
        val (aliasMap, (maybeFilterExprRendered, maybeKeyExprRendered, projectionsRendered)) = (for {
          filter      <- AliasMapRender.collectAll(Some(filterExpr).map(_.render))
          keyExpr     <- AliasMapRender.collectAll(Some(keyConditionExpr).map(_.render))
          projections <- AliasMapRender.forEach(projectionsExpr.toList)
        } yield (filter, keyExpr, projections)).execute

        val allRendered =
          maybeFilterExprRendered.getOrElse(" ") + maybeKeyExprRendered.getOrElse(" ") + projectionsRendered.mkString(
            " "
          )
        val namesSet    = nameAliasSet(allRendered)
        val valuesSet   = valueAliasSet(allRendered)

        assertTrue(namesSet == namesSetFromAliasMap(aliasMap) && valuesSet == valuesSetFromAliasMap(aliasMap))
      }
    )

  private val nameAliasRegex  = """#n[0-9]+""".r
  private val valueAliasRegex = """:v[0-9]+""".r

  private def nameAliasSet(s: String): Set[String] =
    nameAliasRegex.findAllIn(s).toSet

  private def valueAliasSet(s: String): Set[String] =
    valueAliasRegex.findAllIn(s).toSet

  private def valuesSetFromAliasMap(aliasMap: AliasMap): Set[String] =
    aliasMap.map.values
      .filter(v => !v.contains(".") && v.startsWith(":v"))
      .toSet // filter out FullPath values eg "#n2.#n1.#n0"

  private def namesSetFromAliasMap(aliasMap: AliasMap): Set[String] =
    aliasMap.map.values
      .filter(n => !n.contains(".") && n.startsWith("#n"))
      .toSet // filter out FullPath values eg "#n2.#n1.#n0"

}
