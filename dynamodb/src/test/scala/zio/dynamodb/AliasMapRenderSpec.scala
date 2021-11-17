package zio.dynamodb
import zio.test._
import zio.dynamodb.ProjectionExpression._
import zio.test.Assertion._

object AliasMapRenderSpec extends DefaultRunnableSpec {

  val one                                                                    = AttributeValue.Number(1)
  val two                                                                    = AttributeValue.Number(2)
  val three                                                                  = AttributeValue.Number(3)
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("AliasMapRender")(
      suite("ConditionExpression")(
        suite("basic renders")(
          test("AttributeExists") {
            val (aliasMap, expression) =
              ConditionExpression.AttributeExists($("projection")).render.render(AliasMap.empty)
            assert(aliasMap.map)(isEmpty) &&
            assert(expression)(equalTo("attribute_exists(projection)"))
          },
          test("Between") {
            val (aliasMap, expression) = ConditionExpression
              .Between(
                ConditionExpression.Operand.ValueOperand(two),
                one,
                three
              )
              .render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0", one -> ":v1", three -> ":v2"), 3))) &&
            assert(expression)(equalTo(":v0 BETWEEN :v1 AND :v2"))
          },
          test("In") {
            val (aliasMap, expression) = ConditionExpression
              .In(
                ConditionExpression.Operand.ValueOperand(one),
                Set(one, two)
              )
              .render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", two -> ":v1"), 2))) &&
            assert(expression)(equalTo(":v0 IN (:v0, :v1)"))

          }
        )
      )
    )

}
