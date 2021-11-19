package zio.dynamodb
import zio.test._
import zio.dynamodb.ProjectionExpression._
import zio.test.Assertion._

object AliasMapRenderSpec extends DefaultRunnableSpec {

  val one                                                = AttributeValue.Number(1)
  val two                                                = AttributeValue.Number(2)
  val three                                              = AttributeValue.Number(3)
  val number                                             = AttributeValue.String("N")
  val name                                               = AttributeValue.String("name")
  val between                                            = ConditionExpression
    .Between(
      ConditionExpression.Operand.ValueOperand(two),
      one,
      three
    )
  val in                                                 = ConditionExpression
    .In(
      ConditionExpression.Operand.ValueOperand(one),
      Set(one, two)
    )
  private val projection                                 = "projection"
  private val projectionExpression: ProjectionExpression = $(projection)
  val attributeExists                                    = ConditionExpression.AttributeExists(projectionExpression)
  val attributeNotExists                                 = ConditionExpression.AttributeNotExists(projectionExpression)
  val attributeType                                      = ConditionExpression
    .AttributeType(projectionExpression, AttributeValueType.Number)
  val contains                                           = ConditionExpression
    .Contains(
      projectionExpression,
      one
    )
  val beginsWith                                         = ConditionExpression
    .BeginsWith(
      projectionExpression,
      name
    )

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("AliasMapRender")(
      suite("ConditionExpression")(
        suite("basic renders")(
          test("Between") {
            val (aliasMap, expression) = between.render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0", one -> ":v1", three -> ":v2"), 3))) &&
            assert(expression)(equalTo(":v0 BETWEEN :v1 AND :v2"))
          },
          test("In") {
            val (aliasMap, expression) = in.render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", two -> ":v1"), 2))) &&
            assert(expression)(equalTo(":v0 IN (:v0, :v1)"))
          },
          test("AttributeExists") {
            val (aliasMap, expression) =
              attributeExists.render.render(AliasMap.empty)
            assert(aliasMap.map)(isEmpty) &&
            assert(expression)(equalTo(s"attribute_exists($projection)"))
          },
          test("AttributeNotExists") {
            val (aliasMap, expression) =
              attributeNotExists.render.render(AliasMap.empty)

            assert(aliasMap.map)(isEmpty) &&
            assert(expression)(equalTo(s"attribute_not_exists($projection)"))

          },
          test("AttributeType") {
            val (aliasMap, expression) = attributeType.render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(number -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"attribute_type($projection, :v0)"))
          },
          test("Contains") {
            val (aliasMap, expression) = contains.render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"contains($projection, :v0)"))
          },
          test("BeginsWith") {
            val (aliasMap, expression) = beginsWith.render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(name -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"begins_with($projection, :v0)"))
          },
          test("And") {

            val (aliasMap, expression) = ConditionExpression
              .And(
                between,
                attributeType
              )
              .render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0", one -> ":v1", three -> ":v2", number -> ":v3"), 4))) &&
            assert(expression)(equalTo("(:v0 BETWEEN :v1 AND :v2) AND (attribute_type(projection, :v3))"))
          },
          test("Or") {
            val (aliasMap, expression) = ConditionExpression
              .Or(
                between,
                attributeType
              )
              .render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0", one -> ":v1", three -> ":v2", number -> ":v3"), 4))) &&
            assert(expression)(equalTo("(:v0 BETWEEN :v1 AND :v2) OR (attribute_type(projection, :v3))"))
          },
          test("Not") {
            val (aliasMap, expression) = ConditionExpression
              .Not(beginsWith)
              .render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(name -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"NOT (begins_with($projection, :v0))"))
          },
          test("Equals") {
            val (aliasMap, expression) = ConditionExpression
              .Equals(
                ConditionExpression.Operand.ValueOperand(two),
                ConditionExpression.Operand.Size(projectionExpression)
              )
              .render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"(:v0) = (size($projection))"))
          },
          test("NotEqual") {
            val (aliasMap, expression) = ConditionExpression
              .NotEqual(
                ConditionExpression.Operand.ValueOperand(two),
                ConditionExpression.Operand.Size(projectionExpression)
              )
              .render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"(:v0) <> (size($projection))"))
          },
          test("LessThan") {
            val (aliasMap, expression) = ConditionExpression
              .LessThan(
                ConditionExpression.Operand.ValueOperand(one),
                ConditionExpression.Operand.ValueOperand(three)
              )
              .render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", three -> ":v1"), 2))) &&
            assert(expression)(equalTo("(:v0) < (:v1)"))
          },
          test("GreaterThan") {
            val (aliasMap, expression) = ConditionExpression
              .GreaterThan(
                ConditionExpression.Operand.ValueOperand(one),
                ConditionExpression.Operand.ValueOperand(three)
              )
              .render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", three -> ":v1"), 2))) &&
            assert(expression)(equalTo("(:v0) > (:v1)"))
          },
          test("LessThanOrEqual") {
            val (aliasMap, expression) = ConditionExpression
              .LessThanOrEqual(
                ConditionExpression.Operand.ValueOperand(one),
                ConditionExpression.Operand.ValueOperand(three)
              )
              .render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", three -> ":v1"), 2))) &&
            assert(expression)(equalTo("(:v0) <= (:v1)"))
          },
          test("GreaterThanOrEqual") {
            val (aliasMap, expression) = ConditionExpression
              .GreaterThanOrEqual(
                ConditionExpression.Operand.ValueOperand(one),
                ConditionExpression.Operand.ValueOperand(three)
              )
              .render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", three -> ":v1"), 2))) &&
            assert(expression)(equalTo("(:v0) >= (:v1)"))
          }
        )
      ),
      suite("KeyConditionExpression")(
        suite("SortKeyExpression")(
          test("Equals") {
            val (aliasMap, expression) =
              SortKeyExpression.Equals(SortKeyExpression.SortKey("num"), one).render.render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num = :v0"))
          },
          test("LessThan") {
            val (aliasMap, expression) =
              SortKeyExpression.LessThan(SortKeyExpression.SortKey("num"), one).render.render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num < :v0"))
          },
          test("NotEqual") {
            val (aliasMap, expression) =
              SortKeyExpression.NotEqual(SortKeyExpression.SortKey("num"), one).render.render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num <> :v0"))
          },
          test("GreaterThan") {
            val (aliasMap, expression) =
              SortKeyExpression.GreaterThan(SortKeyExpression.SortKey("num"), one).render.render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num > :v0"))
          },
          test("LessThanOrEqual") {
            val (aliasMap, expression) =
              SortKeyExpression.LessThanOrEqual(SortKeyExpression.SortKey("num"), one).render.render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num <= :v0"))
          },
          test("GreaterThanOrEqual") {
            val (aliasMap, expression) =
              SortKeyExpression.GreaterThanOrEqual(SortKeyExpression.SortKey("num"), one).render.render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num >= :v0"))
          },
          test("Between") {
            val (aliasMap, expression) =
              SortKeyExpression.Between(SortKeyExpression.SortKey("num"), one, two).render.render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", two -> ":v1"), 2))) &&
            assert(expression)(equalTo("num BETWEEN :v0 AND :v1"))
          },
          test("BeginsWith") {
            val (aliasMap, expression) =
              SortKeyExpression.BeginsWith(SortKeyExpression.SortKey("num"), name).render.render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(name -> ":v0"), 1))) &&
            assert(expression)(equalTo("begins_with(num, :v0)"))
          }
        ),
        suite("PartitionKeyExpression")(
          test("Equals") {
            val (aliasMap, expression) = PartitionKeyExpression
              .Equals(PartitionKeyExpression.PartitionKey("num"), one)
              .render
              .render(AliasMap.empty)

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num = :v0"))
          }
        ),
        test("And") {
          val (aliasMap, expression) = KeyConditionExpression
            .And(
              PartitionKeyExpression
                .Equals(PartitionKeyExpression.PartitionKey("num"), two),
              SortKeyExpression.Between(SortKeyExpression.SortKey("num"), one, three)
            )
            .render
            .render(AliasMap.empty)

          assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0", one -> ":v1", three -> ":v2"), 3))) &&
          assert(expression)(equalTo("num = :v0 AND num BETWEEN :v1 AND :v2"))
        }
      )
    )

}
