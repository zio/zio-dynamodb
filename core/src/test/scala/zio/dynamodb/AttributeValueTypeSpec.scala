package zio.dynamodb

import zio.test._

object AttributeValueTypeSpec extends ZIOSpecDefault {

  private def render(avt: AttributeValueType): String = avt.render.execute._2

  def spec = suite("AttributeValueType")(
    suite("PrimitiveValueType.render")(
      test("Binary renders as :v alias for 'B'") {
        assertTrue(render(AttributeValueType.Binary).startsWith(":v"))
      },
      test("Number renders as :v alias for 'N'") {
        assertTrue(render(AttributeValueType.Number).startsWith(":v"))
      },
      test("String renders as :v alias for 'S'") {
        assertTrue(render(AttributeValueType.String).startsWith(":v"))
      },
      test("PrimitiveValueType as AttributeValueType dispatches via render on AttributeValueType") {
        val avt: AttributeValueType = AttributeValueType.Binary
        assertTrue(avt.render.execute._2.startsWith(":v"))
      },
      test("PrimitiveValueType Number as AttributeValueType dispatches correctly") {
        val avt: AttributeValueType = AttributeValueType.Number
        assertTrue(avt.render.execute._2.startsWith(":v"))
      }
    ),
    suite("non-primitive AttributeValueType.render")(
      test("Bool renders as :v alias") {
        assertTrue(render(AttributeValueType.Bool).startsWith(":v"))
      },
      test("BinarySet renders as :v alias") {
        assertTrue(render(AttributeValueType.BinarySet).startsWith(":v"))
      },
      test("List renders as :v alias") {
        assertTrue(render(AttributeValueType.List).startsWith(":v"))
      },
      test("Map renders as :v alias") {
        assertTrue(render(AttributeValueType.Map).startsWith(":v"))
      },
      test("NumberSet renders as :v alias") {
        assertTrue(render(AttributeValueType.NumberSet).startsWith(":v"))
      },
      test("Null renders as :v alias") {
        assertTrue(render(AttributeValueType.Null).startsWith(":v"))
      },
      test("StringSet renders as :v alias") {
        assertTrue(render(AttributeValueType.StringSet).startsWith(":v"))
      }
    ),
    suite("attribute_type expression integration")(
      test("attribute_type with Binary type renders correctly") {
        import ConditionExpression._
        import ProjectionExpression.$
        val expr = AttributeType($("field"), AttributeValueType.Binary)
        val s    = expr.render.execute._2
        assertTrue(s.startsWith("attribute_type("))
      },
      test("attribute_type with BinarySet type renders correctly") {
        import ConditionExpression._
        import ProjectionExpression.$
        val expr = AttributeType($("field"), AttributeValueType.BinarySet)
        val s    = expr.render.execute._2
        assertTrue(s.startsWith("attribute_type("))
      },
      test("attribute_type with List type renders correctly") {
        import ConditionExpression._
        import ProjectionExpression.$
        val expr = AttributeType($("field"), AttributeValueType.List)
        val s    = expr.render.execute._2
        assertTrue(s.startsWith("attribute_type("))
      },
      test("attribute_type with Map type renders correctly") {
        import ConditionExpression._
        import ProjectionExpression.$
        val expr = AttributeType($("field"), AttributeValueType.Map)
        val s    = expr.render.execute._2
        assertTrue(s.startsWith("attribute_type("))
      },
      test("attribute_type with NumberSet type renders correctly") {
        import ConditionExpression._
        import ProjectionExpression.$
        val expr = AttributeType($("field"), AttributeValueType.NumberSet)
        val s    = expr.render.execute._2
        assertTrue(s.startsWith("attribute_type("))
      },
      test("attribute_type with Null type renders correctly") {
        import ConditionExpression._
        import ProjectionExpression.$
        val expr = AttributeType($("field"), AttributeValueType.Null)
        val s    = expr.render.execute._2
        assertTrue(s.startsWith("attribute_type("))
      },
      test("attribute_type with StringSet type renders correctly") {
        import ConditionExpression._
        import ProjectionExpression.$
        val expr = AttributeType($("field"), AttributeValueType.StringSet)
        val s    = expr.render.execute._2
        assertTrue(s.startsWith("attribute_type("))
      }
    )
  )
}
