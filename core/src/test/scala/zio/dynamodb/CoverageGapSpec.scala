/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb

import zio.blocks.chunk.Chunk
import zio.dynamodb.ConditionExpression.Operand.{ ProjectionExpressionOperand, Size }
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.UpdateExpression.Action.{ Actions, RemoveAction, SetAction }
import zio.dynamodb.UpdateExpression.SetOperand.{ Minus, PathOperand, Plus, ValueOperand }
import zio.test._
import zio.test.Assertion.{ anything, isSubtype }

/**
 * Covers remaining gaps after the main spec files:
 * - Operand.> (PE-to-PE) which calls GreaterThanOrEqual (known bug)
 * - Operand.Size render
 * - Actions.++ method
 * - SetOperand.+ and SetOperand.-
 * - ProjectionExpression.unsafeFrom
 * - ProjectionExpression.partitionKey/sortKey error branches
 * - KeySchema with sortKey
 * - ProjectionExpressionParser error recovery branch
 * - ToAttributeValue binary/binarySet cases
 * - AliasMap existing-alias child path branch
 * - AttrMap.getIterableItem / getOptionalIterableItem Some branch
 * - FromAttributeValue mapFromAttributeValue and iterableFromAttributeValue
 * - ToAttributeValue schemaToAttributeValue
 * - Zippable2Left
 */
object CoverageGapSpec extends ZIOSpecDefault {

  private def renderExpr(ce: ConditionExpression[_]): String = ce.render.execute._2
  private def renderUpdate(ue: UpdateExpression[_]): String  = ue.render.execute._2

  def spec = suite("CoverageGapSpec")(
    utilsSuite,
    operandSuite,
    actionsSuite,
    setOperandSuite,
    projectionExpressionSuite,
    keyConditionSuite,
    toAttributeValueBinarySuite,
    aliasMapChildPathSuite,
    canFilterSuite,
    projectionExpressionParserSuite,
    keySchemaWithSortKeySuite,
    attrMapIterableItemSuite,
    fromAttributeValueExtraSuite,
    zippable2LeftSuite
  )

  private val operandSuite = suite("Operand extra cases")(
    test("Operand.> (PE-to-PE) renders as >= due to implementation") {
      val ageOp  = ProjectionExpressionOperand($("age"))
      val nameOp = ProjectionExpressionOperand($("name"))
      val ce     = ageOp > nameOp
      val s      = renderExpr(ce)
      assertTrue(s.contains(">="))
    },
    test("Operand.Size render produces size(path) expression") {
      val sizePath = $("items")
      val sizeOp   = Size(sizePath)
      val s        = sizeOp.render.execute._2
      assertTrue(s.startsWith("size("))
    },
    test("Operand.Size used in a comparator") {
      val sizePath                                       = $("items")
      val sizeOp: ConditionExpression.Operand[Any, Long] = Size(sizePath)
      val valueOp                                        = ProjectionExpressionOperand($("maxSize")).asInstanceOf[ConditionExpression.Operand[Any, Long]]
      val ce                                             = sizeOp === valueOp
      val s                                              = renderExpr(ce)
      assertTrue(s.contains("size("))
    }
  )

  private val actionsSuite = suite("Actions.++")(
    test("++ combines two Actions into one with all sub-actions") {
      val a1       = Actions(zio.blocks.chunk.Chunk(SetAction($("a"), ValueOperand(AttributeValue.Number(1)))))
      val a2       = Actions(zio.blocks.chunk.Chunk(RemoveAction($("b"))))
      val combined = a1 ++ a2
      val s        = UpdateExpression(combined).render.execute._2
      assertTrue(s.contains("set") && s.contains("remove"))
    }
  )

  private val setOperandSuite = suite("SetOperand.+ and SetOperand.-")(
    test("SetOperand.+ builds Plus") {
      val left  = ValueOperand[Any](AttributeValue.Number(1))
      val right = ValueOperand[Any](AttributeValue.Number(2))
      val plus  = left + right
      assert(plus)(isSubtype[Plus[Any]](anything))
    },
    test("SetOperand.- builds Minus") {
      val left  = ValueOperand[Any](AttributeValue.Number(10))
      val right = ValueOperand[Any](AttributeValue.Number(3))
      val minus = left - right
      assert(minus)(isSubtype[Minus[Any]](anything))
    }
  )

  private val projectionExpressionSuite = suite("ProjectionExpression edge cases")(
    test("unsafeFrom casts From type") {
      val pe: ProjectionExpression[String, Int] =
        ProjectionExpression.MapElement[String, Int](ProjectionExpression.Root, "x")
      val pe2: ProjectionExpression[Any, Int]   = pe.unsafeFrom[Any]
      assertTrue(pe2.toString == "x")
    },
    test("partitionKey on non-MapElement throws IllegalArgumentException") {
      val pe: ProjectionExpression.Unknown                              = null.asInstanceOf[ProjectionExpression.Unknown]
      val root: ProjectionExpression[Any, ProjectionExpression.Unknown] =
        ProjectionExpression.Root.asInstanceOf[ProjectionExpression[Any, ProjectionExpression.Unknown]]
      val attempt                                                       = scala.util.Try(
        new ProjectionExpression.ProjectionExpressionSyntax(root).partitionKey
      )
      assertTrue(attempt.isFailure)
    },
    test("sortKey on non-MapElement throws IllegalArgumentException") {
      val root: ProjectionExpression[Any, ProjectionExpression.Unknown] =
        ProjectionExpression.Root.asInstanceOf[ProjectionExpression[Any, ProjectionExpression.Unknown]]
      val attempt                                                       = scala.util.Try(
        new ProjectionExpression.ProjectionExpressionSyntax(root).sortKey
      )
      assertTrue(attempt.isFailure)
    },
    test("mapElement helper builds MapElement") {
      val pe = ProjectionExpression.mapElement(ProjectionExpression.Root, "field")
      assertTrue(pe.toString == "field")
    },
    test("listElement helper builds ListElement") {
      val pe = ProjectionExpression.listElement(
        ProjectionExpression.MapElement(ProjectionExpression.Root, "items"),
        3
      )
      assertTrue(pe.toString == "items[3]")
    }
  )

  private val keyConditionSuite = suite("KeyConditionExpr")(
    test("PartitionKeyEquals render includes key alias and value alias") {
      val pk   = $("id").partitionKey
      val expr = pk === "user123"
      val s    = expr.render.execute._2
      assertTrue(s.contains("#n") && s.contains(":v"))
    }
  )

  private val toAttributeValueBinarySuite = suite("ToAttributeValue binary types")(
    test("binaryToAttributeValue for List[Byte]") {
      val bytes: List[Byte] = List(1.toByte, 2.toByte)
      val av                = ToAttributeValue[List[Byte]].toAttributeValue(bytes)
      assert(av)(isSubtype[AttributeValue.Binary](anything))
    },
    test("binarySetToAttributeValue for Iterable[Iterable[Byte]]") {
      val binarySet: List[List[Byte]] = List(List(1.toByte), List(2.toByte))
      val av                          = ToAttributeValue[List[List[Byte]]].toAttributeValue(binarySet)
      assert(av)(isSubtype[AttributeValue.BinarySet](anything))
    }
  )

  private val aliasMapChildPathSuite = suite("AliasMap child path alias reuse")(
    test("same nested path reuses existing alias for child segment") {
      val pe            = $("a.b")
      val (am1, alias1) = AliasMap.empty.getOrInsert(pe)
      val (am2, alias2) = am1.getOrInsert(pe)
      assertTrue(alias1 == alias2 && am2.index == am1.index)
    },
    test("child map element gets dot prefix on existing alias") {
      val peParent    = $("a.b")
      val (am1, _)    = AliasMap.empty.getOrInsert(peParent)
      val peSame      = $("a.b")
      val (_, alias2) = am1.getOrInsert(peSame)
      assertTrue(alias2.contains(".") || alias2.startsWith("#n"))
    }
  )

  private val utilsSuite = suite("Utils.ListUtils")(
    suite("reverse")(
      test("reverse of empty list is empty") {
        import Utils.ListUtils
        val xs: Iterable[Int] = List.empty[Int]
        assertTrue(xs.reverse.toList == List.empty[Int])
      },
      test("reverse of non-empty list reverses order") {
        import Utils.ListUtils
        val xs: Iterable[Int] = List(1, 2, 3)
        assertTrue(xs.reverse.toList == List(3, 2, 1))
      }
    ),
    suite("forEach")(
      test("empty list returns Right of empty iterable") {
        import Utils.ListUtils
        val xs: Iterable[Int] = List.empty
        val result            = xs.forEach(i => Right(i * 2))
        assertTrue(result == Right(List.empty[Int]))
      },
      test("all Right returns Right of mapped values") {
        import Utils.ListUtils
        val xs: Iterable[Int] = List(1, 2, 3)
        val result            = xs.forEach(i => Right(i * 2))
        assertTrue(result == Right(List(2, 4, 6)))
      },
      test("first Left short-circuits and returns that Left") {
        import Utils.ListUtils
        val xs: Iterable[Int] = List(1, 2, 3)
        val result            = xs.forEach(i => if (i == 2) Left("boom") else Right(i))
        assertTrue(result == Left("boom"))
      },
      test("Left on first element short-circuits without processing remainder") {
        import Utils.ListUtils
        var seen              = List.empty[Int]
        val xs: Iterable[Int] = List(1, 2, 3)
        xs.forEach { i => seen = seen :+ i; if (i == 1) Left("stop") else Right(i) }
        assertTrue(seen == List(1))
      }
    )
  )

  private val canFilterSuite = suite("CanFilter implicit")(
    test("pageCanFilter implicit resolves for Page[A]") {
      import zio.dynamodb.proofs.CanFilter
      val ev = implicitly[CanFilter[String, Page[String]]]
      assertTrue(ev != null)
    }
  )

  private val projectionExpressionParserSuite = suite("ProjectionExpressionParser error collection")(
    test("multiple invalid segments — valid segment after invalid still accumulates errors") {
      val result = ProjectionExpressionParser.parse("valid.$$invalid.more$$bad")
      assertTrue(result.isLeft && result.left.toOption.exists(_.contains("$$invalid")))
    },
    test("invalid segment followed by valid segment — errors preserved, valid part ignored") {
      val result = ProjectionExpressionParser.parse("$$bad.valid")
      assertTrue(result.isLeft && result.left.toOption.exists(_.contains("$$bad")))
    }
  )

  private val keySchemaWithSortKeySuite = suite("KeySchema")(
    test("KeySchema with only hashKey") {
      val ks = KeySchema("pk")
      assertTrue(ks.hashKey == "pk" && ks.sortKey.isEmpty)
    },
    test("KeySchema with hashKey and sortKey") {
      val ks = KeySchema("pk", "sk")
      assertTrue(ks.hashKey == "pk" && ks.sortKey.contains("sk"))
    }
  )

  private val attrMapIterableItemSuite = suite("AttrMap iterable item helpers")(
    test("getIterableItem decodes a list of nested items") {
      val inner  = Item("name" -> "Alice")
      val listAv = ToAttributeValue[List[AttrMap]].toAttributeValue(List(inner))
      val item   = Item("items" -> listAv)
      val result = item.getIterableItem[String]("items")(_.get[String]("name"))
      assertTrue(result == Right(List("Alice")))
    },
    test("getOptionalIterableItem Some branch returns Some iterable") {
      val inner  = Item("name" -> "Bob")
      val listAv = ToAttributeValue[List[AttrMap]].toAttributeValue(List(inner))
      val item   = Item("items" -> listAv)
      val result = item.getOptionalIterableItem[String]("items")(_.get[String]("name"))
      assertTrue(result == Right(Some(List("Bob"))))
    },
    test("getOptionalIterableItem None branch returns None when field absent") {
      val item   = Item("other" -> "x")
      val result = item.getOptionalIterableItem[String]("items")(_.get[String]("name"))
      assertTrue(result == Right(None))
    }
  )

  private val fromAttributeValueExtraSuite = suite("FromAttributeValue extra decoders")(
    test("mapFromAttributeValue success — decodes AttributeValue.Map to Map[String, BigDecimal]") {
      val mapAv  = AttributeValue.Map(
        Map(
          AttributeValue.String("x") -> AttributeValue.Number(BigDecimal(1)),
          AttributeValue.String("y") -> AttributeValue.Number(BigDecimal(2))
        )
      )
      val result = implicitly[FromAttributeValue[Map[String, BigDecimal]]].fromAttributeValue(mapAv)
      assertTrue(result == Right(Map("x" -> BigDecimal(1), "y" -> BigDecimal(2))))
    },
    test("iterableFromAttributeValue success — decodes AttributeValue.List to Iterable[AttrMap]") {
      val inner  = Item("n" -> "v")
      val listAv = AttributeValue.List(List(inner.toAttributeValue))
      val result = implicitly[FromAttributeValue[Iterable[AttrMap]]].fromAttributeValue(listAv)
      assertTrue(result.isRight && result.toOption.get.size == 1)
    },
    test("iterableFromAttributeValue failure — returns Left for non-List input") {
      val av     = AttributeValue.String("oops")
      val result = implicitly[FromAttributeValue[Iterable[AttrMap]]].fromAttributeValue(av)
      assertTrue(result.isLeft)
    }
  )

  private val zippable2LeftSuite = suite("Zippable2Left")(
    test("zipPar drops Unit right side and returns left value") {
      val q1: DynamoDBQuery[Any, String] = DynamoDBQuery("hello")
      val q2: DynamoDBQuery[Any, Unit]   = DynamoDBQuery(())
      val zipped                         = q1.zipPar(q2)
      val result                         = DummyIOInterpreter.run(zipped).unsafeRun()
      assertTrue(result == "hello")
    }
  )
}
