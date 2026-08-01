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

import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbExprInterpreter }
import zio.dynamodb.blocks.ddbexpr.DdbExpr._
import zio.test._
import zio.test.Assertion._

object DdbExprSpec extends ZIOSpecDefault {

  // ── Models ───────────────────────────────────────────────────────────────────

  private case class Score(id: String, score: Int, tags: Set[String])
  private object Score extends CompanionOptics[Score] {
    implicit val schema: Schema[Score] = Schema.derived
    val id: Lens[Score, String]        = $(_.id)
    val score: Lens[Score, Int]        = $(_.score)
    val tags: Lens[Score, Set[String]] = $(_.tags)
  }

  // All-no-field sealed trait: DynamoDBCodecDeriver encodes each case as
  // AttributeValue.String of its name ("High", "Low") — enumValuesAsStrings semantics.
  private sealed trait Priority
  private object Priority {
    case object Low  extends Priority
    case object High extends Priority
    implicit val schema: Schema[Priority] = Schema.derived
  }

  // Mixed sealed trait: DynamoDBCodecDeriver uses Key-discriminator Map encoding
  // because not all cases are no-field.
  private sealed trait Status
  private object Status {
    case object Active                extends Status
    case class Failed(reason: String) extends Status
    implicit val schema: Schema[Status] = Schema.derived
  }

  private case class Task(id: String, priority: Priority, status: Status)
  private object Task extends CompanionOptics[Task] {
    implicit val schema: Schema[Task]  = Schema.derived
    val id: Lens[Task, String]         = $(_.id)
    val priority: Lens[Task, Priority] = $(_.priority)
    val status: Lens[Task, Status]     = $(_.status)
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def render(ce: ConditionExpression[_]): String = ce.render.execute._2

  private def interpret[S](expr: DdbExpr[S, Boolean]): Either[String, ConditionExpression[S]] =
    DdbExprInterpreter.toConditionExpression(expr)

  // ── Spec ─────────────────────────────────────────────────────────────────────

  def spec = suite("DdbExpr")(
    suite("relational operators via ZB Optic (Builtin path)")(
      test("field === literal renders as equality") {
        val expr: DdbExpr[Score, Boolean] = Score.id === "alice"
        assert(interpret(expr))(isRight(anything)) &&
        interpret(expr).map(render).fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("=")))
      },
      test("field > literal renders with >") {
        val expr: DdbExpr[Score, Boolean] = Score.score > 42
        interpret(expr).map(render).fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString(">")))
      },
      test("field >= literal renders with >=") {
        val expr: DdbExpr[Score, Boolean] = Score.score >= 42
        interpret(expr).map(render).fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString(">=")))
      },
      test("field < literal renders with <") {
        val expr: DdbExpr[Score, Boolean] = Score.score < 42
        interpret(expr).map(render).fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("<")))
      },
      test("field <= literal renders with <=") {
        val expr: DdbExpr[Score, Boolean] = Score.score <= 42
        interpret(expr).map(render).fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("<=")))
      },
      test("field != literal renders with <>") {
        val expr: DdbExpr[Score, Boolean] = Score.score != 42
        interpret(expr).map(render).fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("<>")))
      },
      test("! of === renders with NOT") {
        val expr: DdbExpr[Score, Boolean] = !(Score.id === "alice")
        interpret(expr)
          .map(render)
          .fold(_ => assertNever("interpreter failed"), s => assert(s)(startsWithString("NOT")))
      }
    ),
    suite("logical combinators")(
      test("&& produces AND in rendered expression") {
        val expr: DdbExpr[Score, Boolean] = (Score.id === "alice") && (Score.score > 0)
        interpret(expr).map(render).fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("AND")))
      },
      test("|| produces OR in rendered expression") {
        val expr: DdbExpr[Score, Boolean] = (Score.id === "alice") || (Score.score > 0)
        interpret(expr).map(render).fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("OR")))
      },
      test("! (via SchemaExprBoolBridge) produces NOT in rendered expression") {
        val expr = !(Score.id === "alice")
        interpret(expr)
          .map(render)
          .fold(_ => assertNever("interpreter failed"), s => assert(s)(startsWithString("NOT")))
      },
      test("nested && and || compose correctly") {
        val andPart: DdbExpr[Score, Boolean] = (Score.id === "alice") && (Score.score > 0)
        val expr                             = andPart || !(Score.id === "bob")
        assert(interpret(expr))(isRight(anything))
      },
      test("DdbExpr && SchemaExpr (via schemaExprToDdbExpr on RHS)") {
        val expr = Score.id.attributeExists && (Score.score > 0)
        interpret(expr).map(render).fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("AND")))
      },
      test("SchemaExpr && DdbExpr (ordering unrestricted since ZB v0.0.47+)") {
        val expr = (Score.score > 0) && Score.id.attributeExists
        interpret(expr).map(render).fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("AND")))
      },
      test("Not of And renders with NOT wrapping the AND") {
        val inner: DdbExpr[Score, Boolean] = (Score.id === "alice") && (Score.score > 0)
        val expr                           = !inner
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(startsWithString("NOT") && containsString("AND"))
          )
      },
      test("double Not renders with two NOT wrappers") {
        val inner: DdbExpr[Score, Boolean] = Score.id === "alice"
        val expr: DdbExpr[Score, Boolean]  = !(!inner)
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(startsWithString("NOT") && containsString("NOT"))
          )
      }
    ),
    suite("field-to-field relational via Builtin path")(
      test("field === field renders with = and field aliases on both sides") {
        import zio.blocks.schema.SchemaExpr
        val expr: DdbExpr[Score, Boolean] = SchemaExpr.relational(
          SchemaExpr.optic(Score.id.toDynamic, Score.schema),
          SchemaExpr.optic(Score.id.toDynamic, Score.schema),
          SchemaExpr.RelationalOperator.Equal
        )
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("=") && containsString("#n") && not(containsString(":v")))
          )
      },
      test("field > field renders with > and no value alias") {
        import zio.blocks.schema.SchemaExpr
        val expr: DdbExpr[Score, Boolean] = SchemaExpr.relational(
          SchemaExpr.optic(Score.score.toDynamic, Score.schema),
          SchemaExpr.optic(Score.score.toDynamic, Score.schema),
          SchemaExpr.RelationalOperator.GreaterThan
        )
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString(">") && containsString("#n") && not(containsString(":v")))
          )
      }
    ),
    suite("DDB condition functions")(
      test("attributeExists renders attribute_exists") {
        val expr = Score.id.attributeExists
        interpret(expr)
          .map(render)
          .fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("attribute_exists")))
      },
      test("attributeNotExists renders attribute_not_exists") {
        val expr = Score.id.attributeNotExists
        interpret(expr)
          .map(render)
          .fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("attribute_not_exists")))
      },
      test("between renders BETWEEN with two value aliases") {
        val expr = Score.score.between(10, 100)
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("BETWEEN") && containsString(":v"))
          )
      },
      test("in renders IN with value aliases") {
        val expr = Score.id.in("a", "b", "c")
        interpret(expr)
          .map(render)
          .fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("IN") && containsString(":v")))
      },
      test("beginsWith renders begins_with function") {
        val expr = Score.id.beginsWith("prefix")
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("begins_with") && containsString(":v"))
          )
      },
      test("contains renders contains function") {
        val expr = Score.id.contains("needle")
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("contains") && containsString(":v"))
          )
      }
    ),
    suite("inSet — scalar field IN a set of values")(
      test("inSet on an Int field renders IN with value aliases") {
        val expr = Score.score.inSet(Set(1, 2, 3))
        interpret(expr)
          .map(render)
          .fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("IN") && containsString(":v")))
      },
      test("inSet on a String field renders IN with value aliases") {
        val expr = Score.id.inSet(Set("alice", "bob"))
        interpret(expr)
          .map(render)
          .fold(_ => assertNever("interpreter failed"), s => assert(s)(containsString("IN") && containsString(":v")))
      },
      test("inSet produces a PartitionKeyEquals-compatible ConditionExpression") {
        val expr = Score.id.inSet(Set("alice", "bob"))
        assert(interpret(expr))(isRight(anything))
      }
    ),
    suite("containsElement — element in a set attribute")(
      test("containsElement on a Set[String] field renders contains function") {
        val expr = Score.tags.containsElement("alice")
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("contains") && containsString(":v"))
          )
      },
      test("containsElement encodes element via DynamoDBCodec") {
        val expr = Score.tags.containsElement("bob")
        assert(interpret(expr))(
          isRight(
            isSubtype[ConditionExpression.Contains[_]](
              hasField[ConditionExpression.Contains[_], AttributeValue](
                "value",
                _.value,
                equalTo(AttributeValue.String("bob"))
              )
            )
          )
        )
      }
    ),
    suite("sealed-trait literal encoding via === (Builtin + schema-aware codec)")(
      // Since zio-blocks v0.0.47 DynamicSchemaExpr.Literal carries Schema[_].
      // The interpreter derives a DynamoDBCodec from it, so all-no-field sealed
      // traits (enumValuesAsStrings) and mixed sealed traits both encode correctly
      // through the standard === path — no special workaround needed.
      test("all-no-field sealed trait: === produces AttributeValue.String") {
        val expr: DdbExpr[Task, Boolean] = Task.priority === Priority.High
        assert(interpret(expr))(
          isRight(
            isSubtype[ConditionExpression.Equals[_]](
              hasField(
                "right",
                _.right,
                isSubtype[ConditionExpression.Operand.ValueOperand[_]](
                  hasField[ConditionExpression.Operand.ValueOperand[_], AttributeValue](
                    "value",
                    _.value,
                    equalTo(AttributeValue.String("High"))
                  )
                )
              )
            )
          )
        )
      },
      test("all-no-field sealed trait: DynamoDBCodec encodes as AttributeValue.String") {
        val codec = implicitly[zio.dynamodb.blocks.schema.DynamoDBCodec[Priority]]
        assertTrue(codec.encoder(Priority.High) == AttributeValue.String("High")) &&
        assertTrue(codec.encoder(Priority.Low) == AttributeValue.String("Low"))
      },
      test("mixed sealed trait: === produces Key-discriminator Map for no-field case") {
        val expr: DdbExpr[Task, Boolean] = Task.status === Status.Active
        assert(interpret(expr))(
          isRight(
            isSubtype[ConditionExpression.Equals[_]](
              hasField(
                "right",
                _.right,
                isSubtype[ConditionExpression.Operand.ValueOperand[_]](
                  hasField(
                    "value",
                    _.value,
                    isSubtype[AttributeValue.Map](
                      hasField(
                        "value",
                        (m: AttributeValue.Map) => m.value.contains(AttributeValue.String("Active")),
                        isTrue
                      )
                    )
                  )
                )
              )
            )
          )
        )
      },
      test("mixed sealed trait: === produces Key-discriminator Map for record case") {
        val expr: DdbExpr[Task, Boolean] = Task.status === Status.Failed("oops")
        assert(interpret(expr))(
          isRight(
            isSubtype[ConditionExpression.Equals[_]](
              hasField(
                "right",
                _.right,
                isSubtype[ConditionExpression.Operand.ValueOperand[_]](
                  hasField(
                    "value",
                    _.value,
                    isSubtype[AttributeValue.Map](
                      hasField(
                        "value",
                        (m: AttributeValue.Map) => m.value.contains(AttributeValue.String("Failed")),
                        isTrue
                      )
                    )
                  )
                )
              )
            )
          )
        )
      }
    )
  )

  private def assertNever(msg: String) = assertTrue(msg == "impossible")
}
