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

import zio.ZIO
import zio.blocks.schema.{ CompanionOptics, Lens, Modifier, Schema }
import zio.dynamodb.blocks.ddbexpr.{ DdbExprInterpreter, DdbUpdateExprInterpreter }
import zio.dynamodb.blocks.ddbexpr.dsl._
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.test._
import zio.test.Assertion._

/**
 * A `Table` built from a codec with optic-addressed overrides: conditions, key conditions and
 * updates resolve their paths and encode their literals with that codec, so they match what `put`
 * stores. A hand-written top-level codec still encodes and decodes whole items, but expressions
 * on a path fail in the effect.
 */
object TableCodecSpec extends ZIOSpecDefault {

  final case class Sensor(name: String, unit: String)
  object Sensor { implicit val schema: Schema[Sensor] = Schema.derived }

  final case class Reading(id: String, sensor: Sensor, celsius: Int)
  object Reading extends CompanionOptics[Reading] {
    implicit val schema: Schema[Reading]  = Schema.derived
    val id: Lens[Reading, String]         = $(_.id)
    val sensorName: Lens[Reading, String] = $(_.sensor.name)
    val celsius: Lens[Reading, Int]       = $(_.celsius)
  }

  private val reading = Reading("r-1", Sensor("probe", "C"), 21)

  private val intAsString: DynamoDBCodec[Int] = new DynamoDBCodec[Int] {
    def encoder: Encoder[Int] = i => ToAttributeValue[String].toAttributeValue(i.toString)
    def decoder: Decoder[Int] = av => FromAttributeValue[String].fromAttributeValue(av).map(_.toInt)
  }

  private val upper: DynamoDBCodec[String] = new DynamoDBCodec[String] {
    def encoder: Encoder[String] = s => ToAttributeValue[String].toAttributeValue(s.toUpperCase)
    def decoder: Decoder[String] = av => FromAttributeValue[String].fromAttributeValue(av)
  }

  private val overridden: DynamoDBCodec[Reading] =
    Reading.schema
      .deriving(DynamoDBCodecDeriver)
      .modifier(Reading.sensorName, Modifier.rename("sn"))
      .instance(Reading.celsius, intAsString)
      .instance(Reading.id, upper)
      .derive

  private val readings = Table("readings", overridden)

  // Not derived by DynamoDBCodecDeriver, so it has no path structure to navigate.
  private val handWritten: DynamoDBCodec[Reading] = {
    val derived = Reading.schema.deriving(DynamoDBCodecDeriver).derive
    new DynamoDBCodec[Reading] {
      def encoder: Encoder[Reading] = derived.encoder
      def decoder: Decoder[Reading] = derived.decoder
    }
  }

  private val handWrittenReadings = Table("readings", handWritten)

  private def condition(expr: zio.dynamodb.blocks.ddbexpr.DdbExpr[Reading, Boolean]) =
    DdbExprInterpreter.toConditionExpression(expr, overridden)

  private def run[A](q: DynamoDBQuery[_, A]) = ZIO.attempt(DummyIOInterpreter.run(q).unsafeRun())

  private val notDerived = "not derived by DynamoDBCodecDeriver"

  def spec = suite("Table built from a codec")(
    suite("optic overrides reach expressions")(
      test("a renamed nested field: the condition path matches the stored item") {
        val stored = overridden.toItem(reading).map.get("sensor")
        assert(stored)(
          isSome(
            isSubtype[AttributeValue.Map](hasField("keys", _.value.keySet.map(_.value), equalTo(Set("sn", "unit"))))
          )
        ) &&
        assert(condition(Reading.sensorName === "probe"))(
          isRight(
            isSubtype[ConditionExpression.Equals[Reading]](
              hasField(
                "left",
                _.left,
                equalTo(
                  ConditionExpression.Operand.ProjectionExpressionOperand(
                    ProjectionExpression.MapElement(
                      ProjectionExpression.MapElement(ProjectionExpression.Root, "sensor"),
                      "sn"
                    )
                  ): ConditionExpression.Operand[Reading, Any]
                )
              )
            )
          )
        )
      },
      test("a field with an instance override: the condition literal is encoded with that codec") {
        assertTrue(overridden.toItem(reading).map.get("celsius").contains(AttributeValue.String("21"))) &&
        assert(condition(Reading.celsius === 21))(
          isRight(
            isSubtype[ConditionExpression.Equals[Reading]](
              hasField(
                "right",
                _.right,
                equalTo(
                  ConditionExpression.Operand
                    .ValueOperand(AttributeValue.String("21")): ConditionExpression.Operand[Reading, Any]
                )
              )
            )
          )
        )
      },
      test("an update writes the overridden field with its codec") {
        assert(DdbUpdateExprInterpreter.toAction(Reading.celsius.set(30), overridden))(
          isSubtype[UpdateExpression.Action.SetAction[Reading, Any]](
            hasField(
              "operand",
              _.operand,
              equalTo(
                UpdateExpression.SetOperand.ValueOperand(AttributeValue.String("30")): UpdateExpression.SetOperand[Any]
              )
            )
          )
        )
      },
      test("a key condition encodes the key value with the key field's codec") {
        assert(get(readings)(Reading.id.partitionKey === "r-1"))(
          isSubtype[DynamoDBQuery.Map[_, _]](
            hasField(
              "query",
              _.query,
              isSubtype[DynamoDBQuery.GetItem](hasField("key", _.key, equalTo(PrimaryKey("id" -> "R-1"))))
            )
          )
        )
      }
    ),
    suite("a hand-written top-level codec")(
      test("whole items still encode and decode") {
        assert(handWrittenReadings.encode(reading).flatMap(handWrittenReadings.decode))(isRight(equalTo(reading)))
      },
      test("a put builds and runs") {
        assertZIO(run(put(handWrittenReadings, reading)).either)(isRight(anything))
      },
      test("a key condition builds, then fails in the effect") {
        val q = get(handWrittenReadings)(Reading.id.partitionKey === "r-1")
        assertZIO(run(q).either)(isLeft(hasMessage(containsString(notDerived))))
      },
      test("a filter builds, then fails in the effect") {
        val q = scan(handWrittenReadings, limit = 10).filter(Reading.celsius > 20)
        assertZIO(run(q.toQuery).either)(isLeft(hasMessage(containsString(notDerived))))
      },
      test("an update builds, then fails in the effect") {
        val q = update(handWrittenReadings)(Reading.id.partitionKey === "r-1")(Reading.celsius.set(30))
        assertZIO(run(q.toQuery).either)(isLeft(hasMessage(containsString(notDerived))))
      }
    )
  )
}
