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
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbExprInterpreter, DdbKeyExpr, DdbKeyExprInterpreter }
import zio.dynamodb.blocks.ddbexpr.DdbExpr._
import zio.dynamodb.blocks.ddbexpr.DdbKeyExpr._
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.test._
import zio.test.Assertion._

object DdbExprValueClassSpec extends ZIOSpecDefault {

  // ── Value class models ────────────────────────────────────────────────────────

  // Transparent schemas: encode as the underlying primitive, not as a single-field
  // record. This is the practical pattern for value classes used as domain IDs/wrappers.
  private case class UserId(value: String) extends AnyVal
  private object UserId {
    implicit val schema: Schema[UserId]       = Schema.string.transform(UserId(_), _.value)
    implicit val codec: DynamoDBCodec[UserId] = schema.deriving(DynamoDBCodecDeriver).derive
  }

  private case class Weight(value: Int) extends AnyVal
  private object Weight {
    implicit val schema: Schema[Weight]       = Schema.int.transform(Weight(_), _.value)
    implicit val codec: DynamoDBCodec[Weight] = schema.deriving(DynamoDBCodecDeriver).derive
  }

  private case class Product(userId: UserId, weight: Weight)
  private object Product extends CompanionOptics[Product] {
    implicit val schema: Schema[Product] = Schema.derived
    val userId: Lens[Product, UserId]    = $(_.userId)
    val weight: Lens[Product, Weight]    = $(_.weight)
  }

  // ── Helpers ───────────────────────────────────────────────────────────────────

  private def render(ce: ConditionExpression[_]): String  = ce.render.execute._2
  private def renderKey(kce: KeyConditionExpr[_]): String = kce.render.execute._2

  private def interpret[S](expr: DdbExpr[S, Boolean]): Either[String, ConditionExpression[S]] =
    DdbExprInterpreter.toConditionExpression(expr)

  private def interpretKey[S](expr: DdbKeyExpr[S]): Either[String, KeyConditionExpr[S]] =
    DdbKeyExprInterpreter.toKeyConditionExpr(expr)

  // ── Spec ──────────────────────────────────────────────────────────────────────

  def spec = suite("DdbExpr — Scala 2 value class fields")(
    suite("filter expressions via === (schema-aware encoding)")(
      test("value class String field === encodes as AttributeValue.String") {
        val expr: DdbExpr[Product, Boolean] = Product.userId === UserId("U-1")
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
                    equalTo(AttributeValue.String("U-1"))
                  )
                )
              )
            )
          )
        )
      },
      test("value class Int field === encodes as AttributeValue.Number") {
        val expr: DdbExpr[Product, Boolean] = Product.weight === Weight(42)
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
                    equalTo(AttributeValue.Number(BigDecimal(42)))
                  )
                )
              )
            )
          )
        )
      },
      test("value class Int field > literal renders via Builtin path") {
        val expr: DdbExpr[Product, Boolean] = Product.weight > Weight(0)
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString(">"))
          )
      },
      test("value class String === && value class Int > compose with &&") {
        val expr = (Product.userId === UserId("U-1")) && (Product.weight > Weight(0))
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("AND"))
          )
      },
      test("! of === renders NOT") {
        val expr: DdbExpr[Product, Boolean] = !(Product.userId === UserId("U-1"))
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(startsWithString("NOT"))
          )
      }
    ),

    suite("key expressions via .partitionKey and .sortKey")(
      test("value class String partitionKey === value produces PartitionKeyEquals") {
        val expr = Product.userId.partitionKey === UserId("U-1")
        assert(interpretKey(expr))(
          isRight(isSubtype[KeyConditionExpr.PartitionKeyEquals[Product]](anything))
        )
      },
      test("value class String partitionKey renders field alias = value alias") {
        val expr = Product.userId.partitionKey === UserId("U-1")
        interpretKey(expr)
          .map(renderKey)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("=") && containsString(":v"))
          )
      },
      test("value class String partitionKey + value class Int sortKey equality produces CompositePrimaryKeyExpr") {
        val expr = Product.userId.partitionKey === UserId("U-1") && Product.weight.sortKey === Weight(10)
        assert(interpretKey(expr))(
          isRight(isSubtype[KeyConditionExpr.CompositePrimaryKeyExpr[Product]](anything))
        )
      },
      test("value class String partitionKey + value class Int sortKey range produces ExtendedCompositePrimaryKeyExpr") {
        val expr = Product.userId.partitionKey === UserId("U-1") && Product.weight.sortKey > Weight(0)
        assert(interpretKey(expr))(
          isRight(isSubtype[KeyConditionExpr.ExtendedCompositePrimaryKeyExpr[Product]](anything))
        )
      },
      test("value class String partitionKey + value class Int sortKey range renders >") {
        val expr = Product.userId.partitionKey === UserId("U-1") && Product.weight.sortKey > Weight(0)
        interpretKey(expr)
          .map(renderKey)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString(">") && containsString("AND"))
          )
      }
    )
  )

  private def assertNever(msg: String) = assertTrue(msg == "impossible")
}
