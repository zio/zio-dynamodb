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
import zio.prelude.{ Newtype, Subtype }
import zio.test._
import zio.test.Assertion._

object DdbExprZioPreludeSpec extends ZIOSpecDefault {

  // ── zio-prelude Newtype/Subtype models ──────────────────────────────────────

  // Per-field DDB operators (===, .partitionKey, .sortKey, etc.) each need a
  // Schema[A]/DynamoDBCodec[A] implicit in their own right, so an explicit instance
  // is required here — same as value classes (DdbExprValueClassSpec) and Scala 3
  // opaque types (DdbExprOpaqueTypeSpec). ZB's auto-derivation (see the
  // "Schema.derived auto-derivation" suite below) only elides this boilerplate
  // for a *containing* case class's Schema.derived, not for standalone field use.
  private object UserId extends Newtype[String] {
    implicit val schema: Schema[UserId.Type]       = Schema[String].transform(UserId.wrap, UserId.unwrap)
    implicit val codec: DynamoDBCodec[UserId.Type] = schema.deriving(DynamoDBCodecDeriver).derive
  }

  private object Weight extends Subtype[Int] {
    implicit val schema: Schema[Weight.Type]       = Schema[Int].transform(Weight.wrap, Weight.unwrap)
    implicit val codec: DynamoDBCodec[Weight.Type] = schema.deriving(DynamoDBCodecDeriver).derive
  }

  private case class Product(userId: UserId.Type, weight: Weight.Type)
  private object Product extends CompanionOptics[Product] {
    implicit val schema: Schema[Product]   = Schema.derived
    val userId: Lens[Product, UserId.Type] = $(_.userId)
    val weight: Lens[Product, Weight.Type] = $(_.weight)
  }

  // No explicit Schema[OrderId.Type] defined anywhere: ZB's Schema.derived macro
  // detects the plain (no custom assertion) Newtype and synthesizes the field
  // schema inline while deriving Schema[Order] itself.
  private object OrderId extends Newtype[String]
  private case class Order(id: OrderId.Type, note: String)
  private object Order {
    implicit val schema: Schema[Order] = Schema.derived
  }

  // ── Helpers ───────────────────────────────────────────────────────────────────

  private def render(ce: ConditionExpression[_]): String  = ce.render.execute._2
  private def renderKey(kce: KeyConditionExpr[_]): String = kce.render.execute._2

  private def interpret[S](expr: DdbExpr[S, Boolean]): Either[String, ConditionExpression[S]] =
    DdbExprInterpreter.toConditionExpression(expr)

  private def interpretKey[S](expr: DdbKeyExpr[S]): Either[String, KeyConditionExpr[S]] =
    DdbKeyExprInterpreter.toKeyConditionExpr(expr)

  // ── Spec ──────────────────────────────────────────────────────────────────────

  def spec = suite("DdbExpr — zio-prelude Newtype/Subtype fields")(
    suite("filter expressions via === (schema-aware encoding)")(
      test("Newtype String field === encodes as AttributeValue.String") {
        val expr: DdbExpr[Product, Boolean] = Product.userId === UserId("u-1")
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
                    equalTo(AttributeValue.String("u-1"))
                  )
                )
              )
            )
          )
        )
      },
      test("Subtype Int field === encodes as AttributeValue.Number") {
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
      test("Subtype Int field > literal renders via Builtin path") {
        val expr: DdbExpr[Product, Boolean] = Product.weight > Weight(0)
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString(">"))
          )
      },
      test("Newtype === && Subtype > compose with &&") {
        val expr = (Product.userId === UserId("u-1")) && (Product.weight > Weight(0))
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("AND"))
          )
      },
      test("! of === renders NOT") {
        val expr: DdbExpr[Product, Boolean] = !(Product.userId === UserId("u-1"))
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(startsWithString("NOT"))
          )
      }
    ),

    suite("key expressions via .partitionKey and .sortKey")(
      test("Newtype partitionKey === value produces PartitionKeyEquals") {
        val expr = Product.userId.partitionKey === UserId("u-1")
        assert(interpretKey(expr))(
          isRight(isSubtype[KeyConditionExpr.PartitionKeyEquals[Product]](anything))
        )
      },
      test("Newtype partitionKey renders field alias = value alias") {
        val expr = Product.userId.partitionKey === UserId("u-1")
        interpretKey(expr)
          .map(renderKey)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("=") && containsString(":v"))
          )
      },
      test("Newtype partitionKey + Subtype sortKey equality produces CompositePrimaryKeyExpr") {
        val expr = Product.userId.partitionKey === UserId("u-1") && Product.weight.sortKey === Weight(10)
        assert(interpretKey(expr))(
          isRight(isSubtype[KeyConditionExpr.CompositePrimaryKeyExpr[Product]](anything))
        )
      },
      test("Newtype partitionKey + Subtype sortKey range produces ExtendedCompositePrimaryKeyExpr") {
        val expr = Product.userId.partitionKey === UserId("u-1") && Product.weight.sortKey > Weight(0)
        assert(interpretKey(expr))(
          isRight(isSubtype[KeyConditionExpr.ExtendedCompositePrimaryKeyExpr[Product]](anything))
        )
      },
      test("Newtype partitionKey + Subtype sortKey range renders >") {
        val expr = Product.userId.partitionKey === UserId("u-1") && Product.weight.sortKey > Weight(0)
        interpretKey(expr)
          .map(renderKey)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString(">") && containsString("AND"))
          )
      }
    ),

    suite("codec encoding — explicit Newtype/Subtype Schema")(
      test("Product encodes with primitive (not nested Map) field values") {
        val codec = Product.schema.deriving(DynamoDBCodecDeriver).derive
        assert(codec.encoder(Product(UserId("u-1"), Weight(42))))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("userId")),
              isSome(equalTo(AttributeValue.String("u-1")))
            ) &&
              hasField[AttributeValue.Map, Option[AttributeValue]](
                "value",
                _.value.get(AttributeValue.String("weight")),
                isSome(equalTo(AttributeValue.Number(BigDecimal(42))))
              )
          )
        )
      },
      test("Product round-trips through encode/decode") {
        val codec   = Product.schema.deriving(DynamoDBCodecDeriver).derive
        val product = Product(UserId("u-2"), Weight(7))
        assertTrue(codec.decoder(codec.encoder(product)) == Right(product))
      }
    ),

    suite("Schema.derived auto-derivation (no explicit Schema for the Newtype field)")(
      test("Order (id: OrderId.Type with no given Schema) round-trips via DynamicValue") {
        val order = Order(OrderId("ord-1"), "a note")
        assertTrue(Order.schema.fromDynamicValue(Order.schema.toDynamicValue(order)) == Right(order))
      }
    )
  )

  private def assertNever(msg: String) = assertTrue(msg == "impossible")
}
