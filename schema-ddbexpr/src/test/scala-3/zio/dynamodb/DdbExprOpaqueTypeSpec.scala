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

import zio.blocks.schema.{ CompanionOptics, Lens, Modifier, NameMapper, Schema }
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbExprInterpreter, DdbKeyExpr, DdbKeyExprInterpreter }
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure
import zio.dynamodb.blocks.ddbexpr.DdbExpr._
import zio.dynamodb.blocks.ddbexpr.DdbKeyExpr._
import zio.test._
import zio.test.Assertion._

// Defined at file scope to avoid macro circular init with CompanionOptics.
opaque type InvoiceId = String
object InvoiceId:
  given Schema[InvoiceId]         = Schema.string
  def apply(s: String): InvoiceId = s

opaque type Amount = Int
object Amount:
  given Schema[Amount]      = Schema.int
  def apply(n: Int): Amount = n

case class Invoice(id: InvoiceId, amount: Amount) derives Schema
object Invoice extends CompanionOptics[Invoice]:
  val id: Lens[Invoice, InvoiceId]  = $(_.id)
  val amount: Lens[Invoice, Amount] = $(_.amount)

object DdbExprOpaqueTypeSpec extends ZIOSpecDefault {

  private def render(ce: ConditionExpression[_]): String  = ce.render.execute._2
  private def renderKey(kce: KeyConditionExpr[_]): String = kce.render.execute._2

  private def interpret[S](expr: DdbExpr[S, Boolean]): Either[String, ConditionExpression[S]] =
    DdbExprInterpreter.toConditionExpression(expr)

  private def interpretKey[S](expr: DdbKeyExpr[S]): Either[String, KeyConditionExpr[S]] =
    DdbKeyExprInterpreter.toKeyConditionExpr(expr)

  // Interpret a filter through the config-aware overload (the one a configured Table uses),
  // then read back the attribute name and literal the expression references.
  private def interpretConfigured(
    expr: DdbExpr[Invoice, Boolean],
    cfg: DynamoDBCodecDeriverConfigure[Invoice]
  ): ConditionExpression[Invoice] =
    DdbExprInterpreter
      .toConditionExpression(expr, cfg, summon[Schema[Invoice]].reflect)
      .fold(m => throw new AssertionError(s"interpretation failed: $m"), identity)

  private def leafName(ce: ConditionExpression[?]): Option[String] = {
    def pe(p: ProjectionExpression[?, ?]): Option[String] = p match {
      case ProjectionExpression.MapElement(_, n) => Some(n)
      case _                                     => None
    }
    ce match {
      case ConditionExpression.Equals(ConditionExpression.Operand.ProjectionExpressionOperand(p), _)      => pe(p)
      case ConditionExpression.GreaterThan(ConditionExpression.Operand.ProjectionExpressionOperand(p), _) => pe(p)
      case _                                                                                              => None
    }
  }

  private def literalOf(ce: ConditionExpression[?]): Option[AttributeValue] = ce match {
    case ConditionExpression.Equals(_, ConditionExpression.Operand.ValueOperand(v))      => Some(v)
    case ConditionExpression.GreaterThan(_, ConditionExpression.Operand.ValueOperand(v)) => Some(v)
    case _                                                                               => None
  }

  def spec = suite("DdbExpr — Scala 3 opaque type fields")(
    suite("filter expressions via === (schema-aware encoding)")(
      test("opaque String field === encodes as AttributeValue.String") {
        val expr: DdbExpr[Invoice, Boolean] = Invoice.id === InvoiceId("INV-001")
        assert(interpret(expr))(
          isRight(
            isSubtype[ConditionExpression.Equals[_]](
              hasField(
                "right",
                _.right,
                isSubtype[ConditionExpression.Operand.ValueOperand[_]](
                  hasField("value", _.value, equalTo(AttributeValue.String("INV-001")))
                )
              )
            )
          )
        )
      },
      test("opaque Int field === encodes as AttributeValue.Number") {
        val expr: DdbExpr[Invoice, Boolean] = Invoice.amount === Amount(42)
        assert(interpret(expr))(
          isRight(
            isSubtype[ConditionExpression.Equals[_]](
              hasField(
                "right",
                _.right,
                isSubtype[ConditionExpression.Operand.ValueOperand[_]](
                  hasField("value", _.value, equalTo(AttributeValue.Number(BigDecimal(42))))
                )
              )
            )
          )
        )
      },
      test("opaque Int field > literal renders via Builtin path") {
        val expr: DdbExpr[Invoice, Boolean] = Invoice.amount > Amount(0)
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString(">"))
          )
      },
      test("opaque String === && opaque Int > compose with &&") {
        val lhs: DdbExpr[Invoice, Boolean] = Invoice.id === InvoiceId("INV-001")
        val expr                           = lhs && (Invoice.amount > Amount(0))
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("AND"))
          )
      },
      test("! of === renders NOT") {
        val expr: DdbExpr[Invoice, Boolean] = !(Invoice.id === InvoiceId("INV-001"))
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(startsWithString("NOT"))
          )
      }
    ),

    suite("configured deriver - attribute name + literal encoding thread through an opaque field")(
      test("opaque String field: withModifier rename reaches the filter's attribute name") {
        val cfg = DynamoDBCodecDeriverConfigure[Invoice]()
          .withModifier(summon[Schema[Invoice]].reflect.typeId, "id", Modifier.rename("invoice_id"))
        val ce  = interpretConfigured(Invoice.id === InvoiceId("INV-001"), cfg)
        assertTrue(
          leafName(ce).contains("invoice_id"),
          literalOf(ce).contains(AttributeValue.String("INV-001"))
        )
      },
      test("opaque Int field: withFieldNameMapper reaches the attribute name; literal still encodes as Number") {
        val cfg = DynamoDBCodecDeriverConfigure[Invoice]().withFieldNameMapper(NameMapper.SnakeCase)
        val ce  = interpretConfigured(Invoice.amount > Amount(0), cfg)
        assertTrue(
          leafName(ce).contains("amount"),
          literalOf(ce).contains(AttributeValue.Number(BigDecimal(0)))
        )
      },
      test("default config: opaque field keeps its raw name and primitive encoding") {
        val cfg = DynamoDBCodecDeriverConfigure[Invoice]()
        val ce  = interpretConfigured(Invoice.id === InvoiceId("INV-001"), cfg)
        assertTrue(
          leafName(ce).contains("id"),
          literalOf(ce).contains(AttributeValue.String("INV-001"))
        )
      }
    ),

    suite("key expressions via .partitionKey and .sortKey")(
      test("opaque String partitionKey === value produces PartitionKeyEquals") {
        val expr = Invoice.id.partitionKey === InvoiceId("INV-001")
        assert(interpretKey(expr))(
          isRight(isSubtype[KeyConditionExpr.PartitionKeyEquals[Invoice]](anything))
        )
      },
      test("opaque String partitionKey renders field alias = value alias") {
        val expr = Invoice.id.partitionKey === InvoiceId("INV-001")
        interpretKey(expr)
          .map(renderKey)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("=") && containsString(":v"))
          )
      },
      test("opaque String partitionKey + opaque Int sortKey equality produces CompositePrimaryKeyExpr") {
        val expr = Invoice.id.partitionKey === InvoiceId("INV-001") && Invoice.amount.sortKey === Amount(10)
        assert(interpretKey(expr))(
          isRight(isSubtype[KeyConditionExpr.CompositePrimaryKeyExpr[Invoice]](anything))
        )
      },
      test("opaque String partitionKey + opaque Int sortKey range produces ExtendedCompositePrimaryKeyExpr") {
        val expr = Invoice.id.partitionKey === InvoiceId("INV-001") && Invoice.amount.sortKey > Amount(0)
        assert(interpretKey(expr))(
          isRight(isSubtype[KeyConditionExpr.ExtendedCompositePrimaryKeyExpr[Invoice]](anything))
        )
      },
      test("opaque String partitionKey + opaque Int sortKey range renders >") {
        val expr = Invoice.id.partitionKey === InvoiceId("INV-001") && Invoice.amount.sortKey > Amount(0)
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
