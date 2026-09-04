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

import zio.blocks.schema.{ CompanionOptics, Lens, NameMapper, Schema }
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbExprInterpreter }
import zio.dynamodb.blocks.ddbexpr.dsl._
import zio.test._
import zio.test.Assertion._

/**
 * Slice 2b parity: `.filter` / `.where` interpretation threads the calling table's
 * `DynamoDBCodecDeriverConfigure` through to both the attribute names it references and
 * the literals it encodes — so a filtered / conditioned field lands on the same wire name
 * and the same encoding as the item body a `put` writes.
 */
object DdbExprFilterConfigSpec extends ZIOSpecDefault {

  sealed trait Status
  object Status {
    case object Open    extends Status
    case object Shipped extends Status
    implicit val schema: Schema[Status] = Schema.derived
  }

  final case class Order(customerId: String, orderRef: String, status: Status)
  object Order extends CompanionOptics[Order] {
    implicit val schema: Schema[Order]  = Schema.derived
    val customerId: Lens[Order, String] = $(_.customerId)
    val orderRef: Lens[Order, String]   = $(_.orderRef)
    val status: Lens[Order, Status]     = $(_.status)
  }

  // Attribute names referenced by a ConditionExpression (each MapElement path flattened,
  // leaf-last). Enough to check which wire name a filtered field resolved to.
  private def attrPaths(ce: ConditionExpression[_]): List[List[String]] = {
    def fromPe(pe: ProjectionExpression[_, _]): List[String]              = pe match {
      case ProjectionExpression.MapElement(ProjectionExpression.Root, n) => List(n)
      case ProjectionExpression.MapElement(parent, n)                    => fromPe(parent) :+ n
      case ProjectionExpression.ListElement(parent, _)                   => fromPe(parent)
      case _                                                             => Nil
    }
    def fromOp(op: ConditionExpression.Operand[_, _]): List[List[String]] = op match {
      case ConditionExpression.Operand.ProjectionExpressionOperand(pe) => List(fromPe(pe))
      case ConditionExpression.Operand.Size(pe)                        => List(fromPe(pe))
      case _                                                           => Nil
    }
    ce match {
      case ConditionExpression.And(l, r)                => attrPaths(l) ++ attrPaths(r)
      case ConditionExpression.Or(l, r)                 => attrPaths(l) ++ attrPaths(r)
      case ConditionExpression.Not(e)                   => attrPaths(e)
      case ConditionExpression.Equals(l, r)             => fromOp(l) ++ fromOp(r)
      case ConditionExpression.NotEqual(l, r)           => fromOp(l) ++ fromOp(r)
      case ConditionExpression.LessThan(l, r)           => fromOp(l) ++ fromOp(r)
      case ConditionExpression.GreaterThan(l, r)        => fromOp(l) ++ fromOp(r)
      case ConditionExpression.LessThanOrEqual(l, r)    => fromOp(l) ++ fromOp(r)
      case ConditionExpression.GreaterThanOrEqual(l, r) => fromOp(l) ++ fromOp(r)
      case ConditionExpression.Between(op, _, _)        => fromOp(op)
      case ConditionExpression.In(op, _)                => fromOp(op)
      case ConditionExpression.AttributeExists(pe)      => List(fromPe(pe))
      case ConditionExpression.AttributeNotExists(pe)   => List(fromPe(pe))
      case ConditionExpression.Contains(pe, _)          => List(fromPe(pe))
      case ConditionExpression.BeginsWith(pe, _)        => List(fromPe(pe))
      case _                                            => Nil
    }
  }

  private def literalOf(ce: ConditionExpression[_]): Option[AttributeValue] = ce match {
    case ConditionExpression.Equals(_, ConditionExpression.Operand.ValueOperand(v)) => Some(v)
    case ConditionExpression.Equals(ConditionExpression.Operand.ValueOperand(v), _) => Some(v)
    case _                                                                          => None
  }

  private def interp(
    expr: DdbExpr[Order, Boolean],
    cfg: DynamoDBCodecDeriverConfigure[Order]
  ): ConditionExpression[Order] =
    DdbExprInterpreter
      .toConditionExpression(expr, cfg, Order.schema.reflect)
      .fold(msg => throw new AssertionError(s"interpretation failed: $msg"), identity)

  private def bodyFieldName(scalaField: String, cfg: DynamoDBCodecDeriverConfigure[Order]): String =
    Order.schema.deriving(cfg.toDeriver).derive.recordFieldNameMap(scalaField)

  def spec = suite("DdbExprFilterConfigSpec")(
    test("default config: filtered field keeps its raw Scala name") {
      val cfg = DynamoDBCodecDeriverConfigure[Order]()
      val ce  = interp(DdbExpr.Builtin(Order.customerId === "c1"), cfg)
      assertTrue(attrPaths(ce).contains(List("customerId")))
    },
    test("withFieldNameMapper(SnakeCase): filtered field resolves to the same wire name as the body") {
      val cfg = DynamoDBCodecDeriverConfigure[Order]().withFieldNameMapper(NameMapper.SnakeCase)
      val ce  = interp(DdbExpr.Builtin(Order.customerId === "c1"), cfg)
      assertTrue(
        attrPaths(ce).contains(List("customer_id")),
        bodyFieldName("customerId", cfg) == "customer_id"
      )
    },
    test("filtered literal is encoded with the table's config (enumValuesAsStrings = true, the default)") {
      val cfg         = DynamoDBCodecDeriverConfigure[Order]()
      val ce          = interp(DdbExpr.Builtin(Order.status === Status.Shipped), cfg)
      val bodyEncoded = Status.schema.deriving(cfg.toDeriver).derive.encoder(Status.Shipped)
      assertTrue(literalOf(ce).contains(bodyEncoded), literalOf(ce).contains(AttributeValue.String("Shipped")))
    },
    test("filtered literal follows enumValuesAsStrings = false, matching the body codec") {
      val cfg         = DynamoDBCodecDeriverConfigure[Order]().withEnumValuesAsStrings(false)
      val ce          = interp(DdbExpr.Builtin(Order.status === Status.Shipped), cfg)
      val bodyEncoded = Status.schema.deriving(cfg.toDeriver).derive.encoder(Status.Shipped)
      assertTrue(
        literalOf(ce).contains(bodyEncoded),
        !literalOf(ce).contains(AttributeValue.String("Shipped"))
      )
    }
  )
}
