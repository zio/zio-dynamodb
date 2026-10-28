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
import zio.blocks.schema.derive.Deriver
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.dynamodb.blocks.ddbexpr.{ DdbUpdateExpr, DdbUpdateExprInterpreter }
import zio.dynamodb.blocks.ddbexpr.DdbExpr.OpticUpdateOps
import zio.test._
import zio.test.Assertion._

/**
 * The update-action counterpart of [[DdbExprFilterConfigSpec]]. `DdbExprApi.update`
 * interprets its `DdbUpdateExpr` argument against the table's codec, so a `set` / `add` / …
 * writes the same attribute name and the same operand encoding as the item body a `put` writes.
 *
 * The `Variant` (sealed-trait) operand cases are the point: a sealed-trait operand in
 * `Task.score.set(x)` must encode consistently with the body under a non-default
 * `enumValuesAsStrings` / `caseNameMapper`, not via an ambient, possibly differently
 * configured `implicit DynamoDBCodec[A]`.
 */
object DdbExprUpdateConfigSpec extends ZIOSpecDefault {

  sealed trait Status
  object Status {
    case object Open    extends Status
    case object Shipped extends Status
    implicit val schema: Schema[Status] = Schema.derived
  }

  final case class Order(customerId: String, status: Status, quantity: Int)
  object Order extends CompanionOptics[Order] {
    implicit val schema: Schema[Order]  = Schema.derived
    val customerId: Lens[Order, String] = $(_.customerId)
    val status: Lens[Order, Status]     = $(_.status)
    val quantity: Lens[Order, Int]      = $(_.quantity)
  }

  private def peNames(pe: ProjectionExpression[_, _]): List[String] = pe match {
    case ProjectionExpression.MapElement(ProjectionExpression.Root, n) => List(n)
    case ProjectionExpression.MapElement(parent, n)                    => peNames(parent) :+ n
    case ProjectionExpression.ListElement(parent, _)                   => peNames(parent)
    case _                                                             => Nil
  }

  private def attrPath(a: UpdateExpression.Action[_]): List[String] = a match {
    case UpdateExpression.Action.SetAction(pe, _)    => peNames(pe)
    case UpdateExpression.Action.AddAction(pe, _)    => peNames(pe)
    case UpdateExpression.Action.RemoveAction(pe)    => peNames(pe)
    case UpdateExpression.Action.DeleteAction(pe, _) => peNames(pe)
    case _                                           => Nil
  }

  private def operandValue(a: UpdateExpression.Action[_]): Option[AttributeValue] = a match {
    case UpdateExpression.Action.SetAction(_, UpdateExpression.SetOperand.ValueOperand(v))   => Some(v)
    case UpdateExpression.Action.SetAction(_, UpdateExpression.SetOperand.IfNotExists(_, v)) => Some(v)
    case UpdateExpression.Action.AddAction(_, v)                                             => Some(v)
    case UpdateExpression.Action.DeleteAction(_, v)                                          => Some(v)
    case _                                                                                   => None
  }

  private def interp(u: DdbUpdateExpr[Order], cfg: Deriver[DynamoDBCodec]): UpdateExpression.Action[Order] =
    DdbUpdateExprInterpreter.toAction(u, Order.schema.deriving(cfg).derive)

  private def bodyEncoded[A](value: A, schema: Schema[A], cfg: Deriver[DynamoDBCodec]): AttributeValue =
    schema.deriving(cfg).derive.encoder(value)

  private def bodyFieldName(scalaField: String, cfg: Deriver[DynamoDBCodec]): String =
    Order.schema.deriving(cfg).derive.recordFieldNameMap(scalaField)

  def spec = suite("DdbExprUpdateConfigSpec")(
    test("an optic update action is a deferred DdbUpdateExpr, never a pre-encoded core Action") {
      val action = Order.customerId.set("c9")
      assert(action)(isSubtype[DdbUpdateExpr[_]](anything) && not(isSubtype[UpdateExpression.Action[_]](anything)))
    },
    test("default config: set field keeps its raw Scala name") {
      val cfg = DynamoDBCodecDeriver
      assertTrue(attrPath(interp(Order.customerId.set("c9"), cfg)) == List("customerId"))
    },
    test("withFieldNameMapper(SnakeCase): set field resolves to the same wire name as the body") {
      val cfg = DynamoDBCodecDeriver.withFieldNameMapper(NameMapper.SnakeCase)
      assertTrue(
        attrPath(interp(Order.customerId.set("c9"), cfg)) == List("customer_id"),
        bodyFieldName("customerId", cfg) == "customer_id"
      )
    },
    test("Variant operand: set(Status.Shipped) under default config encodes as the body codec does (String)") {
      val cfg = DynamoDBCodecDeriver
      val a   = interp(Order.status.set(Status.Shipped), cfg)
      assertTrue(
        operandValue(a).contains(bodyEncoded(Status.Shipped, Status.schema, cfg)),
        operandValue(a).contains(AttributeValue.String("Shipped"))
      )
    },
    test("Variant operand: set(Status.Shipped) follows enumValuesAsStrings = false, matching the body codec") {
      val cfg = DynamoDBCodecDeriver.withEnumValuesAsStrings(false)
      val a   = interp(Order.status.set(Status.Shipped), cfg)
      assertTrue(
        operandValue(a).contains(bodyEncoded(Status.Shipped, Status.schema, cfg)),
        !operandValue(a).contains(AttributeValue.String("Shipped"))
      )
    },
    test("Variant operand: set(Status.Shipped) follows caseNameMapper, matching the body codec") {
      val cfg = DynamoDBCodecDeriver.withCaseNameMapper(NameMapper.SnakeCase)
      val a   = interp(Order.status.set(Status.Shipped), cfg)
      assertTrue(operandValue(a).contains(bodyEncoded(Status.Shipped, Status.schema, cfg)))
    },
    test("Variant operand via setIfNotExists is encoded with the table config") {
      val cfg = DynamoDBCodecDeriver.withEnumValuesAsStrings(false)
      val a   = interp(Order.status.setIfNotExists(Status.Open), cfg)
      assertTrue(operandValue(a).contains(bodyEncoded(Status.Open, Status.schema, cfg)))
    },
    test("composed set + add: both operands and both paths thread the table config") {
      val cfg      = DynamoDBCodecDeriver.withFieldNameMapper(NameMapper.SnakeCase)
      val a        = interp(Order.status.set(Status.Shipped) + Order.quantity.add(1), cfg)
      val rendered = a.render.execute._2
      assertTrue(
        rendered.contains("set"),
        rendered.contains("add")
      )
    }
  )
}
