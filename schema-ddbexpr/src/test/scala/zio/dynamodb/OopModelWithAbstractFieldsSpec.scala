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

import zio.blocks.schema.Schema
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.dynamodb.ProjectionExpression.$
import zio.test._

// Exercises the OO-style hierarchy from docs/design/SumTypesWithAbstractFields.md — the
// same shape zio-schema's docs flag as unsupported ("Deep OO-style hierarchies are not
// supported"). Schema derivation and DynamoDBCodecDeriver both already work for this
// shape against standard, published ZB; this project's own decision (see the doc's
// "TL;DR" section) is that its HL API's optic-based sugar (`.partitionKey`, `===`, etc.)
// is never offered for a field declared on an abstract/intermediate case, for either
// discriminator style — not a pending gap, a deliberate, permanent choice, since the LL
// API already covers every case uniformly with no ZB-side dependency at all. This spec
// proves that LL-API-plus-explicit-codec pattern. No Scala-3-only syntax either, hence
// plain (cross-compiled) source, not scala-3/.
//
// Named Oop* to avoid colliding with the unrelated `Invoice`/`InvoiceId`/`Amount`
// file-scope types already defined in DdbExprOpaqueTypeSpec.scala (same package).
sealed trait OopInvoice { def id: Int }
object OopInvoice       {
  implicit val schema: Schema[OopInvoice] = Schema.derived
}

sealed trait OopBilled extends OopInvoice { def amount: Double }

case class OopBilledMonthly(id: Int, amount: Double, month: Int) extends OopBilled
object OopBilledMonthly {
  implicit val schema: Schema[OopBilledMonthly] = Schema.derived
}

case class OopBilledYearly(id: Int, amount: Double, year: Int) extends OopBilled
object OopBilledYearly {
  implicit val schema: Schema[OopBilledYearly] = Schema.derived
}

case class OopPrebilled(id: Int, count: Int) extends OopInvoice
object OopPrebilled {
  implicit val schema: Schema[OopPrebilled] = Schema.derived
}

object OopModelWithAbstractFieldsSpec extends ZIOSpecDefault {

  def spec = suite("OO model with abstract fields — low-level API (works today, no ZB dependency)")(
    test("A -> Item -> A round-trips via the derived codec") {
      val codec: DynamoDBCodec[OopInvoice] = Schema[OopInvoice].deriving(DynamoDBCodecDeriver).derive
      val original: OopInvoice             = OopBilledMonthly(1, 42.0, 3)

      val item = codec.toItem(original)
      val back = codec.fromItem(item)
      assertTrue(back == Right(original))
    },
    test("a filter for one concrete case names the case wrapper explicitly — LL API path, not an optic") {
      val codec: DynamoDBCodec[OopInvoice] = Schema[OopInvoice].deriving(DynamoDBCodecDeriver).derive
      val item                             = codec.toItem(OopBilledMonthly(1, 42.0, 3))

      // The user writes the real wire-level path themselves — "OopBilledMonthly.amount" —
      // instead of an optic silently resolving (possibly to the wrong case). $ parses
      // dot-notation directly (ProjectionExpressionParser); === is plain LL API syntax,
      // no OopInvoice type ascription needed — ConditionExpression is contravariant in
      // From, so the resulting ConditionExpression[Any] already works as [OopInvoice].
      val condition: ConditionExpression[OopInvoice] = $("OopBilledMonthly.amount") === 42.0

      // Render aliases path segments/values (#n0, :v0, ...) rather than emitting literal
      // names, so assert on the structure the interpreter actually builds, not a
      // hand-guessed rendered string.
      val rendered = condition.render.execute._2
      assertTrue(
        rendered.matches("""\(#n\d+\.#n\d+\) = \(:v\d+\)"""),
        item.map.get("OopBilledMonthly").isDefined
      )
    },
    test("matching amount across every Billed case needs an explicit, user-written Or") {
      val monthly                                    = $("OopBilledMonthly.amount") === 42.0
      val yearly                                     = $("OopBilledYearly.amount") === 42.0
      val condition: ConditionExpression[OopInvoice] = monthly || yearly

      val rendered = condition.render.execute._2
      assertTrue(
        rendered.matches("""\(\(#n\d+\.#n\d+\) = \(:v\d+\)\) OR \(\(#n\d+\.#n\d+\) = \(:v\d+\)\)"""),
        condition == ConditionExpression.Or(monthly, yearly)
      )
    }
  )
}
