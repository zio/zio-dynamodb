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
object OopModelWithAbstractFieldsSpec extends ZIOSpecDefault {

  sealed trait Invoice { def id: Int }
  object Invoice       {
    implicit val schema: Schema[Invoice] = Schema.derived
  }

  sealed trait Billed extends Invoice { def amount: Double }

  case class BilledMonthly(id: Int, amount: Double, month: Int) extends Billed
  object BilledMonthly {
    implicit val schema: Schema[BilledMonthly] = Schema.derived
  }

  case class BilledYearly(id: Int, amount: Double, year: Int) extends Billed
  object BilledYearly {
    implicit val schema: Schema[BilledYearly] = Schema.derived
  }

  case class Prebilled(id: Int, count: Int) extends Invoice
  object Prebilled {
    implicit val schema: Schema[Prebilled] = Schema.derived
  }

  def spec = suite("OO model with abstract fields — low-level API (works today, no ZB dependency)")(
    test("A -> Item -> A round-trips via the derived codec") {
      val codec: DynamoDBCodec[Invoice] = Schema[Invoice].deriving(DynamoDBCodecDeriver).derive
      val original: Invoice             = BilledMonthly(1, 42.0, 3)

      val item = codec.toItem(original)
      val back = codec.fromItem(item)
      assertTrue(back == Right(original))
    },
    test("a filter for one concrete case names the case wrapper explicitly — LL API path, not an optic") {
      val codec: DynamoDBCodec[Invoice] = Schema[Invoice].deriving(DynamoDBCodecDeriver).derive
      val item                          = codec.toItem(BilledMonthly(1, 42.0, 3))

      // The user writes the real wire-level path themselves — "BilledMonthly.amount" —
      // instead of an optic silently resolving (possibly to the wrong case). $ parses
      // dot-notation directly (ProjectionExpressionParser); === is plain LL API syntax,
      // no Invoice type ascription needed — ConditionExpression is contravariant in
      // From, so the resulting ConditionExpression[Any] already works as [Invoice].
      val condition: ConditionExpression[Invoice] = $("BilledMonthly.amount") === 42.0

      // Render aliases path segments/values (#n0, :v0, ...) rather than emitting literal
      // names, so assert on the structure the interpreter actually builds, not a
      // hand-guessed rendered string.
      val rendered = condition.render.execute._2
      assertTrue(
        rendered.matches("""\(#n\d+\.#n\d+\) = \(:v\d+\)"""),
        item.map.get("BilledMonthly").isDefined
      )
    },
    test("matching amount across every Billed case needs an explicit, user-written Or") {
      val monthly                                 = $("BilledMonthly.amount") === 42.0
      val yearly                                  = $("BilledYearly.amount") === 42.0
      val condition: ConditionExpression[Invoice] = monthly || yearly

      val rendered = condition.render.execute._2
      assertTrue(
        rendered.matches("""\(\(#n\d+\.#n\d+\) = \(:v\d+\)\) OR \(\(#n\d+\.#n\d+\) = \(:v\d+\)\)"""),
        condition == ConditionExpression.Or(monthly, yearly)
      )
    }
  )
}
