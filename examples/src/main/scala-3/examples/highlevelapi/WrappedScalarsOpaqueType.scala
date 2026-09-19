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

package examples.highlevelapi

import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.blocks.ddbexpr.DdbExpr
import zio.dynamodb.blocks.ddbexpr.dsl.*
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }

/**
 * A Scala 3 opaque type wrapping a scalar field — see `WrappedScalars.scala` for the same
 * pattern with an `AnyVal` value class and a zio-prelude `Subtype`.
 */
object WrappedScalarsOpaqueType {

  opaque type WidgetId = String
  object WidgetId {
    given Schema[WidgetId]                      = Schema.string
    implicit val codec: DynamoDBCodec[WidgetId] = summon[Schema[WidgetId]].deriving(DynamoDBCodecDeriver).derive
    def apply(s: String): WidgetId              = s
  }

  final case class Widget(widgetId: WidgetId) derives Schema

  object Widget extends CompanionOptics[Widget] {
    val widgetId: Lens[Widget, WidgetId] = $(_.widgetId)
  }

  val widgets: Table[Widget] = Table[Widget]("widgets")

  val widgetIdEq: DdbExpr[Widget, Boolean]      = Widget.widgetId === WidgetId("w-1")
  val widgetIdBetween: DdbExpr[Widget, Boolean] = Widget.widgetId.between(WidgetId("w-1"), WidgetId("w-9"))
}
