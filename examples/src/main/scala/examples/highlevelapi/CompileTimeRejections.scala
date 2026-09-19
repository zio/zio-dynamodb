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

import zio.blocks.schema.{ CompanionOptics, Lens, Optional, Schema }
import zio.dynamodb.DynamoDBQuery
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * Shapes the query DSL rejects at compile time. `CompileTimeRejectionsSpec` verifies each one
 * for real via `zio.test.typeCheck`: `inSet`/`remove(index)`/`.contains` used on the wrong
 * field shape, and `between`/`inSet` on a plain `AnyVal` value class (they do work on an
 * opaque type or a zio-prelude `Subtype`/`Newtype` — see `WrappedScalars.scala`). The last one
 * below is different: it compiles, but fails when the query actually runs.
 */
object CompileTimeRejections {

  final case class Widget(id: String, qty: Int, tags: List[String])

  object Widget extends CompanionOptics[Widget] {
    implicit val schema: Schema[Widget] = Schema.derived

    def firstTag: Optional[Widget, String] = $(_.tags.at(0))
  }

  // A non-String Map key compiles fine, but the query fails when it runs, with a
  // DynamoDBError.ItemError.DecodingError — see CompileTimeRejectionsSpec's "live failure" suite.
  final case class Registry(name: String, counts: Map[Int, Int])

  object Registry extends CompanionOptics[Registry] {
    implicit val schema: Schema[Registry] = Schema.derived

    val name: Lens[Registry, String]                  = $(_.name)
    def countAtKey(key: Int): Optional[Registry, Int] = $(_.counts.atKey(key))
  }

  val registries: Table[Registry] = Table[Registry]("registries")

  val buildsFineFailsOnRun: DynamoDBQuery[Registry, Option[Registry]] =
    update(registries)(Registry.name.partitionKey === "r1")(Registry.countAtKey(1).set(99))
}
