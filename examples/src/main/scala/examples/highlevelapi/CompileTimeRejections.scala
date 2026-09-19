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
 * Shapes the query DSL rejects at compile time, kept as commented-out snippets with the
 * compiler's error alongside each. Scala 3 gives a detailed "Allows Error"; Scala 2.13
 * rejects the same code with a plainer "could not find implicit value" message. The last
 * example here is different: it compiles, but fails when the query actually runs.
 */
object CompileTimeRejections {

  final case class Widget(id: String, qty: Int, tags: List[String])

  object Widget extends CompanionOptics[Widget] {
    implicit val schema: Schema[Widget] = Schema.derived

    // 1. inSet only works on a scalar field, not a list.
    // val badInSet = $(_.tags).inSet(Set(List("a")))
    //   Allows Error: Shape violation at List — Found: SealedTrait(List), Required: N || S || B

    // 2. remove(index) only works on a list field.
    // val badRemoveAt = $(_.qty).remove(0)
    //   Allows Error: Shape violation at Int — Found: Primitive(scala.Int), Required: L

    // 3. .contains only works on a String field.
    // val badContains = $(_.qty).contains("1")
    //   Not found: value contains

    def firstTag: Optional[Widget, String] = $(_.tags.at(0))
  }

  // 4. between/in/inSet don't work on a plain AnyVal value class — they do work on an opaque
  // type or a zio-prelude Subtype/Newtype (see WrappedScalars.scala).
  // final case class Sku(value: String) extends AnyVal
  // ...
  // WidgetWithSku.sku.between(Sku("a"), Sku("z"))
  //   Allows Error: Shape violation at Sku — Found: Record(Sku), Required: N || S || B

  // A non-String Map key compiles fine, but the query fails when it runs, with a
  // DynamoDBError.ItemError.DecodingError.
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
