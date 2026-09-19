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
import zio.dynamodb.DynamoDBQuery
import zio.dynamodb.blocks.ddbexpr.DdbUpdateExpr
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * Every update action: `set`/`set(otherOptic)`/`setIfNotExists` (any attribute type);
 * `increment`/`decrement`/`add` (numbers — `add` is DynamoDB's numeric `ADD`, distinct from
 * `addSet`'s set union); `addSet`/`deleteFromSet` (sets); `appendList`/`prependList`/
 * `remove`/`remove(index)` (lists); and `+` to combine several actions into one update.
 */
object UpdateActions {

  final case class Item(id: String, name: String, altName: String, qty: Int, tags: Set[String], items: List[String])

  object Item extends CompanionOptics[Item] {
    implicit val schema: Schema[Item] = Schema.derived

    val id: Lens[Item, String]          = $(_.id)
    val name: Lens[Item, String]        = $(_.name)
    val altName: Lens[Item, String]     = $(_.altName)
    val qty: Lens[Item, Int]            = $(_.qty)
    val tags: Lens[Item, Set[String]]   = $(_.tags)
    val items: Lens[Item, List[String]] = $(_.items)
  }

  val items: Table[Item] = Table[Item]("items")

  // any attribute type
  val setName: DdbUpdateExpr[Item]     = Item.name.set("renamed")
  val copyName: DdbUpdateExpr[Item]    = Item.altName.set(Item.name) // SET altName = name
  val setIfAbsent: DdbUpdateExpr[Item] = Item.name.setIfNotExists("default")
  val removeName: DdbUpdateExpr[Item]  = Item.name.remove

  // N / Wrapped[N]
  val incrementQty: DdbUpdateExpr[Item] = Item.qty.increment(1)
  val decrementQty: DdbUpdateExpr[Item] = Item.qty.decrement(1)
  val addToQty: DdbUpdateExpr[Item]     = Item.qty.add(5) // numeric ADD, not set-union

  // NS / SS / BS
  val addTags: DdbUpdateExpr[Item]    = Item.tags.addSet(Set("new"))
  val removeTags: DdbUpdateExpr[Item] = Item.tags.deleteFromSet(Set("old"))

  // L
  val appendItems: DdbUpdateExpr[Item]       = Item.items.appendList(Seq("a", "b"))
  val prependItems: DdbUpdateExpr[Item]      = Item.items.prependList(Seq("z"))
  val removeItemAtIndex: DdbUpdateExpr[Item] = Item.items.remove(0)

  // Combine several into one UpdateItem call
  val combined: DdbUpdateExpr[Item] =
    Item.qty.increment(1) + Item.name.set("updated") + Item.tags.addSet(Set("done"))

  // Wired into a real update — see UpdateActionsSpec for this actually run. Only `update` is
  // wired here (not the full six CRUD ops): this file is about update-action shapes, and
  // put/get/delete/query/scan don't exercise anything specific to them.
  val updateQuery: DynamoDBQuery[Item, Option[Item]] =
    update(items)(Item.id.partitionKey === "i-1")(combined)
}
