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

package examples.hlapi

import zio.blocks.chunk.Chunk
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{ DynamoDBError, DynamoDBQuery, Page }
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbUpdateExpr }
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * DynamoDB's native number/string/binary set types: `containsElement` checks whether a set
 * attribute contains a value, `addSet`/`deleteFromSet` add or remove elements. Not to be
 * confused with `in`/`inSet` (`Scalars.scala`), which test a scalar field against a set of
 * candidate values, not a set-typed attribute.
 */
object Sets {

  final case class Inventory(id: String, tags: Set[String], bins: Set[Int], checksums: Set[Chunk[Byte]])

  object Inventory extends CompanionOptics[Inventory] {
    implicit val schema: Schema[Inventory] = Schema.derived

    val id: Lens[Inventory, String]                  = $(_.id)
    val tags: Lens[Inventory, Set[String]]           = $(_.tags)
    val bins: Lens[Inventory, Set[Int]]              = $(_.bins)
    val checksums: Lens[Inventory, Set[Chunk[Byte]]] = $(_.checksums)
  }

  val inventories: Table[Inventory] = Table[Inventory]("inventories")

  // SS
  val hasTag: DdbExpr[Inventory, Boolean]  = Inventory.tags.containsElement("blue")
  val addTags: DdbUpdateExpr[Inventory]    = Inventory.tags.addSet(Set("blue", "green"))
  val removeTags: DdbUpdateExpr[Inventory] = Inventory.tags.deleteFromSet(Set("blue"))

  // NS
  val hasBin: DdbExpr[Inventory, Boolean]  = Inventory.bins.containsElement(7)
  val addBins: DdbUpdateExpr[Inventory]    = Inventory.bins.addSet(Set(7, 8))
  val removeBins: DdbUpdateExpr[Inventory] = Inventory.bins.deleteFromSet(Set(7))

  // BS
  val hasChecksum: DdbExpr[Inventory, Boolean]  = Inventory.checksums.containsElement(Chunk[Byte](1, 2, 3))
  val addChecksums: DdbUpdateExpr[Inventory]    = Inventory.checksums.addSet(Set(Chunk[Byte](1, 2, 3)))
  val removeChecksums: DdbUpdateExpr[Inventory] = Inventory.checksums.deleteFromSet(Set(Chunk[Byte](1, 2, 3)))

  // Wired into the six CRUD operations — see SetsSpec for these actually run.
  val putQuery: DynamoDBQuery[Inventory, Option[Inventory]] =
    put(inventories, Inventory("i-1", Set("urgent"), Set(1, 2), Set(Chunk[Byte](1))))

  val getQuery: DynamoDBQuery[Inventory, Either[DynamoDBError.ItemError, Inventory]] =
    get(inventories)(Inventory.id.partitionKey === "i-1")

  val updateQuery: DynamoDBQuery[Inventory, Option[Inventory]] =
    update(inventories)(Inventory.id.partitionKey === "i-1")(Inventory.tags.addSet(Set("reviewed")))

  val deleteQuery: DynamoDBQuery[Inventory, Option[Inventory]] =
    deleteFrom(inventories)(Inventory.id.partitionKey === "i-1")

  val queryQuery: DynamoDBQuery[Inventory, Page[Either[DynamoDBError.ItemError, Inventory]]] =
    query(inventories, limit = 20).whereKey(Inventory.id.partitionKey === "i-1").filter(hasTag)

  val scanQuery: DynamoDBQuery[Inventory, Page[Either[DynamoDBError.ItemError, Inventory]]] =
    scan(inventories, limit = 20).filter(hasBin)
}
