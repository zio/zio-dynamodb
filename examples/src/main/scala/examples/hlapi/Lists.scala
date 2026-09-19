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

import zio.blocks.schema.{ CompanionOptics, Lens, Optional, Schema }
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbUpdateExpr }
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * List-typed attributes (`List`/`Vector`/`Array`/`Chunk`, of scalars, records, or a
 * sealed-trait's cases): indexed element access, `appendList`/`prependList`, and two ways to
 * remove element `i` — `.at(i).remove` or `.remove(i)` directly on the list field. Note that
 * `containsElement` only works on a set, not a list — see `CompileTimeRejections.scala`.
 */
object Lists {

  final case class LineItem(sku: String, qty: Int)

  object LineItem extends CompanionOptics[LineItem] {
    implicit val schema: Schema[LineItem] = Schema.derived

    val sku: Lens[LineItem, String] = $(_.sku)
    val qty: Lens[LineItem, Int]    = $(_.qty)
  }

  sealed trait Shape
  object Shape {
    final case class Circle(radius: Double) extends Shape
    final case class Square(side: Double)   extends Shape
    implicit val schema: Schema[Shape] = Schema.derived
  }

  final case class Catalog(
    id: String,
    names: List[String],
    vec: Vector[Int],
    items: List[LineItem],
    shapes: List[Shape]
  )

  object Catalog extends CompanionOptics[Catalog] {
    implicit val schema: Schema[Catalog] = Schema.derived

    val id: Lens[Catalog, String]            = $(_.id)
    val names: Lens[Catalog, List[String]]   = $(_.names)
    val vec: Lens[Catalog, Vector[Int]]      = $(_.vec)
    val items: Lens[Catalog, List[LineItem]] = $(_.items)
    val shapes: Lens[Catalog, List[Shape]]   = $(_.shapes)

    def nameAt(i: Int): Optional[Catalog, String]         = $(_.names.at(i))
    def vecAt(i: Int): Optional[Catalog, Int]             = $(_.vec.at(i))
    def itemQtyAt(i: Int): Optional[Catalog, Int]         = $(_.items.at(i).qty)
    def circleRadiusAt(i: Int): Optional[Catalog, Double] = $(_.shapes.at(i).when[Shape.Circle].radius)
  }

  val catalogs: Table[Catalog] = Table[Catalog]("catalogs")

  // List[String] (scalar elements)
  val nameExists: DdbExpr[Catalog, Boolean]      = Catalog.nameAt(0).attributeExists
  val setName: DdbUpdateExpr[Catalog]            = Catalog.nameAt(0).set("renamed")
  val appendNames: DdbUpdateExpr[Catalog]        = Catalog.names.appendList(Seq("x", "y"))
  val prependNames: DdbUpdateExpr[Catalog]       = Catalog.names.prependList(Seq("x"))
  val removeNameViaAt: DdbUpdateExpr[Catalog]    = Catalog.nameAt(2).remove // Remove, via an indexed Optional
  val removeNameViaIndex: DdbUpdateExpr[Catalog] = Catalog.names.remove(2)  // RemoveAt, Allows[L] on the list itself

  // Vector[Int] — same L bucket, different concrete collection
  val vecGt: DdbExpr[Catalog, Boolean] = Catalog.vecAt(0) > 0

  // List[LineItem] (record elements) — index into the list, then project a field
  val itemQtyGt: DdbExpr[Catalog, Boolean]     = Catalog.itemQtyAt(0) > 0
  val incrementItemQty: DdbUpdateExpr[Catalog] = Catalog.itemQtyAt(0).increment(1)

  // List[Shape] (sealed-trait elements) — index into the list, then narrow to a case
  val circleAt0Exists: DdbExpr[Catalog, Boolean] = Catalog.circleRadiusAt(0).attributeExists
}
