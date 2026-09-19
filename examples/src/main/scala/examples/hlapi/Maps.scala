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
 * `Map[String, V]` fields via `.atKey(k)`, for a scalar, a nested record, and a nested map
 * value. DynamoDB map keys are always strings, so `atKey` just needs the key literal. A
 * non-`String`-keyed `Map` compiles but fails when the query runs — see
 * `CompileTimeRejections.scala`.
 */
object Maps {

  final case class Note(text: String, priority: Int)

  object Note extends CompanionOptics[Note] {
    implicit val schema: Schema[Note] = Schema.derived

    val text: Lens[Note, String]  = $(_.text)
    val priority: Lens[Note, Int] = $(_.priority)
  }

  final case class Directory(
    id: String,
    attrs: Map[String, String],
    counts: Map[String, Int],
    notes: Map[String, Note],
    deep: Map[String, Map[String, Int]]
  )

  object Directory extends CompanionOptics[Directory] {
    implicit val schema: Schema[Directory] = Schema.derived

    val id: Lens[Directory, String]                          = $(_.id)
    val attrs: Lens[Directory, Map[String, String]]          = $(_.attrs)
    val counts: Lens[Directory, Map[String, Int]]            = $(_.counts)
    val notes: Lens[Directory, Map[String, Note]]            = $(_.notes)
    val deep: Lens[Directory, Map[String, Map[String, Int]]] = $(_.deep)

    def attrAt(k: String): Optional[Directory, String]           = $(_.attrs.atKey(k))
    def countAt(k: String): Optional[Directory, Int]             = $(_.counts.atKey(k))
    def notePriorityAt(k: String): Optional[Directory, Int]      = $(_.notes.atKey(k).priority)
    def deepAt(k1: String, k2: String): Optional[Directory, Int] = $(_.deep.atKey(k1).atKey(k2))
  }

  val directories: Table[Directory] = Table[Directory]("directories")

  // Map[String, String] — scalar value
  val attrExists: DdbExpr[Directory, Boolean] = Directory.attrAt("color").attributeExists
  val setAttr: DdbUpdateExpr[Directory]       = Directory.attrAt("color").set("blue")

  // Map[String, Int] — scalar value, numeric update
  val countGt: DdbExpr[Directory, Boolean]     = Directory.countAt("views") > 0
  val incrementCount: DdbUpdateExpr[Directory] = Directory.countAt("views").increment(1)

  // Map[String, Note] — record value, project into a field of it
  val notePriorityGt: DdbExpr[Directory, Boolean] = Directory.notePriorityAt("alice") > 0
  val setNotePriority: DdbUpdateExpr[Directory]   = Directory.notePriorityAt("alice").set(5)

  // Map[String, Map[String, Int]] — two-level nested map
  val deepExists: DdbExpr[Directory, Boolean] = Directory.deepAt("2024", "q1").attributeExists
  val setDeep: DdbUpdateExpr[Directory]       = Directory.deepAt("2024", "q1").set(100)
}
