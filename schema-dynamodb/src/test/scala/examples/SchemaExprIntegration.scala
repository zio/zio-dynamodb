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

package examples

import zio.blocks.schema.{ CompanionOptics, Lens, Optic, Schema, SchemaExpr }
import zio.dynamodb.ProjectionExpression
import zio.dynamodb.blocks.OpticToPE

object SchemaExprIntegration {
  sealed trait Gender
  object Gender {
    case object Male   extends Gender
    case object Female extends Gender
  }
  final case class Person(id: String, year: String, age: Int, gender: Gender)
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived
    val id: Lens[Person, String]        = $(_.id)
    val age: Lens[Person, Int]          = $(_.age)
    val year: Lens[Person, String]      = $(_.year)
    val gender: Lens[Person, Gender]    = $(_.gender)
  }

  implicit class SchemaExprOps[S, A](val left: Optic[S, A]) extends AnyVal {
    def ===[B](value: B) /*(implicit ev: A <:< B)*/: ProjectionExpression[S, A]  = ???
    def ====[B](value: B) /*(implicit ev: A <:< B)*/: ProjectionExpression[S, A] = ???
  }

  val x: Lens[Person, String]                    = Person.id
  val expr2: SchemaExpr[Person, Boolean]         = Person.id === "1"
  val y: ProjectionExpression[Person, String]    =
    OpticToPE.pe(Person.id).fold(msg => throw new IllegalArgumentException(msg), identity)
  val expr: ProjectionExpression[Person, String] = Person.id ==== "1"
}
