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

import zio.blocks.schema.{ CompanionOptics, Lens, Modifier, NameMapper, Schema }
import zio.blocks.schema.json.DiscriminatorKind
import zio.dynamodb.{ DynamoDBError, DynamoDBQuery, Page }
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * Two independent ways to configure derivation — value-level (`Table[A].deriving(cfg => ...)`)
 * and annotation-level (`@Modifier` on the type) — and their precedence when both apply to the
 * same field: a per-field `@Modifier.rename` wins over both the type's own class-level
 * `@Modifier.fieldNaming` and any `Table`-level `withFieldNameMapper`.
 */
object TableConfigs {

  // ── Value-level config, via Table.deriving ──────────────────────────────────

  final case class Person(firstName: String, lastName: String)

  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived

    val firstName: Lens[Person, String] = $(_.firstName)
    val lastName: Lens[Person, String]  = $(_.lastName)
  }

  val defaultTable: Table[Person] = Table[Person]("people")

  val snakeCaseTable: Table[Person] =
    Table[Person]("people").deriving(_.withFieldNameMapper(NameMapper.SnakeCase))

  val renamedFieldTable: Table[Person] =
    Table[Person]("people").deriving { cfg =>
      val personType = Person.schema.reflect.typeId
      cfg.withModifier(personType, "firstName", Modifier.rename("fname"))
    }

  // ── Annotation-level config, directly on the type ───────────────────────────

  @Modifier.fieldNaming("snake_case")
  final case class Address(streetName: String, @Modifier.rename("zip") zipCode: String)

  object Address extends CompanionOptics[Address] {
    implicit val schema: Schema[Address] = Schema.derived

    val streetName: Lens[Address, String] = $(_.streetName)
    val zipCode: Lens[Address, String]    = $(_.zipCode)
  }

  // streetName -> "street_name" (class-level @Modifier.fieldNaming); zipCode -> "zip"
  // (per-field @Modifier.rename wins over both the class-level annotation and this
  // Table's own withFieldNameMapper below).
  val addresses: Table[Address] =
    Table[Address]("addresses").deriving(_.withFieldNameMapper(NameMapper.CamelCase))

  // ── Discriminator kind, value-level, on a sealed trait ──────────────────────

  sealed trait Shape
  object Shape {
    final case class Circle(radius: Double) extends Shape
    final case class Square(side: Double)   extends Shape
    implicit val schema: Schema[Shape] = Schema.derived
  }

  final case class Item(id: String, shape: Shape)

  object Item extends CompanionOptics[Item] {
    implicit val schema: Schema[Item] = Schema.derived

    val id: Lens[Item, String]   = $(_.id)
    val shape: Lens[Item, Shape] = $(_.shape)
  }

  val itemsKeyDiscriminated: Table[Item] = Table[Item]("items") // default: DiscriminatorKind.Key

  val itemsFieldDiscriminated: Table[Item] =
    Table[Item]("items").deriving(_.withDiscriminatorKind(DiscriminatorKind.Field("type")))

  // Wired into the six CRUD operations, against renamedFieldTable (the withModifier config)
  // — see TableConfigsSpec for these actually run.
  val putQuery: DynamoDBQuery[Person, Option[Person]] =
    put(renamedFieldTable, Person("Ada", "Lovelace"))

  val getQuery: DynamoDBQuery[Person, Either[DynamoDBError.ItemError, Person]] =
    get(renamedFieldTable)(Person.firstName.partitionKey === "Ada")

  val updateQuery: DynamoDBQuery[Person, Option[Person]] =
    update(renamedFieldTable)(Person.firstName.partitionKey === "Ada")(Person.lastName.set("King"))

  val deleteQuery: DynamoDBQuery[Person, Option[Person]] =
    deleteFrom(renamedFieldTable)(Person.firstName.partitionKey === "Ada")

  val queryQuery: DynamoDBQuery[Person, Page[Either[DynamoDBError.ItemError, Person]]] =
    query(renamedFieldTable, limit = 20)
      .whereKey(Person.firstName.partitionKey === "Ada")
      .filter(Person.lastName.attributeExists)

  val scanQuery: DynamoDBQuery[Person, Page[Either[DynamoDBError.ItemError, Person]]] =
    scan(renamedFieldTable, limit = 20).filter(Person.firstName.attributeExists)
}
