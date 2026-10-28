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

import zio.blocks.schema.{ CompanionOptics, Lens, Modifier, NameMapper, Schema }
import zio.blocks.schema.json.DiscriminatorKind
import zio.dynamodb.{ DynamoDBError, DynamoDBQuery, Page }
import zio.dynamodb.blocks.ddbexpr.dsl._
import zio.dynamodb.blocks.schema.DynamoDBCodecDeriver

/**
 * Two independent ways to configure derivation — value-level (derive the codec yourself and pass
 * it to `Table(name, codec)`) and annotation-level (`@Modifier` on the type) — and their precedence
 * when both apply to the same field: a per-field `@Modifier.rename` wins over both the type's own
 * class-level `@Modifier.fieldNaming` and a deriver-wide `withFieldNameMapper`.
 *
 * Deriver-wide settings go on `DynamoDBCodecDeriver`; per-field ones go on ZIO Blocks' derivation
 * builder, addressed by optic. Expressions on the table resolve against that same codec.
 */
object TableConfigs {

  // ── Value-level config, via Table(name, codec) ──────────────────────────────

  final case class Person(firstName: String, lastName: String)

  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived

    val firstName: Lens[Person, String] = $(_.firstName)
    val lastName: Lens[Person, String]  = $(_.lastName)
  }

  val defaultTable: Table[Person] = Table[Person]("people")

  val snakeCaseTable: Table[Person] =
    Table("people", Person.schema.deriving(DynamoDBCodecDeriver.withFieldNameMapper(NameMapper.SnakeCase)).derive)

  val renamedFieldTable: Table[Person] =
    Table(
      "people",
      Person.schema.deriving(DynamoDBCodecDeriver).modifier(Person.firstName, Modifier.rename("fname")).derive
    )

  // ── Annotation-level config, directly on the type ───────────────────────────

  @Modifier.fieldNaming("snake_case")
  final case class Address(streetName: String, @Modifier.rename("zip") zipCode: String)

  object Address extends CompanionOptics[Address] {
    implicit val schema: Schema[Address] = Schema.derived

    val streetName: Lens[Address, String] = $(_.streetName)
    val zipCode: Lens[Address, String]    = $(_.zipCode)
  }

  // streetName -> "street_name" (class-level @Modifier.fieldNaming); zipCode -> "zip"
  // (per-field @Modifier.rename wins over both the class-level annotation and the
  // deriver-wide withFieldNameMapper below).
  val addresses: Table[Address] =
    Table("addresses", Address.schema.deriving(DynamoDBCodecDeriver.withFieldNameMapper(NameMapper.CamelCase)).derive)

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
    Table(
      "items",
      Item.schema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.Field("type"))).derive
    )

  // Wired into the six CRUD operations, against renamedFieldTable (the optic-addressed rename)
  // — see TableConfigsSpec for these actually run.
  val putQuery: DynamoDBQuery[Person, Unit] =
    put(renamedFieldTable, Person("Ada", "Lovelace"))

  val getQuery: DynamoDBQuery[Person, Option[Either[DynamoDBError.ItemError.DecodingError, Person]]] =
    get(renamedFieldTable)(Person.firstName.partitionKey === "Ada")

  val updateQuery: DynamoDBQuery[Person, Unit] =
    update(renamedFieldTable)(Person.firstName.partitionKey === "Ada")(Person.lastName.set("King"))

  val deleteQuery: DynamoDBQuery[Person, Unit] =
    deleteFrom(renamedFieldTable)(Person.firstName.partitionKey === "Ada")

  val queryQuery: DynamoDBQuery[Person, Page[Either[DynamoDBError.ItemError, Person]]] =
    query(renamedFieldTable, limit = 20)
      .whereKey(Person.firstName.partitionKey === "Ada")
      .filter(Person.lastName.attributeExists)

  val scanQuery: DynamoDBQuery[Person, Page[Either[DynamoDBError.ItemError, Person]]] =
    scan(renamedFieldTable, limit = 20).filter(Person.firstName.attributeExists)
}
