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
import zio.dynamodb.{ DynamoDBError, DynamoDBQuery, Page }
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbUpdateExpr }
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * `Option[A]` fields: a top-level `Option[Int]` needs no narrowing (`attributeExists`/`set`
 * work on the optic as-is), but reaching into the contents of an `Option[List]`,
 * `Option[Map]`, `Option[Record]`, or `Option[Variant]` needs `.when[Some[_]].value...` to
 * narrow past `None`/`Some` first.
 */
object Options {

  final case class Address(city: String)

  object Address extends CompanionOptics[Address] {
    implicit val schema: Schema[Address] = Schema.derived

    val city: Lens[Address, String] = $(_.city)
  }

  sealed trait Status
  object Status {
    case object Active   extends Status
    case object Inactive extends Status
    implicit val schema: Schema[Status] = Schema.derived
  }

  final case class Profile(
    name: String,
    score: Option[Int],
    tags: Option[List[String]],
    meta: Option[Map[String, Int]],
    addr: Option[Address],
    status: Option[Status]
  )

  object Profile extends CompanionOptics[Profile] {
    implicit val schema: Schema[Profile] = Schema.derived

    val name: Lens[Profile, String]            = $(_.name)
    val score: Lens[Profile, Option[Int]]      = $(_.score)
    val addrCity: Optional[Profile, String]    = $(_.addr.when[Some[Address]].value.city)
    val statusValue: Optional[Profile, Status] = $(_.status.when[Some[Status]].value)

    def tagAt(i: Int): Optional[Profile, String]     = $(_.tags.when[Some[List[String]]].value.at(i))
    def metaAtKey(k: String): Optional[Profile, Int] = $(_.meta.when[Some[Map[String, Int]]].value.atKey(k))
  }

  val profiles: Table[Profile] = Table[Profile]("profiles")

  // Option[Int], top-level — no narrowing needed
  val hasScore: DdbExpr[Profile, Boolean] = Profile.score.attributeExists
  val setScore: DdbUpdateExpr[Profile]    = Profile.score.set(Some(42))

  // Option[List[String]] — narrow past Some, then index into the list
  val tagExists: DdbExpr[Profile, Boolean] = Profile.tagAt(0).attributeExists
  val removeTag: DdbUpdateExpr[Profile]    = Profile.tagAt(3).remove

  // Option[Map[String, Int]] — narrow past Some, then key into the map
  val metaExists: DdbExpr[Profile, Boolean] = Profile.metaAtKey("views").attributeExists
  val setMeta: DdbUpdateExpr[Profile]       = Profile.metaAtKey("views").set(42)

  // Option[Address] — narrow past Some, then project a field of the wrapped record
  val cityEq: DdbExpr[Profile, Boolean] = Profile.addrCity === "London"
  val setCity: DdbUpdateExpr[Profile]   = Profile.addrCity.set("Paris")

  // Option[Status] — narrow past Some to the wrapped variant value itself
  val statusIsActive: DdbExpr[Profile, Boolean] = Profile.statusValue === Status.Active

  // Wired into the six CRUD operations — see OptionsSpec for these actually run.
  val putQuery: DynamoDBQuery[Profile, Option[Profile]] =
    put(profiles, Profile("alice", Some(1), Some(List("x")), Some(Map("views" -> 1)), None, Some(Status.Active)))

  val getQuery: DynamoDBQuery[Profile, Either[DynamoDBError.ItemError, Profile]] =
    get(profiles)(Profile.name.partitionKey === "alice")

  val updateQuery: DynamoDBQuery[Profile, Option[Profile]] =
    update(profiles)(Profile.name.partitionKey === "alice")(setScore)

  val deleteQuery: DynamoDBQuery[Profile, Option[Profile]] =
    deleteFrom(profiles)(Profile.name.partitionKey === "alice")

  val queryQuery: DynamoDBQuery[Profile, Page[Either[DynamoDBError.ItemError, Profile]]] =
    query(profiles, limit = 20).whereKey(Profile.name.partitionKey === "alice").filter(hasScore)

  val scanQuery: DynamoDBQuery[Profile, Page[Either[DynamoDBError.ItemError, Profile]]] =
    scan(profiles, limit = 20).filter(tagExists)
}
