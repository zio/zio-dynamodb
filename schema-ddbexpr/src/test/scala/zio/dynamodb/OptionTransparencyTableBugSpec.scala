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

package zio.dynamodb

import zio.blocks.schema.{ CompanionOptics, Lens, Optional, Schema }
import zio.dynamodb.blocks.ddbexpr.dsl._
import zio.test._

/**
 * Regression coverage, through the fully public `Table`/`dsl` API (no `ddbexpr`-package
 * access, no bypass of any kind), for a path that reaches into an `Option[List]` /
 * `Option[Map]` field's inner structure (`.when[Some[...]].value.at(i)` /
 * `.value.atKey(key)`). `ResolverDeriver` previously had no handling for `Reflect.Optional`
 * at all, so a field's resolver was built as a real two-case `Resolver.Variant` — but the
 * optic path already has its `Case(Some)+Field(value)` segment stripped before resolution
 * (`OpticToPE.pruneOptionalNodes`, reused by `ProjectionResolver.resolve`), so the walk had
 * nothing to match once it reached that field. Fixed by deriving `Option[X]`'s resolver as a
 * transparent `Resolver.Wrapper` (mirroring `DynamoDBCodecDeriver`'s own `typeId.isOption`
 * branch), so `ProjectionResolver`'s existing wrapper pass-through skips it the same way it
 * already skips an opaque/newtype wrapper.
 */
object OptionTransparencyTableBugSpec extends ZIOSpecDefault {

  case class Profile(name: String, tags: Option[List[String]], meta: Option[Map[String, Int]])
  object Profile extends CompanionOptics[Profile] {
    implicit val schema: Schema[Profile]            = Schema.derived
    val name: Lens[Profile, String]                 = $(_.name)
    def tagAt(i: Int): Optional[Profile, String]    = $(_.tags.when[Some[List[String]]].value.at(i))
    def metaAt(key: String): Optional[Profile, Int] = $(_.meta.when[Some[Map[String, Int]]].value.atKey(key))
  }

  private val profiles = Table[Profile]("profiles")

  private def run[A](q: DynamoDBQuery[_, A]): A = DummyIOInterpreter.run(q).unsafeRun()

  def spec = suite("Option[A] transparency through a real Table (public API only)")(
    test("update on an Option[List] element resolves") {
      val q = update(profiles)(Profile.name.partitionKey === "n")(Profile.tagAt(3).remove)
      assertTrue(run(q.toQuery).isEmpty)
    },
    test("update on an Option[Map] key resolves") {
      val q = update(profiles)(Profile.name.partitionKey === "n")(Profile.metaAt("views").set(42))
      assertTrue(run(q.toQuery).isEmpty)
    },
    test("filter on an Option[List] element resolves") {
      val page = run(scan(profiles, 20).filter(Profile.tagAt(0).attributeExists).toQuery)
      assertTrue(page.items.isEmpty)
    }
  )
}
