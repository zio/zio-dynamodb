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

package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, DynamicOptic, Lens, Schema }
import zio.test._
import zio.test.Assertion.{ equalTo, hasField, isRight }

object OpticToPESpec extends ZIOSpecDefault {

  private case class Widget(id: String, tags: List[String])
  private object Widget extends CompanionOptics[Widget] {
    implicit val schema: Schema[Widget]  = Schema.derived
    val id: Lens[Widget, String]         = $(_.id)
    val tags: Lens[Widget, List[String]] = $(_.tags)
  }

  def spec = suite("OpticToPE")(
    suite("pe(DynamicOptic)")(
      test("a path of Field nodes converts to nested MapElements") {
        val dyn = DynamicOptic(IndexedSeq(DynamicOptic.Node.Field("a"), DynamicOptic.Node.Field("b")))
        assert(OpticToPE.pe(dyn))(isRight(hasField("toString", _.toString, equalTo("a.b"))))
      },
      test("a non-Field node produces a Left") {
        val dyn = DynamicOptic(IndexedSeq(DynamicOptic.Node.AtIndex(0)))
        assertTrue(OpticToPE.pe(dyn).isLeft)
      }
    ),
    suite("pe(optic: Optic[S, A])")(
      test("dispatches Lens to the Lens overload") {
        assertTrue(OpticToPE.pe(Widget.id: zio.blocks.schema.Optic[Widget, String]).map(_.toString) == Right("id"))
      },
      test("a Traversal (neither Lens nor Optional) produces a Left") {
        val traversal = Widget.tags.listValues
        assertTrue(OpticToPE.pe(traversal: zio.blocks.schema.Optic[Widget, String]).isLeft)
      }
    ),
    test("pe(lens) converts a multi-segment Lens path to nested MapElements") {
      assertTrue(OpticToPE.pe(Widget.id).map(_.toString) == Right("id"))
    }
  )
}
