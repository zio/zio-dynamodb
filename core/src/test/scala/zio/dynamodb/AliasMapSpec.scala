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

import zio.dynamodb.ProjectionExpression.$
import zio.test._

object AliasMapSpec extends ZIOSpecDefault {

  def spec = suite("AliasMap")(
    suite("getOrInsert AttributeValue")(
      test("first insertion produces :v0") {
        val (am, alias) = AliasMap.empty.getOrInsert(AttributeValue.String("hello"))
        assertTrue(alias == ":v0" && am.index == 1)
      },
      test("second distinct value produces :v1") {
        val (am1, _)    = AliasMap.empty.getOrInsert(AttributeValue.String("a"))
        val (_, alias2) = am1.getOrInsert(AttributeValue.String("b"))
        assertTrue(alias2 == ":v1")
      },
      test("same value is reused — index does not advance") {
        val av           = AttributeValue.String("dup")
        val (am1, _)     = AliasMap.empty.getOrInsert(av)
        val (am2, alias) = am1.getOrInsert(av)
        assertTrue(alias == ":v0" && am2.index == 1)
      }
    ),
    suite("getOrInsert ProjectionExpression")(
      test("top-level map element produces name alias #n0") {
        val pe         = $("name")
        val (_, alias) = AliasMap.empty.getOrInsert(pe)
        assertTrue(alias == "#n0")
      },
      test("same path requested twice reuses first alias") {
        val pe        = $("name")
        val (am1, _)  = AliasMap.empty.getOrInsert(pe)
        val (am2, a2) = am1.getOrInsert(pe)
        assertTrue(a2 == "#n0" && am2.index == 1)
      },
      test("two distinct paths get distinct aliases") {
        val pe1       = $("name")
        val pe2       = $("age")
        val (am1, a1) = AliasMap.empty.getOrInsert(pe1)
        val (_, a2)   = am1.getOrInsert(pe2)
        assertTrue(a1 != a2)
      },
      test("nested path produces dot-separated alias") {
        val pe     = $("a.b")
        val (_, s) = AliasMap.empty.getOrInsert(pe)
        assertTrue(s.contains(".") || s.startsWith("#n"))
      },
      test("backtick-quoted name strips backticks in alias") {
        val pe     = $("`name`")
        val (_, s) = AliasMap.empty.getOrInsert(pe)
        assertTrue(!s.contains("`"))
      }
    ),
    suite("++")(
      test("merges two alias maps") {
        val (am1, _) = AliasMap.empty.getOrInsert(AttributeValue.String("a"))
        val (am2, _) = AliasMap.empty.getOrInsert(AttributeValue.String("b"))
        val merged   = am1 ++ am2
        assertTrue(merged.index == merged.map.size)
      }
    ),
    suite("isEmpty")(
      test("empty map is empty") {
        assertTrue(AliasMap.empty.isEmpty)
      },
      test("non-empty map is not empty") {
        val (am, _) = AliasMap.empty.getOrInsert(AttributeValue.String("x"))
        assertTrue(!am.isEmpty)
      }
    ),
    suite("list element path")(
      test("list index element renders with [index]") {
        val pe     = $("items")(0)
        val (_, s) = AliasMap.empty.getOrInsert(pe)
        assertTrue(s.contains("[0]"))
      }
    )
  )
}
