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

import zio.test._

object AttributeValueSpec extends ZIOSpecDefault {

  def spec = suite("AttributeValue")(
    showTypeSuite,
    binarySuite,
    binarySetSuite,
    listSuite,
    numberSetSuite,
    stringSetSuite,
    mapSuite
  )

  private val showTypeSuite = suite("showType")(
    test("Binary") {
      assertTrue(AttributeValue.Binary(List(1.toByte)).showType == "AttributeValue.Binary")
    },
    test("BinarySet") {
      assertTrue(AttributeValue.BinarySet(List(List(1.toByte))).showType == "AttributeValue.BinarySet")
    },
    test("Bool") {
      assertTrue(AttributeValue.Bool(true).showType == "AttributeValue.Bool")
    },
    test("List") {
      assertTrue(AttributeValue.List(List.empty).showType == "AttributeValue.List")
    },
    test("Map") {
      assertTrue(AttributeValue.Map(Map.empty).showType == "AttributeValue.Map")
    },
    test("Number") {
      assertTrue(AttributeValue.Number(BigDecimal(1)).showType == "AttributeValue.Number")
    },
    test("NumberSet") {
      assertTrue(AttributeValue.NumberSet(Set(BigDecimal(1))).showType == "AttributeValue.NumberSet")
    },
    test("Null") {
      assertTrue(AttributeValue.Null.showType == "AttributeValue.Null")
    },
    test("String") {
      assertTrue(AttributeValue.String("x").showType == "AttributeValue.String")
    },
    test("StringSet") {
      assertTrue(AttributeValue.StringSet(Set("a")).showType == "AttributeValue.StringSet")
    }
  )

  private val binarySuite = suite("AttributeValue.Binary")(
    test("equal Binary values with the same bytes are equal") {
      assertTrue(AttributeValue.Binary(Array[Byte](1, 2, 3)) == AttributeValue.Binary(Array[Byte](1, 2, 3)))
    },
    test("Binary is not equal to a non-Binary value") {
      assertTrue(AttributeValue.Binary(Array[Byte](1, 2, 3)) != AttributeValue.String("nope"))
    },
    test("hashCode is consistent with equals") {
      assertTrue(
        AttributeValue.Binary(Array[Byte](1, 2, 3)).hashCode == AttributeValue.Binary(Array[Byte](1, 2, 3)).hashCode
      )
    },
    test("toListOfNumbers produces the schema1 List-of-Number representation") {
      val list = AttributeValue.Binary.toListOfNumbers(Array[Byte](1, 2, 3))
      assertTrue(
        list.value.toList == List(
          AttributeValue.Number(BigDecimal(1)),
          AttributeValue.Number(BigDecimal(2)),
          AttributeValue.Number(BigDecimal(3))
        )
      )
    }
  )

  // Repro for docs2/batch_write_model_correction.md §3: BinarySet.value is a plain
  // Iterable[Iterable[Byte]], so its case-class-derived equals delegates to whatever concrete
  // collection each side happens to be. Local construction (ToAttributeValue.scala:54-55) goes
  // through a generic Col1[Col2[B]] <: Iterable — typically a Set at the outer level — while
  // the real AWS decode path (AwsDynamoDB.scala:163-168) produces `av.bs.asScala.map(...)`, a
  // mutable.Buffer (a Seq) at the outer level. Scala's Set and Seq families never canEqual each
  // other, regardless of content, so the same logical binary set compares unequal depending on
  // which path constructed it.
  private val binarySetSuite = suite("AttributeValue.BinarySet")(
    test("root cause: a Set-shaped and a Seq-shaped BinarySet with identical content are equal") {
      val setShaped: AttributeValue.BinarySet = AttributeValue.BinarySet(Set(List(1.toByte, 2.toByte, 3.toByte)))
      val seqShaped: AttributeValue.BinarySet = AttributeValue.BinarySet(List(List(1.toByte, 2.toByte, 3.toByte)))
      assertTrue(setShaped == seqShaped)
    },
    test("locally-constructed and AWS-decoded shapes of the same binary set are equal") {
      import scala.collection.immutable.ArraySeq
      import scala.collection.mutable

      // Mirrors ToAttributeValue.binarySetToAttributeValue's Col1[Col2[B]] <: Iterable shape
      // for a user constructing an item locally, e.g. Item("photos" -> Set(List[Byte](1,2,3))).
      val locallyConstructed: AttributeValue.BinarySet =
        AttributeValue.BinarySet(Set(List(1.toByte, 2.toByte, 3.toByte)))

      // Mirrors AwsCodecs.fromAwsAttrValue's BinarySet branch exactly: av.bs.asScala.map(b =>
      // ArraySeq.unsafeWrapArray(b.asByteArray)) — asScala on a java.util.List yields a
      // mutable.Buffer.
      val awsDecoded: AttributeValue.BinarySet =
        AttributeValue.BinarySet(mutable.Buffer(ArraySeq.unsafeWrapArray(Array[Byte](1, 2, 3))))

      assertTrue(locallyConstructed == awsDecoded)
    }
  )

  private val listSuite = suite("AttributeValue.List")(
    test("empty creates empty list") {
      assertTrue(AttributeValue.List.empty == AttributeValue.List(Iterable.empty))
    },
    test("+ appends a value") {
      val list    = AttributeValue.List(List(AttributeValue.String("a")))
      val updated = list + AttributeValue.String("b")
      assertTrue(updated.value.toList == List(AttributeValue.String("a"), AttributeValue.String("b")))
    }
  )

  private val numberSetSuite = suite("AttributeValue.NumberSet")(
    test("empty creates empty number set") {
      assertTrue(AttributeValue.NumberSet.empty == AttributeValue.NumberSet(Set.empty))
    },
    test("+ with valid number string adds to set") {
      val ns     = AttributeValue.NumberSet(Set(BigDecimal(1)))
      val result = ns + "2"
      assertTrue(result == Right(AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2)))))
    },
    test("+ with invalid string returns Left") {
      val ns     = AttributeValue.NumberSet.empty
      val result = ns + "not-a-number"
      assertTrue(result.isLeft)
    }
  )

  private val stringSetSuite = suite("AttributeValue.StringSet")(
    test("empty creates empty string set") {
      assertTrue(AttributeValue.StringSet.empty == AttributeValue.StringSet(Set.empty))
    },
    test("+ adds a string to the set") {
      val ss      = AttributeValue.StringSet(Set("a"))
      val updated = ss + "b"
      assertTrue(updated.value == Set("a", "b"))
    }
  )

  private val mapSuite = suite("AttributeValue.Map")(
    test("+ adds a key-value pair") {
      val m       = AttributeValue.Map(Map.empty)
      val updated = m + ("key" -> AttributeValue.String("value"))
      assertTrue(updated.value.get(AttributeValue.String("key")).contains(AttributeValue.String("value")))
    }
  )
}
