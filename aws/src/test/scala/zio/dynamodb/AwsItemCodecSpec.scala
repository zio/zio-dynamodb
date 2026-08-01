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

import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue => AwsAttrValue }
import zio.test._

import java.util.{ HashMap => JHashMap }

/**
 * Verifies AwsCodecs.toAwsItem/fromAwsItem — the item-level conversion between
 *  this library's Item (AttrMap) and the AWS SDK's raw attribute value map.
 *  Neither direction had a direct unit test; only exercised transitively via
 *  the to*Request/from*Response builders.
 */
object AwsItemCodecSpec extends ZIOSpecDefault {

  def spec = suite("AwsCodecs item conversion")(
    itemConversionSuite,
    getItemProjectionsSuite
  )

  private val itemConversionSuite = suite("toAwsItem/fromAwsItem")(
    test("toAwsItem converts a multi-type Item to the equivalent AWS SDK map") {
      val item = Item("id" -> "a", "count" -> 1, "active" -> true)
      val aws  = AwsCodecs.toAwsItem(item)
      assertTrue(
        aws.get("id") == AwsAttrValue.builder().s("a").build(),
        aws.get("count") == AwsAttrValue.builder().n("1").build(),
        aws.get("active") == AwsAttrValue.builder().bool(true).build()
      )
    },
    test("fromAwsItem converts an AWS SDK map back to an Item") {
      val jm   = new JHashMap[String, AwsAttrValue]()
      jm.put("id", AwsAttrValue.builder().s("a").build())
      jm.put("count", AwsAttrValue.builder().n("1").build())
      val item = AwsCodecs.fromAwsItem(jm)
      assertTrue(
        item.get[String]("id") == Right("a"),
        item.get[Int]("count") == Right(1)
      )
    },
    test("toAwsItem andThen fromAwsItem round-trips an Item") {
      // compared per-field via typed accessors rather than raw AttrMap equality:
      // List's underlying collection representation differs from the ArrayBuffer
      // produced by decoding, which is a known equality quirk, not a real bug.
      val item         = Item("id" -> "a", "tags" -> List("x", "y"), "score" -> 3.5)
      val roundTripped = AwsCodecs.fromAwsItem(AwsCodecs.toAwsItem(item))
      val tags         = roundTripped.map.get("tags") match {
        case Some(AttributeValue.List(values)) => values.collect { case AttributeValue.String(s) => s }.toList
        case _                                 => Nil
      }
      assertTrue(
        roundTripped.get[String]("id") == Right("a"),
        tags == List("x", "y"),
        roundTripped.get[Double]("score") == Right(3.5)
      )
    },
    test("fromAwsItem on an empty map produces an empty Item") {
      assertTrue(AwsCodecs.fromAwsItem(new JHashMap[String, AwsAttrValue]()) == Item(Map.empty[String, AttributeValue]))
    }
  )

  private val getItemProjectionsSuite = suite("toGetItemRequest with projections")(
    test("non-empty projections set both projectionExpression and expressionAttributeNames") {
      val q   = DynamoDBQuery.GetItem("t", PrimaryKey("id" -> "a"), projections = List(ProjectionExpression.$("name")))
      val req = AwsCodecs.toGetItemRequest(q)
      assertTrue(
        req.projectionExpression() != null,
        req.projectionExpression().nonEmpty,
        !req.expressionAttributeNames().isEmpty
      )
    },
    test("no projections leaves projectionExpression and expressionAttributeNames unset") {
      val q   = DynamoDBQuery.GetItem("t", PrimaryKey("id" -> "a"))
      val req = AwsCodecs.toGetItemRequest(q)
      assertTrue(req.projectionExpression() == null, req.expressionAttributeNames().isEmpty)
    }
  )
}
