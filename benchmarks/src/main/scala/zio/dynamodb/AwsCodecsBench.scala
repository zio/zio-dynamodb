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

import org.openjdk.jmh.annotations._
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue => AwsAttrValue,
  GetItemRequest,
  GetItemResponse,
  PutItemRequest,
  PutItemResponse
}

import scala.collection.JavaConverters._

// Measures AwsCodecs encode/decode in isolation: no effect runtime, no network.
// Each benchmark method processes a list of `size` items, matching the approach
// from zio-dynamodb's CodecBenchmarks so sizes are directly comparable.
class AwsCodecsBench extends BaseBenchmark {

  @Param(Array("1", "10", "100", "1000", "10000", "100000"))
  var size: Int = _

  var getItemQueries: List[DynamoDBQuery.GetItem] = _
  var putItemQueries: List[DynamoDBQuery.PutItem] = _
  var getItemResponses: List[GetItemResponse]     = _
  var putItemResponses: List[PutItemResponse]     = _

  @Setup
  def setup(): Unit = {
    getItemQueries = (1 to size).map { i =>
      DynamoDBQuery.GetItem("Users", PrimaryKey("id" -> s"user-$i"))
    }.toList

    putItemQueries = (1 to size).map { i =>
      DynamoDBQuery.PutItem(
        "Users",
        Item(
          "id"     -> s"user-$i",
          "name"   -> s"name-$i",
          "score"  -> i,
          "active" -> (i % 2 == 0)
        )
      )
    }.toList

    getItemResponses = (1 to size).map { i =>
      val awsItem = Map(
        "id"     -> AwsAttrValue.builder().s(s"user-$i").build(),
        "name"   -> AwsAttrValue.builder().s(s"name-$i").build(),
        "score"  -> AwsAttrValue.builder().n(i.toString).build(),
        "active" -> AwsAttrValue.builder().bool(i % 2 == 0).build()
      ).asJava
      GetItemResponse.builder().item(awsItem).build()
    }.toList

    putItemResponses = (1 to size).map { _ =>
      PutItemResponse.builder().build()
    }.toList
  }

  // GetItem encode: domain query → AWS SDK GetItemRequest
  @Benchmark def encodingGetItem: List[GetItemRequest] =
    getItemQueries.map(AwsCodecs.toGetItemRequest)

  // GetItem decode: AWS SDK GetItemResponse → domain Option[Item]
  @Benchmark def decodingGetItem: List[Option[Item]] =
    getItemResponses.map(AwsCodecs.fromGetItemResponse)

  // PutItem encode: domain query → AWS SDK PutItemRequest
  @Benchmark def encodingPutItem: List[PutItemRequest] =
    putItemQueries.map(AwsCodecs.toPutItemRequest)

  // PutItem decode: AWS SDK PutItemResponse → domain Option[Item] (no ReturnValues, common case)
  @Benchmark def decodingPutItem: List[Option[Item]] =
    putItemResponses.map(AwsCodecs.fromPutItemResponse)

  // AttrMap construction: measures Item(...) allocation cost directly.
  // Unlike the encode/decode benchmarks, construction happens inside the method body
  // so it is measured rather than amortised away by @Setup.
  @Benchmark def constructItem: List[Item] =
    (1 to size).map { i =>
      Item("id" -> s"user-$i", "name" -> s"name-$i", "score" -> i, "active" -> (i % 2 == 0))
    }.toList
}
