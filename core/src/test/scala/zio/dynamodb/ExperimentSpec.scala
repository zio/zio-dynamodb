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

import zio.dynamodb.DynamoDBQuery.GetItem
import zio.test.{ assertTrue, ZIOSpecDefault }

object ExperimentSpec extends ZIOSpecDefault {

  override def spec = suite("ExperimentSpec")(
    test("DynamoDBQuery.GetItem should be constructable") {
      val query = GetItem(
        tableName = "my-table",
        key = PrimaryKey("id" -> "123"),
        projections = List(ProjectionExpression.$("name"))
      )
      assertTrue(query.tableName == "my-table") &&
      assertTrue(query.key == PrimaryKey("id" -> "123")) &&
      assertTrue(query.projections == List(ProjectionExpression.$("name"))) &&
      assertTrue(query.consistency == ConsistencyMode.Weak) &&
      assertTrue(query.capacity == ReturnConsumedCapacity.None) &&
      assertTrue(query.retryPolicy == None)
    }
  )

}
