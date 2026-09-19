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

import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{ DynamoDBError, DynamoDBQuery, Page, ProjectionExpression, UpdateExpression }
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * Every CRUD builder — `put`, `get`, `deleteFrom`, `update`, `query`, `scan` — plus
 * `updateAction`, an escape hatch for a raw update action built without an `Optic` (e.g. a
 * dynamic attribute name). See `CrudWalkthroughZio`/`CrudWalkthroughCE` for these queries
 * actually run against a table.
 */
object CrudBuilders {

  final case class Task(id: String, title: String, done: Boolean, priority: Int)

  object Task extends CompanionOptics[Task] {
    implicit val schema: Schema[Task] = Schema.derived

    val id: Lens[Task, String]    = $(_.id)
    val title: Lens[Task, String] = $(_.title)
    val done: Lens[Task, Boolean] = $(_.done)
    val priority: Lens[Task, Int] = $(_.priority)
  }

  val tasks: Table[Task] = Table[Task]("tasks")

  val putQuery: DynamoDBQuery[Task, Option[Task]] =
    put(tasks, Task("t1", "write docs", done = false, priority = 1))

  val putWithCondition: DynamoDBQuery[Task, Option[Task]] =
    put(tasks, Task("t1", "write docs", done = false, priority = 1)).where(Task.id.attributeNotExists)

  val getQuery: DynamoDBQuery[Task, Either[DynamoDBError.ItemError, Task]] =
    get(tasks)(Task.id.partitionKey === "t1")

  val deleteQuery: DynamoDBQuery[Task, Option[Task]] =
    deleteFrom(tasks)(Task.id.partitionKey === "t1")

  val deleteWithCondition: DynamoDBQuery[Task, Option[Task]] =
    deleteFrom(tasks)(Task.id.partitionKey === "t1").where(Task.done === true)

  val updateQuery: DynamoDBQuery[Task, Option[Task]] =
    update(tasks)(Task.id.partitionKey === "t1")(Task.done.set(true))

  val updateWithCondition: DynamoDBQuery[Task, Option[Task]] =
    update(tasks)(Task.id.partitionKey === "t1")(Task.priority.increment(1)).where(Task.done === false)

  // Escape hatch: a raw core Action built with the low-level ProjectionExpression `$` syntax.
  val rawAction: UpdateExpression.Action[Task] = ProjectionExpression.$("priority").set(5)

  val updateActionQuery: DynamoDBQuery[Task, Option[Task]] =
    updateAction(tasks)(Task.id.partitionKey === "t1")(rawAction)

  // sortOrder is a method on DynamoDBQuery itself, not on the builder, so the builder converts
  // to a DynamoDBQuery implicitly at that point in the chain.
  val queryQuery: DynamoDBQuery[Task, Page[Either[DynamoDBError.ItemError, Task]]] =
    query(tasks, limit = 20)
      .whereKey(Task.id.partitionKey === "t1")
      .filter(Task.priority > 0)
      .sortOrder(ascending = false)

  val scanQuery: DynamoDBQuery[Task, Page[Either[DynamoDBError.ItemError, Task]]] =
    scan(tasks, limit = 20).filter(Task.done === false)
}
