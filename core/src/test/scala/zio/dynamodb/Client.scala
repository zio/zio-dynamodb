package zio.dynamodb

import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBQuery.BatchWriteItem
import zio.dynamodb.ProjectionExpression.{ $, Unknown }

object Client {
  final case class Person(id: String, name: String)
  val item: AttrMap                             = Item("id" -> "123", "name" -> "test")
  val putItem: DynamoDBQuery[Any, Option[Item]] = DynamoDBQuery.putItem("my-table", item)
  val putItemResult: DummyIO[Option[Item]]      = DummyIOInterpreter.run(putItem)

  val people: List[Person]                                    = List(Person("123", "test"))
  val batchWriteItems: DynamoDBQuery.BatchWriteItem           =
    DynamoDBQuery.batchWriteItem(people)(person =>
      DynamoDBQuery.putItem("my-table", Item("id" -> person.id, "name" -> person.name))
    )
  val batchWriteItemsResult: DummyIO[BatchWriteItem.Response] = DummyIOInterpreter.run(batchWriteItems)

  val batchGetItem: DynamoDBQuery.BatchGetItem =
    DynamoDBQuery.batchGetItem(people) { person =>
      DynamoDBQuery.getItem("my-table", PrimaryKey("id" -> person.id))
    }

  val getItem: DynamoDBQuery[Any, Option[Item]] = DynamoDBQuery.getItem("my-table", PrimaryKey("id" -> "123"))
  val getItemResult: DummyIO[Option[Item]]      = DummyIOInterpreter.run(getItem)

  val updateItem: DynamoDBQuery[Any, Option[Item]] = DynamoDBQuery.updateItem("my-table", PrimaryKey("id" -> "123")) {
    $("count").set(1)
  }

  val xs: Chunk[DynamoDBQuery[Any, Option[Item]]] = Chunk(updateItem)

  val peName: ProjectionExpression[Any, Unknown] = $("name")

  // Condition/Filter expressions
  val expr: FilterExpression[Any]                                  = $("name") === "test" && $("age") > 18
  // KeyCondition expressions - primary keys
  val expr1: KeyConditionExpr.PartitionKeyEquals[Any]              = $("name").partitionKey === "test"
  val expr2: KeyConditionExpr.CompositePrimaryKeyExpr[Any]         =
    $("name").partitionKey === "test" && $("age").sortKey === 18
  // KeyCondition expressions - not a primary key but a key condition expression that can be used in Query operations
  val expr3: KeyConditionExpr.ExtendedCompositePrimaryKeyExpr[Any] =
    $("name").partitionKey === "test" && $("age").sortKey > 18

  /*
  item.zip(item)
  PROBLEMS WITH AUTO BATCHING
  - too powerful - interface presented to the user is so abstract that users get confused when things go wrong
  - not needed - users have clear intent on policies around parallelism and batching before they write code

  PROBLEMS WITH BATCHING:
  - batched API has rules - users are guaranteed to make mistakes with a generic batch API
  RULES
  2 API's
  - Batched reads - only Get
  - Batched writes - only Put and Delete

  WHEN DOES A USER NEED PARALLEL EXECUTION?
  - when they cant batch eg a bunch or Updates

   */
}
