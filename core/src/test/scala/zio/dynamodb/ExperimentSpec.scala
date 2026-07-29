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
