package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, DeleteItem, GetItem, PutItem, Scan }
import zio.dynamodb.TestFixtures._
import zio.stream.ZStream
import zio.test.{ assertCompletes, DefaultRunnableSpec }

import scala.collection.immutable.{ Map => ScalaMap }

object BatchingModelSpec extends DefaultRunnableSpec {

  override def spec = suite("Batch Model")(batchModelSuite)

  val emptyItem                         = Item(ScalaMap.empty)
  def someItem: Option[Item]            = Some(emptyItem)
  def item(a: String): Item             = Item(ScalaMap(a -> AttributeValue.String(a)))
  def someItem(a: String): Option[Item] = Some(item(a))

  val primaryKey1                                             = PrimaryKey(ScalaMap("Field1" -> AttributeValue.String("1")))
  val primaryKey2                                             = PrimaryKey(ScalaMap("Field2" -> AttributeValue.String("2")))
  val tableName1                                              = TableName("T1")
  val tableName2                                              = TableName("T2")
  val tableName3                                              = TableName("T3")
  val indexName1                                              = IndexName("I1")
  val getItem1                                                = GetItem(key = primaryKey, tableName = tableName1)
  val getItem2                                                = GetItem(key = primaryKey, tableName = tableName2)
  val getItem3                                                = GetItem(key = primaryKey, tableName = tableName3)
  val zippedGets: DynamoDBQuery[(Option[Item], Option[Item])] = getItem1 zip getItem2

  val putItem1    = PutItem(tableName = tableName1, item = Item(ScalaMap.empty))
  val deleteItem1 = DeleteItem(tableName = tableName2, key = PrimaryKey(ScalaMap.empty))
  val stream1     = ZStream(emptyItem)
  val scan1       = Scan(tableName1, indexName1)

  /*
BatchGetItem request
{
  "MusicCollection": {
    "Keys": [
      {
        "Artist": {"S": "No One You Know"},
        "SongTitle": {"S": "Call Me Today"}
      },
      {
        "Artist": {"S": "Acme Band"},
        "SongTitle": {"S": "Happy Day"}
      },
      {
        "Artist": {"S": "No One You Know"},
        "SongTitle": {"S": "Scared of My Shadow"}
      }
    ],
    "ProjectionExpression":"Artist, SongTitle, AlbumTitle"
  }
}

BatchGetItem response
{
  "UnprocessedKeys": {},
  "Responses": {
    "MusicCollection": [
      {
        "Artist": {
          "S": "No One You Know"
        },
        "SongTitle": {
          "S": "Call Me Today"
        },
        "AlbumTitle": {
          "S": "Greatest Hits"
        }
      }
    ]
  },
  "ConsumedCapacity": [
    {
      "CapacityUnits": 0.5,
      "TableName": "MusicCollection"
    }
  ]
}

- what if multiple GetItem's for the same table had different ProjectionExpression?
  - we could aggregate the expression on each "add"
- we need to maintain "add" order - so that we can map response to the Tuple
- assumes that each returned Item is a superset of the query key used to find it
  - we could enforce this by adding key attributes - this will increase consumed capacity, maybe without the user realising
  - we prevent the add at compile or runtime with an error
  - for now I am just going to assume that returned item contain key

   */

  val batchModelSuite = suite("batching model")(
    test("explore GetItem batching") {

      val batch = (BatchGetItem(MapOfSet.empty) + GetItem(primaryKey1, tableName1)) + GetItem(primaryKey2, tableName1)

      println(batch)
      assertCompletes
    }
  )

}
