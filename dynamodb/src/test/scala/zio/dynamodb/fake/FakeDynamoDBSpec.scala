package zio.dynamodb.fake

import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery.{ queryAll, querySome, scanAll, scanSome, DeleteItem }
import zio.dynamodb.fake.Database.{ chunkOfPrimaryKeyAndItem, resultItems }
import zio.dynamodb.{ BatchingFixtures, PrimaryKey }
import zio.test.Assertion._
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object FakeDynamoDBSpec extends DefaultRunnableSpec with BatchingFixtures {

  override def spec: ZSpec[Environment, Failure] =
    suite("FakeDynamoDB")(fakeDynamoDbSuite)

  private val executorWithTwoTables = FakeDynamoDBExecutor
    .table(tableName1.value, "k1")(primaryKey1 -> item1, primaryKey1_2 -> item1_2)
    .table(tableName3.value, "k3")(primaryKey3 -> item3)
    .layer

  private val fakeDynamoDbSuite = suite("FakeDynamoDB suite")(
    testM("getItem") {
      for {
        result  <- getItem1.execute
        expected = Some(item1)
      } yield assert(result)(equalTo(expected))
    }.provideLayer(executorWithTwoTables),
    testM("should execute putItem then getItem when sequenced in a ZIO") {
      for {
        _       <- putItem1.execute
        result  <- getItem1.execute
        expected = Some(item1)
      } yield assert(result)(equalTo(expected))
    }.provideLayer(FakeDynamoDBExecutor.table(tableName1.value, "k1")().layer),
    testM("should execute getItem1 zip getItem2 zip getItem3") {
      for {
        assembled <- (getItem1 zip getItem1_2 zip getItem3).execute
      } yield assert(assembled)(equalTo((Some(item1), Some(item1_2), Some(item3))))
    }.provideLayer(executorWithTwoTables),
    testM("should remove an item") {
      for {
        result1 <- getItem1.execute
        _       <- DeleteItem(tableName1, PrimaryKey("k1" -> "k1")).execute
        result2 <- getItem1.execute
        expected = Some(item1)
      } yield assert(result1)(equalTo(expected)) && assert(result2)(equalTo(None))
    }.provideLayer(executorWithTwoTables),
    testM("scanSome with limit greater than table size should scan all items in a table") {
      for {
        t           <- scanSome(tableName1.value, "k1", 10).execute
        (chunk, lek) = t
      } yield assert(chunk)(equalTo(resultItems(1 to 5))) && assert(lek)(equalTo(None))
    }.provideLayer(
      FakeDynamoDBExecutor.table(tableName1.value, "k1")(chunkOfPrimaryKeyAndItem(1 to 5, "k1"): _*).layer
    ),
    testM("scanAll should scan all items in a table") {
      for {
        stream <- scanAll(tableName1.value, "indexNameIgnored").execute
        chunk  <- stream.runCollect
      } yield assert(chunk)(equalTo(resultItems(1 to 5)))
    }.provideLayer(
      FakeDynamoDBExecutor.table(tableName1.value, "k1")(chunkOfPrimaryKeyAndItem(1 to 5, "k1"): _*).layer
    ),
    testM("querySome with limit greater than table size should scan all items in a table") {
      for {
        t           <- querySome(tableName1.value, "k1", 10).execute
        (chunk, lek) = t
      } yield assert(chunk)(equalTo(resultItems(1 to 5))) && assert(lek)(equalTo(None))
    }.provideLayer(
      FakeDynamoDBExecutor.table(tableName1.value, "k1")(chunkOfPrimaryKeyAndItem(1 to 5, "k1"): _*).layer
    ),
    testM("queryAll should scan all items in a table") {
      for {
        stream <- queryAll(tableName1.value, "indexNameIgnored").execute
        chunk  <- stream.runCollect
      } yield assert(chunk)(equalTo(resultItems(1 to 5)))
    }.provideLayer(FakeDynamoDBExecutor.table(tableName1.value, "k1")(chunkOfPrimaryKeyAndItem(1 to 5, "k1"): _*).layer)
  )

}
