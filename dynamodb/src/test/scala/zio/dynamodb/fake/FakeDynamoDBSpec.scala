package zio.dynamodb.fake

import zio.dynamodb.DynamoDBFixtures
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.fake.Database.{ chunkOfPrimaryKeyAndItem, resultItems }
import zio.test.Assertion._
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object FakeDynamoDBSpec extends DefaultRunnableSpec with DynamoDBFixtures {

  override def spec: ZSpec[Environment, Failure] =
    suite("FakeDynamoDB")(fakeDynamoDbSuite)

  private val executorWithTwoTables = FakeDynamoDBExecutor
    .table2(tableName1.value, "k1")(primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
    .table2(tableName3.value, "k3")(primaryKeyT3 -> itemT3)
    .layer

  private val fakeDynamoDbSuite = suite("FakeDynamoDB suite")(
    testM("getItem") {
      for {
        result  <- getItemT1.execute
        expected = Some(itemT1)
      } yield assert(result)(equalTo(expected))
    }.provideLayer(executorWithTwoTables),
    testM("should execute putItem then getItem when sequenced in a ZIO") {
      for {
        _       <- putItem1.execute
        result  <- getItemT1.execute
        expected = Some(itemT1)
      } yield assert(result)(equalTo(expected))
    }.provideLayer(FakeDynamoDBExecutor.table2(tableName1.value, "k1")().layer),
    testM("should execute getItem1 zip getItem2 zip getItem3") {
      for {
        assembled <- (getItemT1 zip getItemT1_2 zip getItemT3).execute
      } yield assert(assembled)(equalTo((Some(itemT1), Some(itemT1_2), Some(itemT3))))
    }.provideLayer(executorWithTwoTables),
    testM("should remove an item") {
      for {
        result1 <- getItemT1.execute
        _       <- DeleteItem(tableName1, primaryKeyT1).execute
        result2 <- getItemT1.execute
        expected = Some(itemT1)
      } yield assert(result1)(equalTo(expected)) && assert(result2)(equalTo(None))
    }.provideLayer(executorWithTwoTables),
    testM("scanSome with limit greater than table size should scan all items in a table") {
      for {
        t           <- scanSome(tableName1.value, "k1", 10).execute
        (chunk, lek) = t
      } yield assert(chunk)(equalTo(resultItems(1 to 5))) && assert(lek)(equalTo(None))
    }.provideLayer(
      FakeDynamoDBExecutor.table2(tableName1.value, "k1")(chunkOfPrimaryKeyAndItem(1 to 5, "k1"): _*).layer
    ),
    testM("scanAll should scan all items in a table") {
      for {
        stream <- scanAll(tableName1.value, "indexNameIgnored").execute
        chunk  <- stream.runCollect
      } yield assert(chunk)(equalTo(resultItems(1 to 5)))
    }.provideLayer(
      FakeDynamoDBExecutor.table2(tableName1.value, "k1")(chunkOfPrimaryKeyAndItem(1 to 5, "k1"): _*).layer
    ),
    testM("querySome with limit greater than table size should scan all items in a table") {
      for {
        t           <- querySome(tableName1.value, "k1", 10).execute
        (chunk, lek) = t
      } yield assert(chunk)(equalTo(resultItems(1 to 5))) && assert(lek)(equalTo(None))
    }.provideLayer(
      FakeDynamoDBExecutor.table2(tableName1.value, "k1")(chunkOfPrimaryKeyAndItem(1 to 5, "k1"): _*).layer
    ),
    testM("queryAll should scan all items in a table") {
      for {
        stream <- queryAll(tableName1.value, "indexNameIgnored").execute
        chunk  <- stream.runCollect
      } yield assert(chunk)(equalTo(resultItems(1 to 5)))
    }.provideLayer(
      FakeDynamoDBExecutor.table2(tableName1.value, "k1")(chunkOfPrimaryKeyAndItem(1 to 5, "k1"): _*).layer
    )
  )

}
