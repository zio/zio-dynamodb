package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery.{ forEach, getItem }
import zio.dynamodb.fake.FakeDynamoDBExecutor
import zio.test.Assertion._
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object BatchingSpec extends DefaultRunnableSpec with BatchingFixtures {

  private val executorWithOneTable = FakeDynamoDBExecutor
    .table(tableName1.value, "k1")(primaryKey1 -> item1, primaryKey1_2 -> item1_2)
    .layer

  private val executorWithTwoTables = FakeDynamoDBExecutor
    .table(tableName1.value, "k1")(primaryKey1 -> item1, primaryKey1_2 -> item1_2)
    .table(tableName3.value, "k3")(primaryKey3 -> item3)
    .layer

  override def spec: ZSpec[Environment, Failure] = suite("Batching")(batchingSuite)

  private val batchingSuite = suite("batching should")(
    testM("batch putItem1 zip putItem1_2") {
      for {
        result      <- (putItem1 zip putItem1_2).execute
        table1Items <- (getItem1 zip getItem1_2).execute
      } yield assert(result)(equalTo(())) && assert(table1Items)(equalTo((Some(item1), Some(item1_2))))
    }.provideLayer(FakeDynamoDBExecutor.table(tableName1.value, "k1")().layer),
    testM("batch getItem1 zip getItem2 zip getItem3 returns 3 items that are found") {
      for {
        result  <- (getItem1 zip getItem1_2 zip getItem3).execute
        expected = (Some(item1), Some(item1_2), Some(item3))
      } yield assert(result)(equalTo(expected))
    }.provideLayer(executorWithTwoTables),
    testM("batch getItem1 zip getItem2 zip getItem3 returns 2 items that are found") {
      for {
        result  <- (getItem1 zip getItem1_2 zip getItem1_NotExists).execute
        expected = (Some(item1), Some(item1_2), None)
      } yield assert(result)(equalTo(expected))
    }.provideLayer(executorWithOneTable),
    testM("batch putItem1 zip getItem1 zip getItem2 zip deleteItem1") {
      for {
        result      <- (putItem3_2 zip getItem1 zip getItem1_2 zip deleteItem3).execute
        expected     = (Some(item1), Some(item1_2))
        table3Items <- (getItem3 zip getItem3_2).execute
      } yield assert(result)(equalTo(expected)) && assert(table3Items)(equalTo((None, Some(item3_2))))
    }.provideLayer(executorWithTwoTables),
    testM("should execute forEach of GetItems (resulting in a batched request)") {
      for {
        result <- forEach(1 to 2) { i =>
                    getItem(tableName1.value, PrimaryKey("k1" -> s"k$i"))
                  }.execute
      } yield assert(result)(equalTo(List(Some(item1), Some(item1_2))))
    }.provideLayer(executorWithTwoTables)
  )

}
