package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ forEach, getItem }
import zio.dynamodb.fake.FakeDynamoDBExecutor
import zio.test.Assertion._
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object BatchingSpec extends DefaultRunnableSpec with DynamoDBFixtures {

  private val executorWithOneTable = FakeDynamoDBExecutor
    .table2(tableName1.value, "k1")(primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
    .layer

  private val executorWithTwoTables = FakeDynamoDBExecutor
    .table2(tableName1.value, "k1")(primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
    .table2(tableName3.value, "k3")(primaryKeyT3 -> itemT3)
    .layer

  override def spec: ZSpec[Environment, Failure] = suite("Batching")(batchingSuite)

  private val batchingSuite = suite("batching should")(
    testM("batch putItem1 zip putItem1_2") {
      for {
        result      <- (putItem1 zip putItem1_2).execute
        table1Items <- (getItemT1 zip getItemT1_2).execute
      } yield assert(result)(equalTo(())) && assert(table1Items)(equalTo((Some(itemT1), Some(itemT1_2))))
    }.provideLayer(FakeDynamoDBExecutor.table2(tableName1.value, "k1")().layer),
    testM("batch getItem1 zip getItem2 zip getItem3 returns 3 items that are found") {
      for {
        result  <- (getItemT1 zip getItemT1_2 zip getItemT3).execute
        expected = (Some(itemT1), Some(itemT1_2), Some(itemT3))
      } yield assert(result)(equalTo(expected))
    }.provideLayer(executorWithTwoTables),
    testM("batch getItem1 zip getItem2 zip getItem3 returns 2 items that are found") {
      for {
        result  <- (getItemT1 zip getItemT1_2 zip getItemT1_NotExists).execute
        expected = (Some(itemT1), Some(itemT1_2), None)
      } yield assert(result)(equalTo(expected))
    }.provideLayer(executorWithOneTable),
    testM("batch putItem1 zip getItem1 zip getItem2 zip deleteItem1") {
      for {
        result      <- (putItem3_2 zip getItemT1 zip getItemT1_2 zip deleteItemT3).execute
        expected     = (Some(itemT1), Some(itemT1_2))
        table3Items <- (getItemT3 zip getItemT3_2).execute
      } yield assert(result)(equalTo(expected)) && assert(table3Items)(equalTo((None, Some(itemT3_2))))
    }.provideLayer(executorWithTwoTables),
    testM("should execute forEach of GetItems (resulting in a batched request)") {
      for {
        result <- forEach(1 to 2) { i =>
                    getItem(tableName1.value, PrimaryKey("k1" -> s"v$i"))
                  }.execute
      } yield assert(result)(equalTo(List(Some(itemT1), Some(itemT1_2))))
    }.provideLayer(executorWithTwoTables)
  )

}
