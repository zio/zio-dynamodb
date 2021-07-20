package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.test.Assertion._
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object FakeDynamoDBSpec extends DefaultRunnableSpec with BatchingFixtures {

  override def spec: ZSpec[Environment, Failure] = suite("Batching")(batchingSuite)
  private val map                                = Map(
    tableName1.value -> Map(primaryKey1 -> item1, primaryKey2 -> item2),
    tableName3.value -> Map(primaryKey3 -> item3)
  )

  private val batchingSuite = suite("FakeDynamoDB suite")(
    testM("getItem") {
      for {
        result  <- getItem1.execute
        expected = Some(item1)
      } yield assert(result)(equalTo(expected))
    }.provideLayer(FakeDynamoDBExecutor(map)("k1")),
//    testM("putItem then getItem") {
//      for {
//        _       <- putItem1.execute
//        result  <- getItem1.execute
//        expected = Some(item1)
//      } yield assert(result)(equalTo(expected))
//    }.provideLayer(FakeDynamoDBExecutor()("k1")),
    testM("should execute getItem1 zip getItem2 zip getItem3") {
      for {
        assembled <- (getItem1 zip getItem2 zip getItem3).execute
      } yield assert(assembled)(equalTo((Some(item1), Some(item2), Some(item3))))
    }.provideLayer(FakeDynamoDBExecutor(map)("k1"))
  )

}
