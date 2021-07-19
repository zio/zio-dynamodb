package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.test.Assertion._
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object FakeDynamoDBSpec extends DefaultRunnableSpec with BatchingFixtures {

  override def spec: ZSpec[Environment, Failure] = suite("Batching")(batchingSuite)
  private val map                                = Map(primaryKey1 -> item1, primaryKey2 -> item2)

  private val batchingSuite = suite("FakeDynamoDB suite")(
    testM("getItem") {
      for {
        result  <- getItem1.execute
        expected = Some(item1)
      } yield assert(result)(equalTo(expected))
    },
    testM("should execute getItem1 zip getItem2") {
      for {
        assembled <- (getItem1 zip getItem2).execute
      } yield assert(assembled)(equalTo((Some(item1), Some(item2))))
    }
  ).provideCustomLayer(
    FakeDynamoDBExecutor.fake(map, pk = item => Item(item.map.filter { case (key, _) => key == "a" }))
  )

}
