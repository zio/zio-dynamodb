package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.test.Assertion._
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object BatchingSpec extends DefaultRunnableSpec with BatchingFixtures {

  override def spec: ZSpec[Environment, Failure] = suite("Batching")(batchingSuite)

  private val batchingSuite = suite("batching should")(
    testM("batch putItem1 zip getItem1 zip getItem2 zip deleteItem1") {
      for {
        result  <- (putItem1 zip getItem1 zip getItem2 zip deleteItem1).execute
        expected = ((((), Some(item1)), Some(item2)), ())
      } yield assert(result)(equalTo(expected))
    },
    testM("batch getItem1 zip getItem2") {
      for {
        result  <- (getItem1 zip getItem2).execute
        expected = (Some(item1), Some(item2))
      } yield assert(result)(equalTo(expected))
    },
    testM("batch updateItem1 zip getItem2") {
      for {
        result  <- (updateItem1 zip getItem2).execute
        expected = ((), Some(item2))
      } yield assert(result)(equalTo(expected))
    },
    testM("batch updateItem1 zip updateItem1") {
      for {
        result  <- (updateItem1 zip updateItem1).execute
        expected = ((), ())
      } yield assert(result)(equalTo(expected))
    }
  ).provideCustomLayer(DynamoDBExecutor.test)

}
