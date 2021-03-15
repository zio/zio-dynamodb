package zio.dynamodb

import zio.dynamodb.TestFixtures._
import zio.test.Assertion._
import zio.test.{ assert, DefaultRunnableSpec }

object BatchingSpec extends DefaultRunnableSpec {

  override def spec = suite("Batch Experiment")(batchingModelSuite)

  val batchingModelSuite = suite("batching should")(
    testM("batch putItem1 zip getItem1 zip getItem2 zip deleteItem1") {
      for {
        result  <- (putItem1 zip getItem1 zip getItem2 zip deleteItem1).execute
        expected = ((((), None), None), ())
      } yield assert(result)(equalTo(expected))
    },
    testM("batch getItem1 zip getItem2") {
      for {
        result  <- (getItem1 zip getItem2).execute
        expected = (None, None)
      } yield assert(result)(equalTo(expected))
    },
    testM("batch getItem1 zip getItem2") {
      for {
        result  <- (updateItem1 zip getItem2).execute
        expected = ((), None)
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
