package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery.{ forEach, parallelize }
import zio.test.Assertion.equalTo
import zio.test.{ assert, DefaultRunnableSpec }

//noinspection TypeAnnotation
object ExecutorSpec2 extends DefaultRunnableSpec {

  override def spec = suite("Executor")(executeSuite)

  val executeSuite = suite("execute")(
    testM("should execute forEach of GetItems") {
      for {
        assembled <- forEach(1 to 2)(i => getItem(i)).execute
      } yield assert(assembled)(equalTo(List(someItem("k1"), someItem("k2"))))
    },
    test("should parallelize forEach of GetItems") {
      val foreach                  = forEach(1 to 3)(i => getItem(i))
      val (constructor, assembler) = parallelize(foreach)
      val assembled                = assembler(Chunk(someItem("1"), someItem("2"), someItem("3")))

      assert(constructor)(equalTo(Chunk(getItem(1), getItem(2), getItem(3)))) && assert(assembled)(
        equalTo(List(someItem("1"), someItem("2"), someItem("3")))
      )
    }
  ).provideCustomLayer(DynamoDBExecutor.test)

}
