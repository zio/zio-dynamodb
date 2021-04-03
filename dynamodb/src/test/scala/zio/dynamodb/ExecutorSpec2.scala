package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery.{ forEach, forEach2, parallelize }
import zio.test.Assertion.equalTo
import zio.test.{ assert, DefaultRunnableSpec, TestAspect }

//noinspection TypeAnnotation
object ExecutorSpec2 extends DefaultRunnableSpec {

  override def spec = suite("Executor")(executeSuite)

  val executeSuite = suite("execute")(
    test("should execute forEach2 of GetItems") {
      val foreach                  = forEach2(1 to 2)(i => getItem(i))
      val (constructor, assembler) =
        parallelize(foreach)
      val assembled                =
        assembler(Chunk(someItem("1"), someItem("2")))
      assert(constructor)(equalTo(Chunk(getItem(1), getItem(2)))) && assert(assembled)(
        equalTo((someItem("1"), someItem("2")))
      )
    },
    test("should execute forEach of GetItems") {
      val foreach                  = forEach(1 to 2)(i => getItem(i))
      println(s"foreach=$foreach")
      /*
      Map(
        Zip(
          GetItem(TableName(T1),PrimaryKey(Map(1 -> String(1))),Weak,List(),None), Map(
            Zip(GetItem(TableName(T1),PrimaryKey(Map(2 -> String(2))),Weak,List(),None), Succeed(zio.dynamodb.DynamoDBQuery$$$Lambda$659/301809128@5525f571)),
            scala.Function2$$Lambda$730/1163110641@e94be69)
          ),
        scala.Function2$$Lambda$730/1163110641@6c99aa69
      )
       */
      println("x")
      val (constructor, assembler) =
        parallelize(foreach)
      println(s"constructor=$constructor assembled=$assembler")
      println("========================================================================================")
      val assembled                =
        assembler(Chunk(someItem("1"), someItem("2")))
      println(assembled)
      assert(constructor)(equalTo(Chunk(getItem(1), getItem(2))))
    } @@ TestAspect.ignore
  ).provideCustomLayer(DynamoDBExecutor.test)

}
