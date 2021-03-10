package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.{ Constructor, Map }
import zio.dynamodb.TestFixtures._
import zio.test.Assertion.equalTo
import zio.test.{ assert, DefaultRunnableSpec }

import scala.collection.immutable.{ Map => ScalaMap }

object ExecutorSpec extends DefaultRunnableSpec {

  override def spec = suite("Executor")(parallelizeSuite, executeSuite)

  val executeSuite = suite("execute")(
    testM("should assemble response") {
      for {
        assembled <- execute(zippedGets)
      } yield assert(assembled)(equalTo((someItem, someItem)))
    }
  ).provideCustomLayer(DynamoDb.test >>> DynamoDBExecutor.live)

  val parallelizeSuite =
    suite(label = "parallelize")(
      test(label = "should process Zipped GetItems") {
        val (constructor, assembler): (Chunk[Constructor[Any]], Chunk[Any] => (Option[Item], Option[Item])) =
          DynamoDBExecutor.parallelize(zippedGets)
        val assembled                                                                                       = assembler(Chunk(someItem("1"), someItem("2")))

        assert(constructor)(equalTo(Chunk(getItem1, getItem2))) && assert(assembled)(
          equalTo((someItem("1"), someItem("2")))
        )
      },
      test("should process Zipped writes") {
        val (constructor, assembler) = DynamoDBExecutor.parallelize(putItem1 zip deleteItem1)
        val assembled                = assembler(Chunk((), ()))

        assert(constructor)(equalTo(Chunk(putItem1, deleteItem1))) &&
        assert(assembled)(equalTo(((), ())))
      },
      test("should process Map constructor") {
        val map                      = Map(
          getItem1,
          (o: Option[Item]) => o.map(_ => Item(ScalaMap("1" -> AttributeValue.String("2"))))
        )
        val (constructor, assembler) = DynamoDBExecutor.parallelize(map)
        val assembled                = assembler(Chunk(someItem("1")))

        assert(constructor)(equalTo(Chunk(getItem1))) && assert(assembled)(
          equalTo(Some(Item(ScalaMap("1" -> AttributeValue.String("2")))))
        )
      },
      test("should process Scan constructor") {
        val (constructor, assembler) = DynamoDBExecutor.parallelize(scan1)
        val assembled                = assembler(Chunk((stream1, None)))

        assert(constructor)(equalTo(Chunk(scan1))) && assert(assembled)(equalTo((stream1, None)))
      }
    )

}
