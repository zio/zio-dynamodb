package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.{ parallelize, Constructor, Map }
import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.test.Assertion.equalTo
import zio.test.{ assert, DefaultRunnableSpec }

import scala.collection.immutable.{ Map => ScalaMap }

//noinspection TypeAnnotation
object ExecutorSpec extends DefaultRunnableSpec {

  override def spec = suite("Executor")(parallelizeSuite, executeSuite)

  val executeSuite = suite("execute")(
    testM("should execute getItem1 zip getItem2") {
      for {
        assembled <- (getItem1 zip getItem2).execute
      } yield assert(assembled)(equalTo((Some(item1), Some(item2))))
    },
    testM("should execute a single GetItem - note these are still batched") {
      for {
        assembled <- getItem1.execute
      } yield assert(assembled)(equalTo(Some(item1)))
    },
    testM("should execute putItem1 zip deleteItem1") {
      for {
        assembled <- (putItem1 zip deleteItem1).execute
      } yield assert(assembled)(equalTo(((), ())))
    },
    testM("should execute a ScanPage") {
      for {
        assembled <- scanPage1.execute
      } yield assert(assembled)(equalTo((Chunk(emptyItem), None)))
    },
    testM("should execute a QueryPage") {
      for {
        assembled <- queryPage1.execute
      } yield assert(assembled)(equalTo((Chunk(emptyItem), None)))
    },
    testM("should execute a ScanAll") {
      for {
        assembled <- scanAll1.execute
      } yield assert(assembled)(equalTo(stream1))
    },
    testM("should execute a QueryAll") {
      for {
        assembled <- queryAll1.execute
      } yield assert(assembled)(equalTo(stream1))
    },
    testM("should execute create table") {
      for {
        assembled <- createTable1.execute
      } yield assert(assembled)(equalTo(()))
    }
  ).provideCustomLayer(DynamoDBExecutor.test)

  val parallelizeSuite =
    suite(label = "parallelize")(
      test(label = "should process Zipped GetItems") {
        val (constructor, assembler): (Chunk[Constructor[Any]], Chunk[Any] => (Option[Item], Option[Item])) =
          parallelize(getItem1 zip getItem2)
        val assembled                                                                                       = assembler(Chunk(someItem("1"), someItem("2")))

        assert(constructor)(equalTo(Chunk(getItem1, getItem2))) && assert(assembled)(
          equalTo((someItem("1"), someItem("2")))
        )
      },
      test("should process Zipped writes") {
        val (constructor, assembler) = parallelize(putItem1 zip deleteItem1)
        val assembled                = assembler(Chunk((), ()))

        assert(constructor)(equalTo(Chunk(putItem1, deleteItem1))) &&
        assert(assembled)(equalTo(((), ())))
      },
      test("should process Map constructor") {
        val map                      = Map(
          getItem1,
          (o: Option[Item]) => o.map(_ => Item(ScalaMap("1" -> AttributeValue.String("2"))))
        )
        val (constructor, assembler) = parallelize(map)
        val assembled                = assembler(Chunk(someItem("1")))

        assert(constructor)(equalTo(Chunk(getItem1))) && assert(assembled)(
          equalTo(Some(Item(ScalaMap("1" -> AttributeValue.String("2")))))
        )
      },
      test("should process ScanPage constructor") {
        val (constructor, assembler) = parallelize(scanPage1)
        val assembled                = assembler(Chunk((Chunk(emptyItem), None)))

        assert(constructor)(equalTo(Chunk(scanPage1))) && assert(assembled)(equalTo((Chunk(emptyItem), None)))
      }
    )

}
