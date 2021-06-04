package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery._
import zio.test.Assertion.equalTo
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object ExecutorSpec extends DefaultRunnableSpec {

  override def spec: ZSpec[Environment, Failure] = suite("Executor")(parallelizeSuite, executeSuite)

  private val putItem1    = putItem("T1", item = Item("k1" -> "k1"))
  private val deleteItem1 = deleteItem("T1", key = PrimaryKey.empty)

  private val executeSuite = suite("execute")(
    testM("should execute forEach of GetItems (resulting in a batched request)") {
      for {
        assembled <- forEach(1 to 2)(i => createGetItem(i)).execute
      } yield assert(assembled)(equalTo(List(someItem("k1"), someItem("k2"))))
    },
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
    testM("should execute a ScanSome") {
      for {
        assembled <- scanPage1.execute
      } yield assert(assembled)(equalTo((Chunk(emptyItem), None)))
    },
    testM("should execute a QuerySome") {
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

  private val parallelizeSuite =
    suite(label = "parallelize")(
      test("should parallelize forEach of GetItems") {
        val foreach                   = forEach(1 to 3)(i => createGetItem(i))
        val (constructors, assembler) = parallelize(foreach)
        val assembled                 = assembler(Chunk(someItem("1"), someItem("2"), someItem("3")))

        assert(constructors)(equalTo(Chunk(createGetItem(1), createGetItem(2), createGetItem(3)))) && assert(assembled)(
          equalTo(List(someItem("1"), someItem("2"), someItem("3")))
        )
      },
      test(label = "should process Zipped GetItems") {
        val (constructors, assembler): (Chunk[Constructor[Any]], Chunk[Any] => (Option[AttrMap], Option[AttrMap])) =
          parallelize(getItem1 zip getItem2)
        val assembled                                                                                              = assembler(Chunk(someItem("1"), someItem("2")))

        assert(constructors)(equalTo(Chunk(getItem1, getItem2))) && assert(assembled)(
          equalTo((someItem("1"), someItem("2")))
        )
      },
      test("should process Zipped writes") {
        val (constructors, assembler) = parallelize(putItem1 zip deleteItem1)
        val assembled                 = assembler(Chunk((), ()))

        assert(constructors)(equalTo(Chunk(putItem1, deleteItem1))) &&
        assert(assembled)(equalTo(((), ())))
      },
      test("should process Map constructor") {
        val map                       = Map(
          getItem1,
          (o: Option[AttrMap]) => o.map(_ => AttrMap("1" -> "2"))
        )
        val (constructors, assembler) = parallelize(map)
        val assembled                 = assembler(Chunk(someItem("1")))

        assert(constructors)(equalTo(Chunk(getItem1))) && assert(assembled)(
          equalTo(Some(AttrMap("1" -> "2")))
        )
      },
      test("should process ScanSome constructor") {
        val (constructors, assembler) = parallelize(scanPage1)
        val assembled                 = assembler(Chunk((Chunk(emptyItem), None)))

        assert(constructors)(equalTo(Chunk(scanPage1))) && assert(assembled)(equalTo((Chunk(emptyItem), None)))
      }
    )

}
