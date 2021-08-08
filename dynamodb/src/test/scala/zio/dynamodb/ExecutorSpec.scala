package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery._
import zio.test.Assertion.equalTo
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object ExecutorSpec extends DefaultRunnableSpec with DynamoDBFixtures {
  override def spec: ZSpec[Environment, Failure] = suite("Executor")(parallelizeSuite)

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
          parallelize(getItemT1 zip getItemT2)
        val assembled                                                                                              = assembler(Chunk(someItem("1"), someItem("2")))

        assert(constructors)(equalTo(Chunk(getItemT1, getItemT2))) && assert(assembled)(
          equalTo((someItem("1"), someItem("2")))
        )
      },
      test("should process Zipped writes") {
        val (constructors, assembler) = parallelize(putItem1 zip deleteItem1)
        val assembled                 = assembler(Chunk())

        assert(constructors)(equalTo(Chunk(putItem1, deleteItem1))) &&
        assert(assembled)(equalTo(()))
      },
      test("should process Map constructor") {
        val map                       = Map(
          getItemT1,
          (o: Option[AttrMap]) => o.map(_ => AttrMap("1" -> "2"))
        )
        val (constructors, assembler) = parallelize(map)
        val assembled                 = assembler(Chunk(someItem("1")))

        assert(constructors)(equalTo(Chunk(getItemT1))) && assert(assembled)(
          equalTo(Some(AttrMap("1" -> "2")))
        )
      },
      test("should process ScanSome constructor") {
        val scanPage                  = scanSome("T1", "I1", limit = 10)
        val (constructors, assembler) = parallelize(scanPage)
        val assembled                 = assembler(Chunk((Chunk(Item.empty), None)))

        assert(constructors)(equalTo(Chunk(scanPage))) && assert(assembled)(equalTo((Chunk(Item.empty), None)))
      }
    )

}
