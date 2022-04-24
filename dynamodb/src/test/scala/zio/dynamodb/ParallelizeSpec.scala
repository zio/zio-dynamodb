package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery._
import zio.test.Assertion.equalTo
import zio.test.assert
import zio.test.ZIOSpecDefault

object ParallelizeSpec extends ZIOSpecDefault with DynamoDBFixtures {
  override def spec = suite("Executor")(parallelizeSuite)

  private val parallelizeSuite =
    suite(label = "parallelize")(
      test(label = "should process Zipped GetItems") {
        val (constructors, assembler): (Chunk[Constructor[Any]], Chunk[Any] => (Option[AttrMap], Option[AttrMap])) =
          parallelize(getItemT1 zip getItemT1_2)
        val assembled                                                                                              = assembler(Chunk(Some(itemT1), Some(itemT1_2)))

        assert(constructors)(equalTo(Chunk(getItemT1, getItemT1_2))) && assert(assembled)(
          equalTo((Some(itemT1), Some(itemT1_2)))
        )
      },
      test("should process Zipped writes") {
        val (constructors, assembler) = parallelize(putItemT1 zip deleteItemT1)
        val assembled                 = assembler(Chunk())

        assert(constructors)(equalTo(Chunk(putItemT1, deleteItemT1))) &&
        assert(assembled)(equalTo(()))
      },
      test("should process Map constructor") {
        val map                       = Map(
          getItemT1,
          (o: Option[AttrMap]) => o.map(_ => AttrMap("1" -> "2"))
        )
        val (constructors, assembler) = parallelize(map)
        val assembled                 = assembler(Chunk(Some(itemT1)))

        assert(constructors)(equalTo(Chunk(getItemT1))) && assert(assembled)(
          equalTo(Some(AttrMap("1" -> "2")))
        )
      },
      test("should process ScanSome constructor") {
        val scanPage                  = scanSomeItem("T1", limit = 10)
        val (constructors, assembler) = parallelize(scanPage)
        val assembled                 = assembler(Chunk((Chunk(Item.empty), None)))

        assert(constructors)(equalTo(Chunk(scanPage))) && assert(assembled)(equalTo((Chunk(Item.empty), None)))
      }
    )

}
