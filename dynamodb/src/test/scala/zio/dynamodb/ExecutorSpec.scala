package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.{ Constructor, DeleteItem, GetItem, Map, PutItem, Scan }
import zio.stream.ZStream
import zio.test.Assertion.equalTo
import zio.test.{ assert, DefaultRunnableSpec }

import scala.collection.immutable.{ Map => ScalaMap }

object ExecutorSpec extends DefaultRunnableSpec {

  val emptyItem                         = Item(ScalaMap.empty)
  def someItem: Option[Item]            = Some(emptyItem)
  def item(a: String): Item             = Item(ScalaMap(a -> AttributeValue.String(a)))
  def someItem(a: String): Option[Item] = Some(item(a))

  val primaryKey                                              = PrimaryKey(ScalaMap.empty)
  val tableName1                                              = TableName("T1")
  val tableName2                                              = TableName("T2")
  val indexName1                                              = IndexName("I1")
  val getItem1                                                = GetItem(key = primaryKey, tableName = tableName1)
  val getItem2                                                = GetItem(key = primaryKey, tableName = tableName2)
  val zippedGets: DynamoDBQuery[(Option[Item], Option[Item])] = getItem1 zip getItem2

  val putItem1    = PutItem(tableName = tableName1, item = Item(ScalaMap.empty))
  val deleteItem1 = DeleteItem(tableName = tableName2, key = PrimaryKey(ScalaMap.empty))
  val stream1     = ZStream(emptyItem)
  val scan1       = Scan(tableName1, indexName1)

  override def spec = suite("Executor")(parallelizeSuite, executeAgainstDbSuite)

  val executeAgainstDbSuite = suite("Execute against DDB")(
    testM("should assemble response") {
      for {
        assembled <- execute(zippedGets)
      } yield assert(assembled)(equalTo((someItem, someItem)))
    }
//    testM("test2") {
//      val constructors                        = Chunk(getItem1, getItem2, putItem1)
//      val x                                   = constructors.foldRight((List.empty[Constructor[Any]], BatchWriteItem(WriteItemsMap()))) {
//        case (cons, (xs, bwi)) =>
//          cons match {
//            case getItem @ GetItem(_, _, _, _, _) => (xs, bwi.+ getItem)
//            case el                               => (xs :+ el, bwi)
//          }
//      }
//      val xx: ZIO[Any, Exception, Chunk[Any]] = ZIO.foreach(tuple._1)(executeQueryAgainstDdb)
//
//      for {
//        chunks   <- xx
//        assembled = tuple._2(chunks)
//        _        <- UIO(println(s"assembled=$assembled"))
//      } yield assertCompletes
//    }
  ).provideCustomLayer(DynamoDb.test >>> DynamoDBExecutor.live)

  val parallelizeSuite =
    suite(label = "parallelize")(
      test(label = "should aggregate Zipped GetItems") {
        val (constructor, assembler): (Chunk[Constructor[Any]], Chunk[Any] => (Option[Item], Option[Item])) =
          DynamoDBExecutor.parallelize(zippedGets)
        val assembled                                                                                       = assembler(Chunk(someItem("1"), someItem("2")))

        assert(constructor)(equalTo(Chunk(getItem1, getItem2))) && assert(assembled)(
          equalTo((someItem("1"), someItem("2")))
        )
      },
      test("should aggregate Zipped writes") {
        val (constructor, assembler) = DynamoDBExecutor.parallelize(putItem1 zip deleteItem1)
        val assembled                = assembler(Chunk((), ()))

        assert(constructor)(equalTo(Chunk(putItem1, deleteItem1))) &&
        assert(assembled)(equalTo(((), ())))
      },
      test("should process Map constructor") {
        val map1                     = Map(
          getItem1,
          (o: Option[Item]) => o.map(_ => Item(ScalaMap("1" -> AttributeValue.String("2"))))
        )
        val (constructor, assembler) = DynamoDBExecutor.parallelize(map1)
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
