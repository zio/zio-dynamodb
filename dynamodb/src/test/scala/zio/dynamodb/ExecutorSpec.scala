package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.{ Constructor, DeleteItem, GetItem, Map, PutItem, Scan }
import zio.stream.ZStream
import zio.test.Assertion.equalTo
import zio.test.{ assert, assertCompletes, DefaultRunnableSpec, TestAspect }

import scala.collection.immutable.{ Map => ScalaMap }

object ExecutorSpec extends DefaultRunnableSpec {

  private val emptyItem                         = Item(ScalaMap.empty)
  private def someItem: Option[Item]            = Some(emptyItem)
  private def item(a: String): Item             = Item(ScalaMap(a -> AttributeValue.String(a)))
  private def someItem(a: String): Option[Item] = Some(item(a))

  private val primaryKey                                              = PrimaryKey(ScalaMap.empty)
  private val tableName1                                              = TableName("T1")
  private val tableName2                                              = TableName("T2")
  private val tableName3                                              = TableName("T3")
  private val indexName1                                              = IndexName("I1")
  private val getItem1                                                = GetItem(key = primaryKey, tableName = tableName1)
  private val getItem2                                                = GetItem(key = primaryKey, tableName = tableName2)
  private val getItem3                                                = GetItem(key = primaryKey, tableName = tableName3)
  private val zippedGets: DynamoDBQuery[(Option[Item], Option[Item])] = getItem1 zip getItem2

  private val putItem1    = PutItem(tableName = tableName1, item = Item(ScalaMap.empty))
  private val deleteItem1 = DeleteItem(tableName = tableName2, key = PrimaryKey(ScalaMap.empty))
  private val stream1     = ZStream(emptyItem)
  private val scan1       = Scan(tableName1, indexName1)

  override def spec = suite("Executor")(parallelizeSuite, executeSuite, experimentalSuite)

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

  /*
      override def execute[A](query: DynamoDBQuery[A]): ZIO[Any, Exception, A] = {
        val (constructors, assembler) = parallelize(query)

        for {
          chunks   <- ZIO.foreach(constructors)(dynamoDb.execute)
          assembled = assembler(chunks)
        } yield assembled
      }
   */
  val experimentalSuite = suite("random stuff")(
    test("explore GetItem batching2") {
      val zipped1: DynamoDBQuery[(Option[Item], Option[Item])] = getItem1 zip getItem2
      val batched: DynamoDBQuery[(Option[Item], Option[Item])] = DynamoDBExecutor.batchGetItems(zipped1)

      println(s"$batched")
      assertCompletes
//      assert(batched)(equalTo(BatchGetItem(ScalaMap.empty)))
    },
    test("explore GetItem batching") {
      val zipped  = getItem1 zip getItem2 zip getItem3 zip putItem1
      val wtf     = DynamoDBExecutor.batchGetItems(zipped)
      val equal   = zipped == wtf
      val zipped2 = putItem1 zip getItem1 zip getItem2 zip getItem3 zip putItem1
      val wtf2    = DynamoDBExecutor.batchGetItems(zipped2)
      val zipped3 = getItem1 zip getItem2 zip getItem3 zip putItem1 zip getItem1
      val wtf3    = DynamoDBExecutor.batchGetItems(zipped3)
      println(s"$equal $wtf2 $wtf3")
      assertCompletes
    } @@ TestAspect.ignore
  ).provideCustomLayer(DynamoDb.test >>> DynamoDBExecutor.live)

}
