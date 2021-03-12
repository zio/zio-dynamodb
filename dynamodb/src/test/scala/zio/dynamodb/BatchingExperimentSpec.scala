package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, Constructor, GetItem, Zip }
import zio.dynamodb.TestFixtures._
import zio.test.Assertion._
import zio.test.{ assert, assertCompletes, DefaultRunnableSpec, TestAspect }

import scala.collection.immutable.{ Map => ScalaMap }

object BatchingExperimentSpec extends DefaultRunnableSpec {

  override def spec = suite("Batch Experiment")(experimentalSuite)

  def batchGetItemResponseToGetItemResponses(
    batchQuery: BatchGetItem,
    batchResponse: BatchGetItem.Response
  ): Chunk[Option[Item]] = {
    /*
     for each key, check it exists in the response and create a corresponding Optional Item value
     */
    val keySet: Iterable[TableName] = batchQuery.requestItems.map.keys
    println(keySet)
    println(batchResponse)

    Chunk.empty
  }

  def batchAdjacentGetItems[A](query: DynamoDBQuery[A]): DynamoDBQuery[A] =
    query match {
      case Zip(left, right)                     =>
        (left, right) match {
          case (getItemLeft @ GetItem(_, _, _, _, _), getItemRight @ GetItem(_, _, _, _, _))         =>
            val batch = (BatchGetItem(MapOfSet.empty) + getItemLeft) + getItemRight
            batchAdjacentGetItems(batch.asInstanceOf[DynamoDBQuery[A]])
          case (Zip(x, getItemLeft @ GetItem(_, _, _, _, _)), getItemRight @ GetItem(_, _, _, _, _)) =>
            batchAdjacentGetItems(Zip(x, (BatchGetItem(MapOfSet.empty) + getItemRight) + getItemLeft))
          case (getItemLeft @ GetItem(_, _, _, _, _), batchRight @ BatchGetItem(_, _, _))            =>
            (batchRight + getItemLeft).asInstanceOf[DynamoDBQuery[A]]
          case (Zip(x, getItemLeft @ GetItem(_, _, _, _, _)), batchRight @ BatchGetItem(_, _, _))    =>
            batchAdjacentGetItems(Zip(x, batchRight + getItemLeft))
          case _                                                                                     =>
            Zip(batchAdjacentGetItems(left), batchAdjacentGetItems(right))
        }
      // create a MAP with function from BatchGetItem.Response => TupleN[Option[Item]]
      case batchGetItem @ BatchGetItem(_, _, _) =>
        // for now uses a hard coded mapping of BatchGetItem.Response to Tuple2[Option[Item]]
        // TODO: create real function from BatchGetItem.Response => TupleN[Option[Item]]
        DynamoDBQuery.Map(
          batchGetItem,
          (_: BatchGetItem.Response) => (Some(Item(ScalaMap.empty)), Some(Item(ScalaMap.empty))).asInstanceOf[A]
        )
      case other                                =>
        other
    }

  /*
      override def execute[A](query: DynamoDBQuery[A]): ZIO[Any, Exception, A] = {
        val (constructors, assembler) = parallelize(query)

        for {
          chunks   <- ZIO.foreach(constructors)(dynamoDb.execute)
          assembled = assembler(chunks)
        } yield assembled
      }
   */
  val experimentalSuite = suite("explore batching")(
    test("explore GetItem batching") {

      val (constructors, assembler)                 = DynamoDBQuery.parallelize(putItem1 zip getItem1 zip getItem2 zip deleteItem1)
      println(assembler)
      val x: (List[Constructor[Any]], BatchGetItem) =
        constructors.foldLeft((List.empty[Constructor[Any]], BatchGetItem(MapOfSet.empty))) {
          case ((xs, batch), constructor) =>
            constructor match {
              case getItem @ GetItem(_, _, _, _, _) => (xs, batch + getItem)
              case el                               => (xs :+ el, batch)
            }
        }
      println(x)
      assertCompletes
    },
    testM("explore getItem1 zip getItem2 zip putItem1") {
      val zipped1: DynamoDBQuery[((Option[Item], Option[Item]), Unit)] = getItem1 zip getItem2 zip putItem1
      val batched: DynamoDBQuery[((Option[Item], Option[Item]), Unit)] = batchAdjacentGetItems(zipped1)

      println(s"$batched")
      for {
        result  <- batched.execute
        expected = ((Some(Item(ScalaMap.empty)), Some(Item(ScalaMap.empty))), ())
      } yield (assert(result)(equalTo(expected)))
    } @@ TestAspect.ignore,
    testM("explore putItem1 zip getItem1 zip getItem2") {
      val zipped1: DynamoDBQuery[((Unit, Option[Item]), Option[Item])] = putItem1 zip getItem1 zip getItem2
      val batched: DynamoDBQuery[((Unit, Option[Item]), Option[Item])] = batchAdjacentGetItems(zipped1)

      println(s"$batched")
      for {
        result  <- batched.execute
        expected = (((), Some(Item(ScalaMap.empty))), Some(Item(ScalaMap.empty)))
        /*
ACTUAL
((),(Some(Item(Map())),Some(Item(Map()))))
EXPECTED
(((),Some(Item(Map()))),Some(Item(Map())))
         */
      } yield (assert(result)(equalTo(expected)))
    } @@ TestAspect.ignore,
    test("explore GetItem batching3") {
      val zipped                                                            = getItem1 zip getItem2 zip putItem1
      val wtf: DynamoDBQuery[((Option[Item], Option[Item]), Unit)]          = batchAdjacentGetItems(zipped)
      val equal                                                             = zipped == wtf
      val zipped2                                                           = putItem1 zip getItem1 zip getItem2 zip putItem1
      val wtf2: DynamoDBQuery[(((Unit, Option[Item]), Option[Item]), Unit)] = batchAdjacentGetItems(zipped2)
      val zipped3                                                           = getItem1 zip getItem2 zip putItem1 zip getItem1
      val wtf3                                                              = batchAdjacentGetItems(zipped3)
      println(s"$equal $wtf2 $wtf3")
      assertCompletes
    } @@ TestAspect.ignore
  ).provideCustomLayer(DynamoDBExecutor.test)

}
