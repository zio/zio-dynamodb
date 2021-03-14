package zio.dynamodb

import zio.{ Chunk, ZIO }
import zio.dynamodb.DynamoDBQuery.{
  parallelize,
  BatchGetItem,
  BatchWriteItem,
  Constructor,
  DeleteItem,
  GetItem,
  PutItem,
  Write,
  Zip
}
import zio.dynamodb.TestFixtures._
import zio.test.Assertion._
import zio.test.{ assert, assertCompletes, DefaultRunnableSpec, TestAspect }

import scala.collection.immutable.{ Map => ScalaMap }

object BatchingExperimentSpec extends DefaultRunnableSpec {

  override def spec = suite("Batch Experiment")(experimentalSuite)

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

//  private def batchGets(
//    tuple: (Chunk[Constructor[Any]], Chunk[Any] => Any)
//  ): (Chunk[Constructor[Any]], Chunk[Any] => Any) =
//    tuple match {
//      case (constructors, assembler) =>
//        type IndexedConstructor = (Constructor[Any], Int)
//        type IndexedGetItem     = (GetItem, Int)
//        // partition into gets/non gets
//        val (nonGets, gets) =
//          constructors.zipWithIndex.foldLeft[(Chunk[IndexedConstructor], Chunk[IndexedGetItem])](
//            (Chunk.empty, Chunk.empty)
//          ) {
//            case ((nonGets, gets), (y: GetItem, index)) => (nonGets, gets :+ ((y, index)))
//            case ((nonGets, gets), (y, index))          => (nonGets :+ ((y, index)), gets)
//          }
//      /*
//        add gets to BatchGetItem
//       */
//    }

  private def batchGets(
    constructors: Chunk[Constructor[Any]]
  ): (Chunk[(Constructor[Any], Int)], (BatchGetItem, Chunk[Int]), (BatchWriteItem, Chunk[Int])) = {
    type IndexedConstructor = (Constructor[Any], Int)
    type IndexedGetItem     = (GetItem, Int)
    type IndexedWriteItem   = (Write[Unit], Int)

    // partition into nonBatched/batched gets/batched writes
    val (nonBatched, gets, writes) =
      constructors.zipWithIndex.foldLeft[(Chunk[IndexedConstructor], Chunk[IndexedGetItem], Chunk[IndexedWriteItem])](
        (Chunk.empty, Chunk.empty, Chunk.empty)
      ) {
        case ((nonGets, gets, writes), (gi @ GetItem(_, _, _, _, _), index))       =>
          (nonGets, gets :+ ((gi, index)), writes)
        case ((nonGets, gets, writes), (pi @ PutItem(_, _, _, _, _, _), index))    =>
          (nonGets, gets, writes :+ ((pi, index)))
        case ((nonGets, gets, writes), (di @ DeleteItem(_, _, _, _, _, _), index)) =>
          (nonGets, gets, writes :+ ((di, index)))
        case ((nonGets, gets, writes), (nonGetItem, index))                        =>
          (nonGets :+ ((nonGetItem, index)), gets, writes)
      }

    val indexedBatchGetItem: (BatchGetItem, Chunk[Int]) = gets
      .foldLeft[(BatchGetItem, Chunk[Int])]((BatchGetItem(), Chunk.empty)) {
        case ((batchGetItem, indexes), (getItem, index)) => (batchGetItem + getItem, indexes :+ index)
      }

    val indexedBatchWrite: (BatchWriteItem, Chunk[Int]) = writes
      .foldLeft[(BatchWriteItem, Chunk[Int])]((BatchWriteItem(), Chunk.empty)) {
        case ((batchWriteItem, indexes), (writeItem, index)) => (batchWriteItem + writeItem, indexes :+ index)
      }

    println(s"nonGets=$nonBatched")
    println(s"indexedBatchGetItem=$indexedBatchGetItem")
    println(s"indexedBatchWrite=$indexedBatchWrite")

    (nonBatched, indexedBatchGetItem, indexedBatchWrite)
  }

  val experimentalSuite = suite("explore batching")(
    testM("batch GetItem's and PutItem's and DeleteItem's") {
      val (constructors, assembler)                                                                   =
        parallelize(putItem1 zip getItem1 zip getItem2 zip deleteItem1)
      val (indexedConstructors, (batchGetItem, batchGetIndexes), (batchWriteItem, batchWriteIndexes)) =
        batchGets(constructors)

      for {
        indexedNonGetResponses <- ZIO.foreach(indexedConstructors) {
                                    case (constructor, index) =>
                                      ddbExecute(constructor).map(result => (result, index))
                                  }
        indexedGetResponses    <-
          ddbExecute(batchGetItem).map(resp => batchGetItem.toGetItemResponses(resp) zip batchGetIndexes)
        indexedWriteResponses  <-
          // TODO: think about mapping return values from writes
          ddbExecute(batchWriteItem).map(_ => batchWriteItem.addList.map(_ => ()) zip batchWriteIndexes)
        combined                = (indexedNonGetResponses ++ indexedGetResponses ++ indexedWriteResponses).sortBy {
                                    case (_, index) => index
                                  }.map(_._1)
        _                       = println(s"combined=$combined")
        result                  = assembler(combined)
        expected                = ((((), None), None), ())
      } yield assert(result)(equalTo(expected))
    },
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
    } @@ TestAspect.ignore,
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
