package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, Constructor, GetItem, Zip }
import zio.dynamodb.TestFixtures._
import zio.test.{ assertCompletes, DefaultRunnableSpec, TestAspect }

object BatchingExperimentSpec extends DefaultRunnableSpec {

  override def spec = suite("Executor")(experimentalSuite)

  def batchGetItems[A](query: DynamoDBQuery[A]): DynamoDBQuery[A] =
    query match {
      case Zip(left, right) =>
        (left, right) match {
          case (getItemLeft @ GetItem(_, _, _, _, _), getItemRight @ GetItem(_, _, _, _, _))         =>
            val batch = (BatchGetItem(MapOfSet.empty) + getItemLeft) + getItemRight
            batchGetItems(batch.asInstanceOf[DynamoDBQuery[A]])
          case (Zip(x, getItemLeft @ GetItem(_, _, _, _, _)), getItemRight @ GetItem(_, _, _, _, _)) =>
            batchGetItems(Zip(x, (BatchGetItem(MapOfSet.empty) + getItemRight) + getItemLeft))
          case (getItemLeft @ GetItem(_, _, _, _, _), batchRight @ BatchGetItem(_, _))               =>
            (batchRight + getItemLeft).asInstanceOf[DynamoDBQuery[A]]
          case (Zip(x, getItemLeft @ GetItem(_, _, _, _, _)), batchRight @ BatchGetItem(_, _))       =>
            batchGetItems(Zip(x, batchRight + getItemLeft))
          case _                                                                                     =>
            Zip(batchGetItems(left), batchGetItems(right))
        }
      // TODO: create a MAP with function from BatchGetItem.Response => TupleN[Option[Item]]
      case other            =>
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

      val constructors = Chunk(getItem1, getItem2, putItem1)
      val x            = constructors.foldLeft((List.empty[Constructor[Any]], BatchGetItem(MapOfSet.empty))) {
        case ((xs, batch), constructor) =>
          constructor match {
            case getItem @ GetItem(_, _, _, _, _) => (xs, batch + getItem)
            case el                               => (xs :+ el, batch)
          }
      }
      println(x)
      assertCompletes
    },
    test("explore GetItem batching2") {
      val zipped1: DynamoDBQuery[(Option[Item], Option[Item])] = getItem1 zip getItem2
      val batched: DynamoDBQuery[(Option[Item], Option[Item])] = batchGetItems(zipped1)

      println(s"$batched")
      assertCompletes
//      assert(batched)(equalTo(BatchGetItem(ScalaMap.empty)))
    },
    test("explore GetItem batching3") {
      val zipped  = getItem1 zip getItem2 zip getItem3 zip putItem1
      val wtf     = batchGetItems(zipped)
      val equal   = zipped == wtf
      val zipped2 = putItem1 zip getItem1 zip getItem2 zip getItem3 zip putItem1
      val wtf2    = batchGetItems(zipped2)
      val zipped3 = getItem1 zip getItem2 zip getItem3 zip putItem1 zip getItem1
      val wtf3    = batchGetItems(zipped3)
      println(s"$equal $wtf2 $wtf3")
      assertCompletes
    } @@ TestAspect.ignore
  ).provideCustomLayer(DynamoDBExecutor.test)

}
