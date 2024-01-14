package zio.dynamodb

import zio.test.ZIOSpecDefault
import zio.test.assertTrue
import scala.util.control.NoStackTrace
import zio.Chunk
import zio.ZIO

//import zio.dynamodb.Model._

object UnifiedErrorSpec extends ZIOSpecDefault {

  override def spec =
    suite("UnifiedErrorSpec")(
      test("experiment") {
        assertTrue(true)
      }
    )

}

trait Foo {
  def get(i: Int): ZIO[Any, DynamoDBError2, Int]

  def decode(): Either[DynamoDBError2, Int]

  def execute(i: Int): ZIO[Any, Throwable, Either[DynamoDBError2, Int]]
}

sealed trait DynamoDBError2 extends Throwable with NoStackTrace with Product with Serializable {
  def message: String
  override def getMessage(): String = message
}

// TODO:
/*

 */

object DynamoDBError2 {
  // TODO: remove and replace with Option in API
  final case class ValueNotFound(message: String) extends DynamoDBError2

  // this is the only remaining ITEM level error - and I think we can push it to the ERROR CHANNEL
  final case class DecodingError(message: String) extends DynamoDBError2
}

sealed trait DynamoDBBatchError2 extends Throwable with NoStackTrace with Product with Serializable {
  def message: String
  override def getMessage(): String = message
}

object DynamoDBBatchError2 {
  sealed trait Write                       extends Product with Serializable
  final case class Delete(key: PrimaryKey) extends Write
  final case class Put(item: Item)         extends Write

  final case class BatchWriteError(unprocessedItems: Map[String, Chunk[Write]]) extends DynamoDBBatchError2 {
    val message = "BatchWriteError: unprocessed items returned by aws"
  }

  final case class BatchGetError(unprocessedKeys: Map[String, Set[PrimaryKey]]) extends DynamoDBBatchError2 {
    val message = "BatchGetError: unprocessed keys returned by aws"
  }
}

object Experiment {

  /*
   aim is to have a unified error channel for all operations

   could defaulting to Option reduce the number of errors we need to handle? and hance make unification easier?
   would we lose information?

   changing get return type to Option (ie ditching Either) would mean we no longer have to "absolve"

   // current
   def get[From: Schema]: DynamoDBQuery[From, Either[DynamoDBError, From]] =
   def execute: ZIO[DynamoDBExecutor, Throwable, Out]

   // desired
   def get[From: Schema]: DynamoDBQuery[From, Option[From]] =
   def execute: ZIO[DynamoDBExecutor, DynamoDBError, Out]

   */

  // --------------------------------------
  // favour not found as a value
  // --------------------------------------
  sealed trait XError                               extends Throwable
  sealed trait ItemError                            extends Throwable with XError
  final case class DecodingError(message: String)   extends ItemError
  final case class NotFound(message: String)        extends ItemError
  sealed trait ItemsError                           extends Throwable with XError
  final case class BatchWriteError(message: String) extends ItemsError

  implicit class XErrorOps[A](zio: ZIO[Any, XError, A]) {
    def mapNotFound(f: NotFound => Throwable): ZIO[Any, Throwable, A] =
      zio.mapError {
        case e: NotFound => f(e)
        case e           => e
      }

  }

  // (1) BREAKTHROUGH!!!!! eliminated Either from API
  val get: ZIO[Any, XError, Int]            = ??? // we would need NotFound sum type instance for this
  val getOpt: ZIO[Any, XError, Option[Int]] = ???

  // (2) need to decide if forcing user to handle Batch errors is a good idea
  // (3) discomfort could be offset by having syntax methods to do the common stuff

  // user would only have to dig into error type, BUT ONLY for REAL FUCKING ERRORS!!!!!!!
  val e1 = get.mapError {
    case _: ItemError  => ()
    case _: ItemsError => ()
  }

  val e2: ZIO[Any, XError, Int] = get.catchSome {
    case _: Experiment.BatchWriteError => ZIO.succeed(10)
  }

  val y: ZIO[Any, XError, Int]          = getOpt.someOrFail(NotFound(""))
  // going from A + error channel to Option[A] loses information
  val z: ZIO[Any, Nothing, Option[Int]] = y.option
}
