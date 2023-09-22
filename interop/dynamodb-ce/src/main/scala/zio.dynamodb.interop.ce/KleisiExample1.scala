import cats.data.Kleisli
import cats.effect.{ IO, Sync }
import scala.util.{ Success, Try }

/*
FROM: https://medium.com/@supermanue/real-world-applications-of-scala-kleisli-dependency-injection-36ef589ee77b
 */
object KleisliExample1 {

  case class Shipment(ref: String, status: String)

  trait ShipmentStorage[F[_]] {
    def retrieveShipment(shipmentReference: String): F[Shipment]
  }

  class ShipmentStorageImpl1 extends ShipmentStorage[IO] {
    override def retrieveShipment(shipmentReference: String): IO[Shipment] = IO.pure(Shipment(shipmentReference, "OK"))
  }

  class ShipmentStorageImpl2[F[_]: Sync] extends ShipmentStorage[F] {
    override def retrieveShipment(shipmentReference: String): F[Shipment] =
      Sync[F].delay(Shipment(shipmentReference, "OK"))
  }

  class ShipmentStorageImpl3 extends ShipmentStorage[Try] {
    override def retrieveShipment(shipmentReference: String): Try[Shipment] = Success(Shipment(shipmentReference, "OK"))
  }

  type Operation[F[_], R] = Kleisli[F, ShipmentStorage[F], R]

  object OperationService {

    def getShipment[F[_]](shipmentReference: String): Operation[F, Shipment] =
      Kleisli { shipmentStorage: ShipmentStorage[F] =>
        //log stuff before accessing
        shipmentStorage.retrieveShipment(shipmentReference)
        //process result after accessing
      }

    def storeShipment[F[_]](shipmentReference: String): Operation[F, Shipment] = ???

    def deleteShipment[F[_]](shipmentReference: String): Operation[F, Shipment] = ???

  }

  val shipmentToProcess = "1234"
  val storage1          = new ShipmentStorageImpl1()
//  val res               = OperationService.getShipment(shipmentToProcess)(storage1).unsafeRunSync()
  //res: Shipment(1234, OK)

  val storage2 = new ShipmentStorageImpl2[IO]()
//  val res2     = OperationService.getShipment(shipmentToProcess).run(storage2).unsafeRunSync()
  //res: Shipment(1234, OK)

  val storage3 = new ShipmentStorageImpl3()
  val res3     = OperationService.getShipment(shipmentToProcess)(storage3)
  //res: Success(Shipment(1234, OK))

}
