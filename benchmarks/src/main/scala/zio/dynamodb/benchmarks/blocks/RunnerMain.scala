package zio.dynamodb.benchmarks.blocks

import ListOfRecordsDomain._

object RunnerMain extends App {
  val max: Long = 100000000L
  var i: Long   = 0L

  val person = Person(
    12345678901L,
    "John",
    30,
    "123 Main St"
//    map = Map("key1" -> 1, "key2" -> 2, "key3" -> 3)
    //          List(5, 7, 9)
    //          paymentMethod = PaymentMethod.CreditCard("John", 123)
  )

  while (i < max) {
    if (i % 10000000 == 0) {
      println(s"Encoded $i records")
      Thread.sleep(1)
    }
    zioBlocksCodec.encoder(person)
    i += 1
  }
}
