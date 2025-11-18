package zio.dynamodb.benchmarks.blocks

import ListOfRecordsDomain._

object RunnerMain extends App {
   val max: Long = 1000000000
   var i: Long = 0L

  val person = Person(
    12345678901L,
    "John",
    30,
    "123 Main St"
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
