package zio.dynamodb.examples.javasdk

sealed trait Payment
object Payment {
  final case object DebitCard  extends Payment
  final case object CreditCard extends Payment
  final case object PayPal     extends Payment
}
