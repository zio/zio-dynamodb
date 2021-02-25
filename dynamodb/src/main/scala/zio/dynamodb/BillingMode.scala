package zio.dynamodb

sealed trait BillingMode
object BillingMode {
  case object Provisioned   extends BillingMode
  case object PayPerRequest extends BillingMode
}
