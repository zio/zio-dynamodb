package zio.dynamodb

sealed trait BillingMode
object BillingMode {
  private[dynamodb] final case class Provisioned(pt: ProvisionedThroughput) extends BillingMode
  case object PayPerRequest                                                 extends BillingMode

  def provisioned(readCapacityUnit: Int, writeCapacityUnit: Int): Provisioned =
    Provisioned(ProvisionedThroughput(readCapacityUnit, writeCapacityUnit))
}
