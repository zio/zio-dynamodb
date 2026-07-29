package zio.dynamodb

sealed trait BillingMode
object BillingMode {
  private[dynamodb] final case class Provisioned(pt: ProvisionedThroughput) extends BillingMode
  case object PayPerRequest                                                 extends BillingMode

  def provisioned(readCapacityUnit: Long, writeCapacityUnit: Long): Provisioned =
    Provisioned(ProvisionedThroughput(readCapacityUnit, writeCapacityUnit))
}
