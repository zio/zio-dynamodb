package zio.dynamodb

/** How a table is charged: [[BillingMode.PayPerRequest]] or provisioned capacity from [[BillingMode.provisioned]]. */
sealed trait BillingMode

/** The billing modes of a table. */
object BillingMode {
  private[dynamodb] final case class Provisioned(pt: ProvisionedThroughput) extends BillingMode

  /** Pay-per-request billing (`PAY_PER_REQUEST`). No provisioned throughput is sent when the table is created. */
  case object PayPerRequest extends BillingMode

  /**
   * Returns the provisioned billing mode (`PROVISIONED`) with the given read and write capacity units, which are sent
   * when the table is created.
   */
  def provisioned(readCapacityUnit: Long, writeCapacityUnit: Long): Provisioned =
    Provisioned(ProvisionedThroughput(readCapacityUnit, writeCapacityUnit))
}
