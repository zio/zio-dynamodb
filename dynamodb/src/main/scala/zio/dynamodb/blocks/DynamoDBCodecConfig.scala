package zio.dynamodb.blocks

trait DynamoDBCodecConfig[A] {
  def configure(d: DynamoDBCodecDeriver): DynamoDBCodecDeriver
}

object DynamoDBCodecConfig {
  def identity[A]: DynamoDBCodecConfig[A] = (d: DynamoDBCodecDeriver) => d

  implicit def default[A]: DynamoDBCodecConfig[A] = identity
}
