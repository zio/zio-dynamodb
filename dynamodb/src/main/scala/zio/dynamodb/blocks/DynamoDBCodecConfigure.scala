package zio.dynamodb.blocks

trait DynamoDBCodecConfigure[+A] {
  def configure(d: DynamoDBCodecDeriver): DynamoDBCodecDeriver
}

object DynamoDBCodecConfigure {
  def identity[A]: DynamoDBCodecConfigure[A] = (d: DynamoDBCodecDeriver) => d

  implicit def default[A]: DynamoDBCodecConfigure[A] = identity
}
