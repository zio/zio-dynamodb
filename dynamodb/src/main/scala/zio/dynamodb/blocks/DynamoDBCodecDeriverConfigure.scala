package zio.dynamodb.blocks

trait DynamoDBCodecDeriverConfigure[+A] {
  def configure(d: DynamoDBCodecDeriver): DynamoDBCodecDeriver
}

object DynamoDBCodecDeriverConfigure {
  def identity[A]: DynamoDBCodecDeriverConfigure[A] = (d: DynamoDBCodecDeriver) => d

  implicit def default[A]: DynamoDBCodecDeriverConfigure[A] = identity
}
