package zio.dynamodb.blocks

import zio.blocks.schema.derive.DerivationBuilder

trait DerivationBuilderConfigure[A] {
  def configure(d: DerivationBuilder[DynamoDBCodec, A]): DerivationBuilder[DynamoDBCodec, A]
}

object DerivationBuilderConfigure {
  def identity[A]: DerivationBuilderConfigure[A] =
    (d: DerivationBuilder[DynamoDBCodec, A]) => d

  implicit def default[A]: DerivationBuilderConfigure[A] = identity
}
