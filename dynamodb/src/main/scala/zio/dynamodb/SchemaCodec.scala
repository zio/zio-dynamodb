package zio.dynamodb

import zio.dynamodb.blocks.{ BlocksCodec }
import zio.schema.Schema

trait SchemaCodec[A] {
  def encoder: Encoder[A]
}

object SchemaCodec {
  def apply[A](implicit ev: SchemaCodec[A]): SchemaCodec[A] = ev

  // ZIO Schema V1
  implicit def schema1ToSchemaCodec[A: Schema]: SchemaCodec[A] =
    new SchemaCodec[A] {
      override def encoder: Encoder[A] = Codec.encoder(Schema[A])
    }

  // ZIO Schema V2 (from Blocks)
  implicit def schema2ToSchemaCodec[A: zio.blocks.schema.Schema]: SchemaCodec[A] =
    new SchemaCodec[A] {
      override def encoder: Encoder[A] = BlocksCodec.encoder(zio.blocks.schema.Schema[A])
    }

}
