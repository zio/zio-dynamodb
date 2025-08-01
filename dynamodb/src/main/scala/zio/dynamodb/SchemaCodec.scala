package zio.dynamodb

import zio.dynamodb.blocks.{ BlocksCodec }
import zio.schema.Schema
import zio.Chunk

// Captures schema based codec capabilities
trait SchemaCodec[A] {
  def encoder: Encoder[A]
  def decoder: Decoder[A]
  def projectionsFromSchema: Chunk[ProjectionExpression[_, _]]
}

object SchemaCodec {
  def apply[A](implicit ev: SchemaCodec[A]): SchemaCodec[A] = ev

  // ZIO Schema V1
  implicit def schema1ToSchemaCodec[A: Schema]: SchemaCodec[A] =
    new SchemaCodec[A] {
      override def encoder: Encoder[A] = Codec.encoder(Schema[A])
      override def decoder: Decoder[A] = Codec.decoder(Schema[A])

      def projectionsFromSchema: Chunk[ProjectionExpression[_, _]] =
        Schema[A] match {
          case r: Schema.Record[A] =>
            r.fields.map { f =>
              ProjectionExpression.MapElement(ProjectionExpression.Root, f.name)
            }
          case _                   => Chunk.empty
        }

    }

  // ZIO Schema V2 (from Blocks)
  implicit def schema2ToSchemaCodec[A: zio.blocks.schema.Schema]: SchemaCodec[A] =
    new SchemaCodec[A] {
      override def encoder: Encoder[A] = BlocksCodec.encoder(zio.blocks.schema.Schema[A])
      override def decoder: Decoder[A] = BlocksCodec.decoder(zio.blocks.schema.Schema[A])

      override def projectionsFromSchema: Chunk[ProjectionExpression[_, _]] = {
        def projections[A](reflect: zio.blocks.schema.Reflect.Bound[A]): Chunk[ProjectionExpression[_, _]] =
          reflect match {
            case zio.blocks.schema.Reflect.Record(fields, _, _, _, modifiers) =>
              Chunk.fromIterable(fields.map { f =>
                ProjectionExpression.MapElement(ProjectionExpression.Root, f.name)
              })
            case _                                                            => Chunk.empty
          }

        projections(zio.blocks.schema.Schema[A].reflect)
      }
    }

}
