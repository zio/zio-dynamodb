package zio.dynamodb

import zio.Chunk
import zio.schema.annotation.{ caseName, discriminatorName }

object Annotations {
  final case class enumOfCaseObjects() extends scala.annotation.Annotation

  def maybeCaseName(annotations: Chunk[Any]): Option[String]      =
    annotations.collect { case caseName(name) => name }.headOption

  def maybeDiscriminator(annotations: Chunk[Any]): Option[String] =
    annotations.collect { case discriminatorName(name) => name }.headOption
}
