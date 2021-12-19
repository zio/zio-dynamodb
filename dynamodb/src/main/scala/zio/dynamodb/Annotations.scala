package zio.dynamodb

object Annotations {
  final case class discriminator(name: String) extends scala.annotation.Annotation
  final case class constantValue()             extends scala.annotation.Annotation
}
