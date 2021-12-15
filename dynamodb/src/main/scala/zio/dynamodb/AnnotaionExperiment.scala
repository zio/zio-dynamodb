package zio.dynamodb

object AnnotaionExperiment {
  final case class discriminator(name: String) extends scala.annotation.Annotation
  final class sumTypePolicy                    extends scala.annotation.Annotation
}
