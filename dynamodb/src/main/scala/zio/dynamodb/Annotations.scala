package zio.dynamodb

object Annotations {
  final case class discriminator(name: String)   extends scala.annotation.Annotation
  final case class enumNameAsValue()             extends scala.annotation.Annotation
  final case class constantValue2(value: String) extends scala.annotation.Annotation
}
