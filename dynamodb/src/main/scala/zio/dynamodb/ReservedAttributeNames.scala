package zio.dynamodb

// TODO: Avi - rename
private[dynamodb] object ReservedAttributeNames {
  val Prefix: String    = "~~~~~~~~~~~~"
  val boundaryCharRegex = "[\\.|\\[|\\)|\\,]$".r
  private val pathRegex = s"($Prefix\\S+\\,|$Prefix\\S+\\.|$Prefix\\S+\\[|$Prefix\\S+\\)|$Prefix\\S+)".r

  def escape(pathSegment: String): String = s"$Prefix$pathSegment"

  // returns Map of substituted name to actual name, and substituted expression
  def parse(escapedExpression: String): (Map[String, String], String) = {
    def trimBoundaryChars(s: String) = boundaryCharRegex.replaceFirstIn(s, "")

    val targetsToEscape                              = pathRegex.findAllIn(escapedExpression).map(trimBoundaryChars(_))
    val replacements: List[(String, String, String)] = targetsToEscape.foldLeft(List.empty[(String, String, String)]) {
      case (acc, s) =>
        val replaced = s.replace(Prefix, "")
        acc :+ ((s"N_$replaced", replaced, s))
    }

    val escaped = replacements.foldLeft(escapedExpression) {
      case (acc, (sub, _, s)) =>
        val x = acc.replace(s, "#" + sub)
        x
    }
    val map     = replacements.map {
      case (sub, rep, _) =>
        ("#" + sub, rep)
    }.toMap

    val x = (map, escaped)
    x
  }
}
