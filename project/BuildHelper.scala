import sbt._
import sbt.Keys._
import sbtbuildinfo._
import BuildInfoKeys._

object BuildHelper {
  // Align with zio-schema since we have a deep dependency on it
  val Scala212                = "2.12.18"
  val Scala213                = "2.13.12"
  val Scala3                  = "3.3.0"
  private val SilencerVersion = "1.7.16"

  private val stdOptions = Seq(
    "-encoding",
    "UTF-8",
    "-explaintypes",
    "-feature",
    "-language:higherKinds",
    "-language:existentials",
    "-unchecked",
    "-deprecation",
    "-Xfatal-warnings"
  )

  private val stdOpts2 = Seq(
    "-language:higherKinds",
    "-explaintypes",
    "-Yrangepos",
    "-Xlint:_,-missing-interpolator,-type-parameter-shadow,-infer-any",
    "-Ypatmat-exhaust-depth",
    "40",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Xsource:3.0"
  )

  private val stdOpts213 = Seq(
    "-opt-warnings",
    "-Ywarn-extra-implicit",
    "-Ywarn-unused",
    "-Ymacro-annotations",
    "-Ywarn-macros:after"
  )

  private val stdOptsUpto212 = Seq(
    "-Ypartial-unification",
    "-opt-warnings",
    "-Ywarn-extra-implicit",
    "-Yno-adapted-args",
    "-Ywarn-inaccessible",
    "-Ywarn-nullary-override",
    "-Ywarn-nullary-unit",
    "-Wconf:cat=unused-nowarn:s"
  )

  private def extraOptions(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((3, 3))  =>
        List(
          "-language:implicitConversions",
          "-Xignore-scala2-macros"
        )
      case Some((3, 2))  =>
        List(
          "-language:implicitConversions",
          "-Xignore-scala2-macros"
        )
      case Some((2, 13)) =>
        stdOpts213 ++ stdOpts2
      case Some((2, 12)) =>
        Seq(
          "-opt-warnings",
          "-Ywarn-extra-implicit",
          "-Ywarn-unused:_,imports",
          "-Ywarn-unused:imports",
          "-opt:l:inline",
          "-opt-inline-from:<source>"
        ) ++ stdOptsUpto212 ++ stdOpts2
      case _             =>
        Seq("-Xexperimental") ++ stdOptsUpto212 ++ stdOpts2
    }

  def buildInfoSettings(packageName: String) =
    Seq(
      buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, isSnapshot),
      buildInfoPackage := packageName,
      buildInfoObject := "BuildInfo"
    )

  def stdSettings(prjName: String) =
    Seq(
      name := s"$prjName",
      crossScalaVersions := Seq(Scala212, Scala213, Scala3),
      ThisBuild / scalaVersion := Scala213,
      scalacOptions := stdOptions ++ extraOptions(scalaVersion.value),
      libraryDependencies ++= {
        if (scalaVersion.value == Scala3) Seq()
        else
          Seq(
            ("com.github.ghik"                % "silencer-lib"    % SilencerVersion % Provided)
              .cross(CrossVersion.full),
            compilerPlugin(("com.github.ghik" % "silencer-plugin" % SilencerVersion).cross(CrossVersion.full)),
            compilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")
          )
      },
      incOptions ~= (_.withLogRecompileOnMacro(false))
    )
}
