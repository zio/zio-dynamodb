ThisBuild / scalaVersion := "3.3.7"
ThisBuild / organization := "dev.zio"

val zioVersion        = "2.1.23"
val zioAwsVersion     = "7.39.6.4"
val zioSchemaVersion  = "1.7.5"
val zioBlocksVersion  = "0.0.27"
val zioPreludeVersion = "1.0.0-RC44"
val zioJsonVersion    = "0.7.45"

lazy val root = project
  .in(file("."))
  .settings(
    name := "examples-scala3",
    version := "0.1.0",
    scalacOptions ++= Seq(
      "-deprecation",
      "-unchecked",
      "-feature"
    ),
    resolvers ++= Resolver.sonatypeOssRepos("releases"),
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-blocks-schema"     % zioBlocksVersion,
      "dev.zio" %% "zio"          % zioVersion,
      "dev.zio" %% "zio-prelude"  % zioPreludeVersion,
      "dev.zio" %% "zio-streams"  % zioVersion,
      "dev.zio" %% "zio-test"     % zioVersion % "test",
      "dev.zio" %% "zio-test-sbt" % zioVersion % "test",
      "dev.zio" %% "zio-dynamodb" % "1.0.0-RC24"
    )
  )
