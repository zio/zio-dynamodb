import sbt.CrossVersion

val zioVersion = "2.1.24"

ThisBuild / version               := "3.0.0-SNAPSHOT"
ThisBuild / organization          := "dev.zio"
ThisBuild / scalaVersion          := "2.13.18"
ThisBuild / sonatypeProfileName   := "dev.zio"
ThisBuild / homepage              := Some(url("https://github.com/zio/zio-dynamodb"))
ThisBuild / licenses              := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers            := List(
  Developer("jdegoes", "John De Goes", "john@degoes.net", url("http://degoes.net"))
)
ThisBuild / scmInfo               := Some(
  ScmInfo(
    url("https://github.com/zio/zio-dynamodb"),
    "scm:git:git@github.com:zio/zio-dynamodb.git"
  )
)

lazy val core = (project in file("core"))
  .settings(
    name               := "zio-dynamodb-core",
    crossScalaVersions := Seq("2.13.18", "3.3.7"),
    Compile / unmanagedSourceDirectories ++= {
      val base = (Compile / sourceDirectory).value
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3, _)) => Seq(base / "scala-3")
        case Some((2, _)) => Seq(base / "scala-2")
        case _            => Seq.empty
      }
    },
    Test / unmanagedSourceDirectories ++= {
      val base = (Test / sourceDirectory).value
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3, _)) => Seq(base / "scala-3")
        case Some((2, _)) => Seq(base / "scala-2")
        case _            => Seq.empty
      }
    },
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val root = (project in file("."))
  .aggregate(core)
  .settings(
    name           := "zio-dynamodb",
    publish / skip := true
  )
