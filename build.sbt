import sbt.CrossVersion
import zio.sbt.WebsitePlugin.autoImport._

addCommandAlias("lint", "; scalafmtSbtCheck; scalafmtCheckAll; headerCheckAll")
addCommandAlias("fmt", "; scalafmtSbt; scalafmtAll; headerCreateAll")

val zioVersion        = "2.1.24"
val zioPreludeVersion = "1.0.0-RC47"
val zioBlocksVersion  = "0.0.47+16-7ff60266-SNAPSHOT"
val awsSdkVersion     = "2.26.31"
val scala213Version   = "2.13.18"
val scala3Version     = "3.3.8"

ThisBuild / version             := "3.0.0-SNAPSHOT"
ThisBuild / organization        := "dev.zio"
ThisBuild / scalaVersion        := scala213Version
ThisBuild / sonatypeProfileName := "dev.zio"
ThisBuild / homepage            := Some(url("https://github.com/zio/zio-dynamodb"))
ThisBuild / licenses            := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers          := List(
  Developer("jdegoes", "John De Goes", "john@degoes.net", url("http://degoes.net"))
)
ThisBuild / scmInfo             := Some(
  ScmInfo(
    url("https://github.com/zio/zio-dynamodb"),
    "scm:git:git@github.com:zio/zio-dynamodb.git"
  )
)
ThisBuild / headerLicense       := Some(HeaderLicense.ALv2("2021-2026", "John A. De Goes and the ZIO Contributors"))
// zio-blocks-schema (used by core for zio.blocks.chunk.Chunk) is not yet on Maven Central,
// only published as a snapshot — see https://github.com/zio/zio-blocks.
ThisBuild / resolvers += "Sonatype Central Snapshots" at "https://central.sonatype.com/repository/maven-snapshots"

lazy val core = (project in file("core"))
  .settings(
    name               := "zio-dynamodb-core",
    crossScalaVersions := Seq(scala213Version, scala3Version),
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
      "dev.zio" %% "zio-blocks-chunk" % zioBlocksVersion,
      "dev.zio" %% "zio-test"         % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt"     % zioVersion % Test
    ),
    testFrameworks     := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val aws = (project in file("aws"))
  .dependsOn(core)
  .settings(
    name               := "zio-dynamodb-aws",
    crossScalaVersions := Seq(scala213Version, scala3Version),
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "dynamodb"     % awsSdkVersion,
      "dev.zio"               %% "zio-test"     % zioVersion % Test,
      "dev.zio"               %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks     := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val schemaDynamodb = (project in file("schema-dynamodb"))
  .dependsOn(core)
  .settings(
    name               := "zio-dynamodb-blocks-schema",
    crossScalaVersions := Seq(scala213Version, scala3Version),
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
      "dev.zio" %% "zio-blocks-chunk"  % zioBlocksVersion,
      "dev.zio" %% "zio-blocks-schema" % zioBlocksVersion,
      "dev.zio" %% "zio-test"          % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt"      % zioVersion % Test
    ),
    testFrameworks     := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val zioInterpreter = (project in file("zio"))
  .dependsOn(aws)
  .settings(
    name               := "zio-dynamodb-zio",
    crossScalaVersions := Seq(scala213Version, scala3Version),
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"          % zioVersion,
      "dev.zio" %% "zio-streams"  % zioVersion % Test,
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks     := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val schemaDdbExpr = (project in file("schema-ddbexpr"))
  .dependsOn(schemaDynamodb)
  .settings(
    name               := "zio-dynamodb-schema-ddbexpr",
    crossScalaVersions := Seq(scala213Version, scala3Version),
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"     % zioVersion        % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion        % Test,
      "dev.zio" %% "zio-prelude"  % zioPreludeVersion % Test
    ),
    testFrameworks     := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val futureInterpreter = (project in file("future"))
  .dependsOn(aws)
  .settings(
    name               := "zio-dynamodb-future",
    crossScalaVersions := Seq(scala213Version, scala3Version),
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks     := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val docs = project
  .in(file("zio-dynamodb-docs"))
  .settings(
    name                                       := "zio-dynamodb-docs",
    moduleName                                 := "zio-dynamodb-docs",
    // The 2.x branch's content lives at top-level docs/, not the project's own
    // basedir (zio-dynamodb-docs/src/main/mdoc) — mirrored explicitly here rather
    // than relying on mdoc's implicit default, which doesn't match that layout.
    mdocIn                                     := (ThisBuild / baseDirectory).value / "docs",
    projectName                                := "ZIO DynamoDB",
    mainModuleName                             := (core / moduleName).value,
    projectStage                               := ProjectStage.Experimental,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(
      core,
      aws,
      schemaDynamodb,
      zioInterpreter,
      schemaDdbExpr,
      futureInterpreter
    ),
    publish / skip                             := true
  )
  .dependsOn(core, aws, schemaDynamodb, zioInterpreter, schemaDdbExpr, futureInterpreter)
  .enablePlugins(WebsitePlugin)

lazy val root = (project in file("."))
  .aggregate(core, aws, schemaDynamodb, zioInterpreter, schemaDdbExpr, futureInterpreter, docs)
  .enablePlugins(zio.sbt.ZioSbtCiPlugin)
  .settings(
    name              := "zio-dynamodb",
    publish / skip    := true,
    ciEnabledBranches := Seq("series/3.x")
  )
