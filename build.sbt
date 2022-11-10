import BuildHelper._

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.dev/zio-dynamodb/")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "jdegoes",
        "John De Goes",
        "john@degoes.net",
        url("http://degoes.net")
      ),
      Developer(
        "googley42",
        "Avinder Bahra",
        "avinder.bahra@gmail.com",
        url("https://github.com/googley42")
      )
    ),
    pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    scmInfo := Some(
      ScmInfo(url("https://github.com/googley42/zio-dynamodb"), "scm:git:git@github.com:googley42/zio-dynamodb.git")
    )
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

// from this blog http://softwarebyjosh.com/2018/03/25/how-to-unit-test-your-dynamodb-queries.html
lazy val copyJars = taskKey[Unit]("copyJars")

def copyJarSetting(dir: String) =
  Seq(
    copyJars := {
      import java.nio.file.Files
      import java.io.File
      // For Local Dynamo DB to work, we need to copy SQLLite native libs from
      // our test dependencies into a directory that Java can find ("lib" in this case)
      // Then in our Java/Scala program, we need to set System.setProperty("sqlite4java.library.path", "lib");
      // before attempting to instantiate a DynamoDBEmbedded instance
      val artifactTypes = Set("dylib", "so", "dll")
      val files         = Classpaths.managedJars(sbt.Test, artifactTypes, update.value).files
      Files.createDirectories(new File(s"$dir/native-libs").toPath)
      files.foreach { f =>
        val fileToCopy = new File(s"$dir/native-libs", f.name)
        if (!fileToCopy.exists())
          Files.copy(f.toPath, fileToCopy.toPath)
      }
    }
  )

val zioVersion       = "1.0.13"
val zioConfigVersion = "1.0.6"
val zioAwsVersion    = "3.17.87.2"

lazy val root =
  project
    .in(file("."))
    .settings(skip in publish := true)
    .aggregate(zioDynamodb, examples)

lazy val zioDynamodb = module("zio-dynamodb", "dynamodb")
  .enablePlugins(BuildInfoPlugin)
  .settings(buildInfoSettings("zio.dynamodb"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    resolvers += "DynamoDB Local Release Repository" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release",
    resolvers += Resolver.sonatypeRepo("releases"),
    copyJarSetting("dynamodb"),
    (Compile / compile) := (Compile / compile).dependsOn(copyJars).value,
    libraryDependencies ++= Seq(
      "dev.zio"               %% "zio"                   % zioVersion,
      "dev.zio"               %% "zio-streams"           % zioVersion,
      "dev.zio"               %% "zio-test"              % zioVersion % "it,test",
      "dev.zio"               %% "zio-test-sbt"          % zioVersion % "it,test",
      "dev.zio"               %% "zio-schema"            % "0.1.9",
      "dev.zio"               %% "zio-schema-derivation" % "0.1.9",
      "io.github.vigoo"       %% "zio-aws-http4s"        % zioAwsVersion,
      "io.github.vigoo"       %% "zio-aws-dynamodb"      % zioAwsVersion,
      "org.scala-lang"         % "scala-reflect"         % scalaVersion.value,
      "software.amazon.awssdk" % "dynamodb"              % "2.16.20",
      "com.amazonaws"          % "DynamoDBLocal"         % "1.17.0"   % "it,test"
    ),
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework")),
    Compile / sourceGenerators += Def.task {
      val dir                      = (Compile / sourceManaged).value
      val file                     = dir / "zio" / "dynamodb" / "GeneratedAttrMapApplies.scala"
      def upperAlpha(i: Int): Char = (('A'.toInt - 1) + i).toChar
      def lowerAlpha(i: Int): Char = (('a'.toInt - 1) + i).toChar
      val applyMethods             = (1 to 22).map {
        i =>
          val types     = (1 to i).map(upperAlpha).mkString(", ")
          val tparams   = (1 to i).map(p => s"t$p: (String, ${upperAlpha(p)})").mkString(", ")
          val implicits = (1 to i).map(p => s"${lowerAlpha(p)}: ToAttributeValue[${upperAlpha(p)}]").mkString(", ")
          val tentries  = (1 to i).map(p => s"t$p._1 -> ${lowerAlpha(p)}.toAttributeValue(t$p._2)").mkString(", ")

          s"""def apply[$types]($tparams)(implicit $implicits): AttrMap =
             |    AttrMap(
             |      Map($tentries)
             |    )""".stripMargin
      }
      IO.write(
        file,
        s"""package zio.dynamodb
           |
           |private[dynamodb] trait GeneratedAttrMapApplies {
           |  
           |  ${applyMethods.mkString("\n\n  ")}
           |}""".stripMargin
      )
      Seq(file)
    }.taskValue,
    Compile / sourceGenerators += Def.task {
      val dir                      = (Compile / sourceManaged).value
      val file                     = dir / "zio" / "dynamodb" / "Zippable.scala"
      def upperAlpha(i: Int): Char = (('A'.toInt - 1) + i).toChar
      val implicitZippables        = (3 to 22).map {
        i =>
          val types      = (1 until i).map(upperAlpha).mkString(", ")
          val leftTuples = (1 until i).map(i => s"left._$i").mkString(", ")

          s"""implicit def Zippable$i[$types, Z]: Zippable.Out[($types), Z, ($types, Z)] =
             |    new Zippable[($types), Z] {
             |      type Out = ($types, Z)
             |
             |      def zip(left: ($types), right: Z): Out = ($leftTuples, right)
             |    }
             |""".stripMargin
      }
      IO.write(
        file,
        s"""package zio.dynamodb
           |
           |sealed trait Zippable[-A, -B] {
           |  type Out
           |
           |  def zip(left: A, right: B): Out
           |}
           |object Zippable extends ZippableLowPriority1 {
           |  type Out[-A, -B, C] = Zippable[A, B] { type Out = C }
           |
           |  implicit def ZippableUnit[A]: Zippable.Out[A, Unit, A] =
           |    new Zippable[A, Unit] {
           |      type Out = A
           |
           |      def zip(left: A, right: Unit): Out = left
           |    }
           |
           |}
           |trait ZippableLowPriority1 extends ZippableLowPriority2 {
           |  ${implicitZippables.mkString("\n  ")}
           |
           |}
           |trait ZippableLowPriority2 extends ZippableLowPriority3 {
           |
           |  implicit def Zippable2Right[B]: Zippable.Out[Unit, B, B] =
           |    new Zippable[Unit, B] {
           |      type Out = B
           |
           |      def zip(left: Unit, right: B): Out = right
           |    }
           |}
           |trait ZippableLowPriority3 extends ZippableLowPriority4 {
           |  implicit def Zippable2Left[A]: Zippable.Out[A, Unit, A] =
           |    new Zippable[A, Unit] {
           |      type Out = A
           |
           |      def zip(left: A, right: Unit): Out = left
           |    }
           |
           |}
           |
           |trait ZippableLowPriority4 {
           |  implicit def Zippable2[A, B]: Zippable.Out[A, B, (A, B)] =
           |    new Zippable[A, B] {
           |      type Out = (A, B)
           |
           |      def zip(left: A, right: B): Out = (left, right)
           |    }
           |}""".stripMargin
      )
      Seq(file)
    }.taskValue,
    Compile / sourceGenerators += Def.task {
      val dir                      = (Compile / sourceManaged).value
      val file                     = dir / "zio" / "dynamodb" / "GeneratedFromAttributeValueAs.scala"
      def upperAlpha(i: Int): Char = (('A'.toInt - 1) + i).toChar
      def lowerAlpha(i: Int): Char = (('a'.toInt - 1) + i).toChar
      val applyMethods             = (2 to 22).map {
        i =>
          val returnType = upperAlpha(i + 1)
          val tparams    = (1 to i).map(p => s"${upperAlpha(p)}: FromAttributeValue").mkString(", ")
          val params     = (1 to i).map(p => s"field$p: String").mkString(",\n    ")
          val ftypes     = (1 to i).map(p => s"${upperAlpha(p)}").mkString(", ")
          val fparams    = (1 to i).map(p => s"${lowerAlpha(p)}").mkString(", ")
          val gets       = (1 to i).map(p => s"${lowerAlpha(p)} <- get[${upperAlpha(p)}](field$p)").mkString("\n      ")
          s"""def as[$tparams, $returnType](
             |    $params
             |  )(fn: ($ftypes) => $returnType): Either[String, $returnType] =
             |    for {
             |      $gets
             |    } yield fn($fparams)""".stripMargin
      }
      IO.write(
        file,
        s"""package zio.dynamodb
           |
           |private[dynamodb] trait GeneratedFromAttributeValueAs { this: AttrMap =>
           |
           |  ${applyMethods.mkString("\n\n  ")}
           |}""".stripMargin
      )
      Seq(file)
    }.taskValue,
    Compile / sourceGenerators += Def.task {
      val dir                      = (Compile / sourceManaged).value
      val file                     = dir / "zio" / "dynamodb" / "GeneratedFromAttributeValueAs.scala"
      def upperAlpha(i: Int): Char = (('A'.toInt - 1) + i).toChar
      def lowerAlpha(i: Int): Char = (('a'.toInt - 1) + i).toChar
      val applyMethods             = (2 to 22).map {
        i =>
          val returnType = upperAlpha(i + 1)
          val tparams    = (1 to i).map(p => s"${upperAlpha(p)}: FromAttributeValue").mkString(", ")
          val params     = (1 to i).map(p => s"field$p: String").mkString(",\n    ")
          val ftypes     = (1 to i).map(p => s"${upperAlpha(p)}").mkString(", ")
          val fparams    = (1 to i).map(p => s"${lowerAlpha(p)}").mkString(", ")
          val gets       = (1 to i).map(p => s"${lowerAlpha(p)} <- get[${upperAlpha(p)}](field$p)").mkString("\n      ")
          s"""def as[$tparams, $returnType](
             |    $params
             |  )(fn: ($ftypes) => $returnType): Either[String, $returnType] =
             |    for {
             |      $gets
             |    } yield fn($fparams)""".stripMargin
      }
      IO.write(
        file,
        s"""package zio.dynamodb
           |
           |private[dynamodb] trait GeneratedFromAttributeValueAs { this: AttrMap =>
           |
           |  ${applyMethods.mkString("\n\n  ")}
           |}""".stripMargin
      )
      Seq(file)
    }.taskValue,
    Compile / sourceGenerators += Def.task {
      val dir                      = (Compile / sourceManaged).value
      val file                     = dir / "zio" / "dynamodb" / "GeneratedCaseClassDecoders.scala"
      def upperAlpha(i: Int): Char = (('A'.toInt - 1) + i).toChar
      val applyMethods             = (1 to 22).map {
        i =>
          val constructorParams = (1 to i)
            .map(p => s"xs(${p - 1}).asInstanceOf[${upperAlpha(p)}]")
            .mkString(", ")
          val fieldParams       = (1 to i).map(p => s"schema.field${if (i == 1) "" else p.toString}").mkString(",")
          val fieldTypes        = (1 to i).map(p => s"${upperAlpha(p)}").mkString(", ")
          s"""def caseClass${i}Decoder[$fieldTypes, Z](schema: Schema.CaseClass${i}[$fieldTypes, Z]): Decoder[Z] =  { (av: AttributeValue) =>
             |    Codec.Decoder.decodeFields(av, $fieldParams).map { xs =>
             |      schema.construct($constructorParams)
             |    }
             |  }""".stripMargin
      }
      IO.write(
        file,
        s"""package zio.dynamodb
           |
           |import zio.schema.Schema
           |
           |private[dynamodb] trait GeneratedCaseClassDecoders {
           |
           |  ${applyMethods.mkString("\n\n  ")}
           |
           |}""".stripMargin
      )
      Seq(file)
    }.taskValue
  )
  .settings(
    stdSettings("zio-dynamodb")
  )

lazy val examples = module("zio-dynamodb-examples", "examples")
  .settings(
    resolvers += "DynamoDB Local Release Repository" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release",
    resolvers += Resolver.sonatypeRepo("releases"),
    copyJarSetting("examples"),
    (Compile / compile) := (Compile / compile).dependsOn(copyJars).value,
    skip in publish := true,
    fork := true,
    libraryDependencies ++= Seq(
      "dev.zio"               %% "zio-test"      % zioVersion % "test",
      "dev.zio"               %% "zio-test-sbt"  % zioVersion % "test",
      "software.amazon.awssdk" % "dynamodb"      % "2.16.20",
      "com.amazonaws"          % "DynamoDBLocal" % "1.17.0"
    ),
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )
  .dependsOn(zioDynamodb)

def module(moduleName: String, fileName: String): Project =
  Project(moduleName, file(fileName))
    .settings(stdSettings(moduleName))
    .settings(
      libraryDependencies ++= Seq(
        "dev.zio" %% "zio" % zioVersion
      )
    )

lazy val docs = project
  .in(file("zio-dynamodb-docs"))
  .settings(
    skip.in(publish) := true,
    moduleName := "zio-dynamodb-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion
    )
  )
  .enablePlugins(WebsitePlugin)

