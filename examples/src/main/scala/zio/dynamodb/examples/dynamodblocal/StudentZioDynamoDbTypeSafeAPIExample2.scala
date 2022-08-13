package zio.dynamodb.examples.dynamodblocal

import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.blocking.Blocking
import zio.clock.Clock
import zio.dynamodb.ConditionExpression.Operand.{ ProjectionExpressionOperand, ValueOperand }
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression.Root
import zio.dynamodb._
import zio.dynamodb.examples.LocalDdbServer
import zio.dynamodb.examples.dynamodblocal.StudentZioDynamoDbTypeSafeAPIExample2.TrafficLight.{ Amber, Box, Green }
import zio.schema.DeriveSchema
import zio.{ App, ExitCode, Has, URIO, ZIO, ZLayer }

import java.net.URI

// example without discriminator
object StudentZioDynamoDbTypeSafeAPIExample2 extends App {

  sealed trait TrafficLight
  object TrafficLight {
    final case class Green(rgb: Int) extends TrafficLight
    object Green {
      implicit val schema = DeriveSchema.gen[Green]
      val rgb             = ProjectionExpression.accessors[Green]
    }
    final case class Red(rgb: Int) extends TrafficLight
    object Red   {
      implicit val schema = DeriveSchema.gen[Red]
      val rgb             = ProjectionExpression.accessors[Red]
    }
    final case class Amber(rgb: Int) extends TrafficLight
    object Amber {
      implicit val schema = DeriveSchema.gen[Amber]
      val rgb             = ProjectionExpression.accessors[Amber]
    }
    final case class Box(id: Int, code: Int, trafficLightColour: TrafficLight)
    object Box   {
      implicit val schema                = DeriveSchema.gen[Box]
      val (id, code, trafficLightColour) = ProjectionExpression.accessors[Box]
    }

    implicit val schema = DeriveSchema.gen[TrafficLight]

    val (amber, green, red) = ProjectionExpression.accessors[TrafficLight]
  }

  private val awsConfig = ZLayer.succeed(
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = SystemPropertyCredentialsProvider.create(),
      endpointOverride = None,
      commonClientConfig = None
    )
  )

  private val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (http4s.default ++ awsConfig) >>> config.configured() >>> dynamodb.customized { builder =>
      builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  private val layer = ((dynamoDbLayer ++ ZLayer.identity[Has[Clock.Service]]) >>> DynamoDBExecutor.live) ++ (ZLayer
    .identity[Has[Blocking.Service]] >>> LocalDdbServer.inMemoryLayer)

  private val program = for {
    _         <- createTable("box", KeySchema("id", "code"), BillingMode.PayPerRequest)(
                   AttributeDefinition.attrDefnNumber("id"),
                   AttributeDefinition.attrDefnNumber("code")
                 ).execute
    boxOfGreen = Box(1, 1, Green(1))
    boxOfAmber = Box(1, 2, Amber(1))
    _         <- put[Box]("box", boxOfGreen).execute
    _         <- put[Box]("box", boxOfAmber).execute
    // this query works as we use a top level lens for trafficLightColour and compare against a complete instance
    query      = queryAll[Box]("box").whereKey(Box.id === 1).filter(Box.trafficLightColour === Green(1))
    // if we want to dig into the enum we need to traverse from top level Root all the way to "rgb"
    // I don't think we have a way to do this with existing Reified Optics - for instance TrafficLight prism is unaware of Box lens
    // eg
    // Map(trafficLightColour -> Map(String(Green) -> Map(String(rgb) -> Number(42))))
    root       = ProjectionExpression.mapElement(Root, "trafficLightColour")
    green      = ProjectionExpression.mapElement(root, "Green")
    rgb        = ProjectionExpression.mapElement(green, "rgb")
    condEprn   = ConditionExpression
                   .Equals(ProjectionExpressionOperand(rgb), ValueOperand(AttributeValue.Number(BigDecimal(1))))
    x          = Box.id
    y          = Box.id === 1
    z          = Box.trafficLightColour >>> TrafficLight.green >>> Green.rgb
    zz         = (Box.trafficLightColour >>> TrafficLight.green >>> Green.rgb) === 1
    _          = println(s"$x $y $z $zz $condEprn")
    query2     = queryAll[Box]("box")
                   .whereKey(Box.id === 1)
                   .filter(zz)
// Green.rgb === 1 does not work. I think we may need to compose RO PEs somehow eg "Box.trafficLightColour / Green.rgb === 42"
    _         <- ZIO.debug(s"query=$query\nquery2=$query2")
    stream    <- query2.execute
    list      <- stream.runCollect
    _         <- ZIO.debug(s"boxes=$list")
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(layer).exitCode
}
// example without discriminator
