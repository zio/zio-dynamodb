package zio.dynamodb

import zio.test.ZIOSpec

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.ZLayer
import zio.aws.core.config
import zio.aws.dynamodb
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty

import java.net.URI
import zio.ZIO
import zio.test.TestResult
import zio.Scope

abstract class DynamoDBLocalSpec extends ZIOSpec[DynamoDBExecutor] {

  private val awsConfig = ZLayer.succeed {
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")),
      endpointOverride = None,
      commonClientConfig = None
    )
  }

  private val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (netty.NettyHttpClient.default ++ awsConfig) >>> config.AwsConfig.default >>> dynamodb.DynamoDb.customized {
      builder =>
        builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  private val dynamoDBExecutorLayer = dynamoDbLayer >>> DynamoDBExecutor.live

  override def bootstrap: ZLayer[Any, Nothing, DynamoDBExecutor] = dynamoDBExecutorLayer.orDie

  final def managedTable(
    tableDefinition: String => DynamoDBQuery[Any, Unit]
  ): ZIO[DynamoDBExecutor with Scope, Throwable, TableName] =
    ZIO
      .acquireRelease(
        for {
          tableName <- zio.Random.nextUUID.map(_.toString)
          _         <- tableDefinition(tableName).execute
        } yield TableName(tableName)
      )(tName => DynamoDBQuery.deleteTable(tName.value).execute.orDie)

  def singleIdKeyTable(tableName: String): DynamoDBQuery.CreateTable =
    DynamoDBQuery.createTable(tableName, KeySchema("id"), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnString("id")
    )

  def idAndYearKeyTable(tableName: String): DynamoDBQuery.CreateTable =
    DynamoDBQuery.createTable(tableName, KeySchema("id", "year"), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnString("id"),
      AttributeDefinition.attrDefnString("year")
    )

  def idAndAccountIdGsiTable(tableName: String): DynamoDBQuery[Any, Unit] =
    DynamoDBQuery
      .createTable(tableName, KeySchema("id"), BillingMode.PayPerRequest)(
        AttributeDefinition.attrDefnString("id"),
        AttributeDefinition.attrDefnString("accountId")
      )
      .gsi(
        indexName = "accountId",
        KeySchema("accountId"),
        ProjectionType.All
      )

  def withSingleIdKeyTable(
    f: String => ZIO[DynamoDBExecutor, Throwable, TestResult]
  ): ZIO[DynamoDBExecutor with Scope, Throwable, TestResult] =
    ZIO.scoped {
      managedTable(singleIdKeyTable).flatMap(t => f(t.value))
    }

  def withIdAndYearKeyTable(
    f: String => ZIO[DynamoDBExecutor, Throwable, TestResult]
  ): ZIO[DynamoDBExecutor with Scope, Throwable, TestResult] =
    ZIO.scoped {
      managedTable(idAndYearKeyTable).flatMap(t => f(t.value))
    }

  def withTwoSingleIdKeyTables(
    f: (String, String) => ZIO[DynamoDBExecutor, Throwable, TestResult]
  ): ZIO[DynamoDBExecutor with Scope, Throwable, TestResult] =
    ZIO.scoped {
      for {
        t1 <- managedTable(singleIdKeyTable)
        t2 <- managedTable(singleIdKeyTable)
        a  <- f(t1.value, t2.value)
      } yield a
    }

  def withIdAndAccountIdGsiTable(
    f: String => ZIO[DynamoDBExecutor, Throwable, TestResult]
  ): ZIO[DynamoDBExecutor with Scope, Throwable, TestResult] =
    ZIO.scoped {
      managedTable(idAndAccountIdGsiTable).flatMap(t => f(t.value))
    }

}
