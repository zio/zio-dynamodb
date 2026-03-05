package zio.dynamodb.mock

import zio.aws.dynamodb.model.DescribeEndpointsResponse.ReadOnly
import zio.mock.{ Mock, Proxy }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.aws.dynamodb.model.primitives.{ AttributeName, TableName }
import zio.aws.core.{ AwsError, StreamingOutputResult }
import zio.aws.core.aspects.AwsCallAspect
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model.{
  BatchExecuteStatementRequest,
  BatchGetItemRequest,
  BatchWriteItemRequest,
  CreateBackupRequest,
  CreateGlobalTableRequest,
  CreateTableRequest,
  DeleteBackupRequest,
  DeleteItemRequest,
  DeleteResourcePolicyRequest,
  DeleteTableRequest,
  DescribeBackupRequest,
  DescribeContinuousBackupsRequest,
  DescribeContributorInsightsRequest,
  DescribeEndpointsRequest,
  DescribeExportRequest,
  DescribeGlobalTableRequest,
  DescribeGlobalTableSettingsRequest,
  DescribeImportRequest,
  DescribeKinesisStreamingDestinationRequest,
  DescribeLimitsRequest,
  DescribeTableReplicaAutoScalingRequest,
  DescribeTableRequest,
  DescribeTimeToLiveRequest,
  DisableKinesisStreamingDestinationRequest,
  EnableKinesisStreamingDestinationRequest,
  ExecuteStatementRequest,
  ExecuteTransactionRequest,
  ExportTableToPointInTimeRequest,
  GetItemRequest,
  GetResourcePolicyRequest,
  ImportTableRequest,
  ListBackupsRequest,
  ListContributorInsightsRequest,
  ListExportsRequest,
  ListGlobalTablesRequest,
  ListImportsRequest,
  ListTablesRequest,
  ListTagsOfResourceRequest,
  PutItemRequest,
  PutResourcePolicyRequest,
  QueryRequest,
  RestoreTableFromBackupRequest,
  RestoreTableToPointInTimeRequest,
  ScanRequest,
  TagResourceRequest,
  TransactGetItemsRequest,
  TransactWriteItemsRequest,
  UntagResourceRequest,
  UpdateContinuousBackupsRequest,
  UpdateContributorInsightsRequest,
  UpdateGlobalTableRequest,
  UpdateGlobalTableSettingsRequest,
  UpdateItemRequest,
  UpdateKinesisStreamingDestinationRequest,
  UpdateTableReplicaAutoScalingRequest,
  UpdateTableRequest,
  UpdateTimeToLiveRequest
}
import zio.{ IO, URLayer, ZEnvironment, ZIO }
import zio.stream.ZStream

// ZIO AWS DynamoDB no longer comes with generated zio-mock's - also this library is deprecated - so as a short term
// measure I have copied the mock here until another mock/stub solution is found.
object DynamoDbMock extends Mock[DynamoDb] {
  object PutItem
      extends Effect[
        PutItemRequest,
        AwsError,
        zio.aws.dynamodb.model.PutItemResponse.ReadOnly
      ]
  object RestoreTableToPointInTime
      extends Effect[
        RestoreTableToPointInTimeRequest,
        AwsError,
        zio.aws.dynamodb.model.RestoreTableToPointInTimeResponse.ReadOnly
      ]
  object UpdateKinesisStreamingDestination
      extends Effect[
        UpdateKinesisStreamingDestinationRequest,
        AwsError,
        zio.aws.dynamodb.model.UpdateKinesisStreamingDestinationResponse.ReadOnly
      ]
  object UpdateGlobalTableSettings
      extends Effect[
        UpdateGlobalTableSettingsRequest,
        AwsError,
        zio.aws.dynamodb.model.UpdateGlobalTableSettingsResponse.ReadOnly
      ]
  object BatchExecuteStatement
      extends Effect[
        BatchExecuteStatementRequest,
        AwsError,
        zio.aws.dynamodb.model.BatchExecuteStatementResponse.ReadOnly
      ]
  object ExecuteTransaction
      extends Effect[
        ExecuteTransactionRequest,
        AwsError,
        zio.aws.dynamodb.model.ExecuteTransactionResponse.ReadOnly
      ]
  object ListImports
      extends Stream[
        ListImportsRequest,
        AwsError,
        zio.aws.dynamodb.model.ImportSummary.ReadOnly
      ]
  object ListImportsPaginated
      extends Effect[
        ListImportsRequest,
        AwsError,
        zio.aws.dynamodb.model.ListImportsResponse.ReadOnly
      ]
  object ListBackups
      extends Effect[
        ListBackupsRequest,
        AwsError,
        zio.aws.dynamodb.model.ListBackupsResponse.ReadOnly
      ]
  object UpdateContributorInsights
      extends Effect[
        UpdateContributorInsightsRequest,
        AwsError,
        zio.aws.dynamodb.model.UpdateContributorInsightsResponse.ReadOnly
      ]
  object DescribeGlobalTable
      extends Effect[
        DescribeGlobalTableRequest,
        AwsError,
        zio.aws.dynamodb.model.DescribeGlobalTableResponse.ReadOnly
      ]
  object CreateTable
      extends Effect[
        CreateTableRequest,
        AwsError,
        zio.aws.dynamodb.model.CreateTableResponse.ReadOnly
      ]
  object DeleteTable
      extends Effect[
        DeleteTableRequest,
        AwsError,
        zio.aws.dynamodb.model.DeleteTableResponse.ReadOnly
      ]
  object ExecuteStatement
      extends Effect[
        ExecuteStatementRequest,
        AwsError,
        StreamingOutputResult[
          Any,
          zio.aws.dynamodb.model.ExecuteStatementResponse.ReadOnly,
          Map[AttributeName, zio.aws.dynamodb.model.AttributeValue.ReadOnly]
        ]
      ]
  object ExecuteStatementPaginated
      extends Effect[
        ExecuteStatementRequest,
        AwsError,
        zio.aws.dynamodb.model.ExecuteStatementResponse.ReadOnly
      ]
  object CreateBackup
      extends Effect[
        CreateBackupRequest,
        AwsError,
        zio.aws.dynamodb.model.CreateBackupResponse.ReadOnly
      ]
  object EnableKinesisStreamingDestination
      extends Effect[
        EnableKinesisStreamingDestinationRequest,
        AwsError,
        zio.aws.dynamodb.model.EnableKinesisStreamingDestinationResponse.ReadOnly
      ]
  object DescribeContributorInsights
      extends Effect[
        DescribeContributorInsightsRequest,
        AwsError,
        zio.aws.dynamodb.model.DescribeContributorInsightsResponse.ReadOnly
      ]
  object ListGlobalTables
      extends Effect[
        ListGlobalTablesRequest,
        AwsError,
        zio.aws.dynamodb.model.ListGlobalTablesResponse.ReadOnly
      ]
  object BatchGetItem
      extends Effect[
        BatchGetItemRequest,
        AwsError,
        zio.aws.dynamodb.model.BatchGetItemResponse.ReadOnly
      ]
  object PutResourcePolicy
      extends Effect[
        PutResourcePolicyRequest,
        AwsError,
        zio.aws.dynamodb.model.PutResourcePolicyResponse.ReadOnly
      ]
  object DescribeExport
      extends Effect[
        DescribeExportRequest,
        AwsError,
        zio.aws.dynamodb.model.DescribeExportResponse.ReadOnly
      ]
  object ListExports
      extends Stream[
        ListExportsRequest,
        AwsError,
        zio.aws.dynamodb.model.ExportSummary.ReadOnly
      ]
  object ListExportsPaginated
      extends Effect[
        ListExportsRequest,
        AwsError,
        zio.aws.dynamodb.model.ListExportsResponse.ReadOnly
      ]
  object DisableKinesisStreamingDestination
      extends Effect[
        DisableKinesisStreamingDestinationRequest,
        AwsError,
        zio.aws.dynamodb.model.DisableKinesisStreamingDestinationResponse.ReadOnly
      ]
  object DescribeGlobalTableSettings
      extends Effect[
        DescribeGlobalTableSettingsRequest,
        AwsError,
        zio.aws.dynamodb.model.DescribeGlobalTableSettingsResponse.ReadOnly
      ]
  object DescribeTableReplicaAutoScaling
      extends Effect[
        DescribeTableReplicaAutoScalingRequest,
        AwsError,
        zio.aws.dynamodb.model.DescribeTableReplicaAutoScalingResponse.ReadOnly
      ]
  object Query
      extends Stream[
        QueryRequest,
        AwsError,
        Map[AttributeName, zio.aws.dynamodb.model.AttributeValue.ReadOnly]
      ]
  object QueryPaginated
      extends Effect[
        QueryRequest,
        AwsError,
        zio.aws.dynamodb.model.QueryResponse.ReadOnly
      ]
  object ExportTableToPointInTime
      extends Effect[
        ExportTableToPointInTimeRequest,
        AwsError,
        zio.aws.dynamodb.model.ExportTableToPointInTimeResponse.ReadOnly
      ]
  object ListTables        extends Stream[ListTablesRequest, AwsError, TableName]
  object ListTablesPaginated
      extends Effect[
        ListTablesRequest,
        AwsError,
        zio.aws.dynamodb.model.ListTablesResponse.ReadOnly
      ]
  object DescribeTable
      extends Effect[
        DescribeTableRequest,
        AwsError,
        zio.aws.dynamodb.model.DescribeTableResponse.ReadOnly
      ]
  object RestoreTableFromBackup
      extends Effect[
        RestoreTableFromBackupRequest,
        AwsError,
        zio.aws.dynamodb.model.RestoreTableFromBackupResponse.ReadOnly
      ]
  object UpdateContinuousBackups
      extends Effect[
        UpdateContinuousBackupsRequest,
        AwsError,
        zio.aws.dynamodb.model.UpdateContinuousBackupsResponse.ReadOnly
      ]
  object UpdateTable
      extends Effect[
        UpdateTableRequest,
        AwsError,
        zio.aws.dynamodb.model.UpdateTableResponse.ReadOnly
      ]
  object CreateGlobalTable
      extends Effect[
        CreateGlobalTableRequest,
        AwsError,
        zio.aws.dynamodb.model.CreateGlobalTableResponse.ReadOnly
      ]
  object UntagResource     extends Effect[UntagResourceRequest, AwsError, Unit]
  object ListContributorInsights
      extends Stream[
        ListContributorInsightsRequest,
        AwsError,
        zio.aws.dynamodb.model.ContributorInsightsSummary.ReadOnly
      ]
  object ListContributorInsightsPaginated
      extends Effect[
        ListContributorInsightsRequest,
        AwsError,
        zio.aws.dynamodb.model.ListContributorInsightsResponse.ReadOnly
      ]
  object DescribeImport
      extends Effect[
        DescribeImportRequest,
        AwsError,
        zio.aws.dynamodb.model.DescribeImportResponse.ReadOnly
      ]
  object DeleteResourcePolicy
      extends Effect[
        DeleteResourcePolicyRequest,
        AwsError,
        zio.aws.dynamodb.model.DeleteResourcePolicyResponse.ReadOnly
      ]
  object DescribeTimeToLive
      extends Effect[
        DescribeTimeToLiveRequest,
        AwsError,
        zio.aws.dynamodb.model.DescribeTimeToLiveResponse.ReadOnly
      ]
  object DescribeEndpoints extends Effect[DescribeEndpointsRequest, AwsError, ReadOnly]
  object TransactWriteItems
      extends Effect[
        TransactWriteItemsRequest,
        AwsError,
        zio.aws.dynamodb.model.TransactWriteItemsResponse.ReadOnly
      ]
  object DeleteBackup
      extends Effect[
        DeleteBackupRequest,
        AwsError,
        zio.aws.dynamodb.model.DeleteBackupResponse.ReadOnly
      ]
  object DescribeContinuousBackups
      extends Effect[
        DescribeContinuousBackupsRequest,
        AwsError,
        zio.aws.dynamodb.model.DescribeContinuousBackupsResponse.ReadOnly
      ]
  object TagResource       extends Effect[TagResourceRequest, AwsError, Unit]
  object TransactGetItems
      extends Effect[
        TransactGetItemsRequest,
        AwsError,
        zio.aws.dynamodb.model.TransactGetItemsResponse.ReadOnly
      ]
  object UpdateItem
      extends Effect[
        UpdateItemRequest,
        AwsError,
        zio.aws.dynamodb.model.UpdateItemResponse.ReadOnly
      ]
  object UpdateTableReplicaAutoScaling
      extends Effect[
        UpdateTableReplicaAutoScalingRequest,
        AwsError,
        zio.aws.dynamodb.model.UpdateTableReplicaAutoScalingResponse.ReadOnly
      ]
  object GetItem
      extends Effect[
        GetItemRequest,
        AwsError,
        zio.aws.dynamodb.model.GetItemResponse.ReadOnly
      ]
  object GetResourcePolicy
      extends Effect[
        GetResourcePolicyRequest,
        AwsError,
        zio.aws.dynamodb.model.GetResourcePolicyResponse.ReadOnly
      ]
  object UpdateTimeToLive
      extends Effect[
        UpdateTimeToLiveRequest,
        AwsError,
        zio.aws.dynamodb.model.UpdateTimeToLiveResponse.ReadOnly
      ]
  object DescribeLimits
      extends Effect[
        DescribeLimitsRequest,
        AwsError,
        zio.aws.dynamodb.model.DescribeLimitsResponse.ReadOnly
      ]
  object UpdateGlobalTable
      extends Effect[
        UpdateGlobalTableRequest,
        AwsError,
        zio.aws.dynamodb.model.UpdateGlobalTableResponse.ReadOnly
      ]
  object BatchWriteItem
      extends Effect[
        BatchWriteItemRequest,
        AwsError,
        zio.aws.dynamodb.model.BatchWriteItemResponse.ReadOnly
      ]
  object Scan
      extends Stream[
        ScanRequest,
        AwsError,
        Map[AttributeName, zio.aws.dynamodb.model.AttributeValue.ReadOnly]
      ]
  object ScanPaginated
      extends Effect[
        ScanRequest,
        AwsError,
        zio.aws.dynamodb.model.ScanResponse.ReadOnly
      ]
  object ListTagsOfResource
      extends Stream[
        ListTagsOfResourceRequest,
        AwsError,
        zio.aws.dynamodb.model.Tag.ReadOnly
      ]
  object ListTagsOfResourcePaginated
      extends Effect[
        ListTagsOfResourceRequest,
        AwsError,
        zio.aws.dynamodb.model.ListTagsOfResourceResponse.ReadOnly
      ]
  object DescribeBackup
      extends Effect[
        DescribeBackupRequest,
        AwsError,
        zio.aws.dynamodb.model.DescribeBackupResponse.ReadOnly
      ]
  object DeleteItem
      extends Effect[
        DeleteItemRequest,
        AwsError,
        zio.aws.dynamodb.model.DeleteItemResponse.ReadOnly
      ]
  object ImportTable
      extends Effect[
        ImportTableRequest,
        AwsError,
        zio.aws.dynamodb.model.ImportTableResponse.ReadOnly
      ]
  object DescribeKinesisStreamingDestination
      extends Effect[
        DescribeKinesisStreamingDestinationRequest,
        AwsError,
        zio.aws.dynamodb.model.DescribeKinesisStreamingDestinationResponse.ReadOnly
      ]
  val compose: URLayer[Proxy, DynamoDb] = zio.ZLayer {
    ZIO.service[Proxy].flatMap { proxy =>
      withRuntime[Proxy, DynamoDb] { rts =>
        ZIO.succeed {
          new DynamoDb {
            val api: DynamoDbAsyncClient                                     = null
            def withAspect[R1](
              newAspect: AwsCallAspect[R1],
              r: ZEnvironment[R1]
            ): DynamoDb                                                      = this
            def putItem(
              request: PutItemRequest
            ): IO[AwsError, zio.aws.dynamodb.model.PutItemResponse.ReadOnly] =
              proxy(PutItem, request)
            def restoreTableToPointInTime(
              request: RestoreTableToPointInTimeRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.RestoreTableToPointInTimeResponse.ReadOnly
            ]                                                                = proxy(RestoreTableToPointInTime, request)
            def updateKinesisStreamingDestination(
              request: UpdateKinesisStreamingDestinationRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.UpdateKinesisStreamingDestinationResponse.ReadOnly
            ]                                                                = proxy(UpdateKinesisStreamingDestination, request)
            def updateGlobalTableSettings(
              request: UpdateGlobalTableSettingsRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.UpdateGlobalTableSettingsResponse.ReadOnly
            ]                                                                = proxy(UpdateGlobalTableSettings, request)
            def batchExecuteStatement(
              request: BatchExecuteStatementRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.BatchExecuteStatementResponse.ReadOnly
            ]                                                                = proxy(BatchExecuteStatement, request)
            def executeTransaction(request: ExecuteTransactionRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.ExecuteTransactionResponse.ReadOnly
            ]                                                                = proxy(ExecuteTransaction, request)
            def listImports(request: ListImportsRequest): ZStream[
              Any,
              AwsError,
              zio.aws.dynamodb.model.ImportSummary.ReadOnly
            ]                                                                =
              zio.Unsafe.unsafe { implicit u =>
                rts.unsafe.run {
                  proxy(ListImports, request)
                }
                  .getOrThrowFiberFailure()
              }
            def listImportsPaginated(request: ListImportsRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.ListImportsResponse.ReadOnly
            ]                                                                = proxy(ListImportsPaginated, request)
            def listBackups(request: ListBackupsRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.ListBackupsResponse.ReadOnly
            ]                                                                = proxy(ListBackups, request)
            def updateContributorInsights(
              request: UpdateContributorInsightsRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.UpdateContributorInsightsResponse.ReadOnly
            ]                                                                = proxy(UpdateContributorInsights, request)
            def describeGlobalTable(request: DescribeGlobalTableRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.DescribeGlobalTableResponse.ReadOnly
            ]                                                                = proxy(DescribeGlobalTable, request)
            def createTable(request: CreateTableRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.CreateTableResponse.ReadOnly
            ]                                                                = proxy(CreateTable, request)
            def deleteTable(request: DeleteTableRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.DeleteTableResponse.ReadOnly
            ]                                                                = proxy(DeleteTable, request)
            def executeStatement(
              request: ExecuteStatementRequest
            ): ZIO[
              Any,
              AwsError,
              StreamingOutputResult[
                Any,
                zio.aws.dynamodb.model.ExecuteStatementResponse.ReadOnly,
                Map[AttributeName, zio.aws.dynamodb.model.AttributeValue.ReadOnly]
              ]
            ]                                                                = proxy(ExecuteStatement, request)
            def executeStatementPaginated(request: ExecuteStatementRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.ExecuteStatementResponse.ReadOnly
            ]                                                                = proxy(ExecuteStatementPaginated, request)
            def createBackup(request: CreateBackupRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.CreateBackupResponse.ReadOnly
            ]                                                                = proxy(CreateBackup, request)
            def enableKinesisStreamingDestination(
              request: EnableKinesisStreamingDestinationRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.EnableKinesisStreamingDestinationResponse.ReadOnly
            ]                                                                = proxy(EnableKinesisStreamingDestination, request)
            def describeContributorInsights(
              request: DescribeContributorInsightsRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.DescribeContributorInsightsResponse.ReadOnly
            ]                                                                = proxy(DescribeContributorInsights, request)
            def listGlobalTables(request: ListGlobalTablesRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.ListGlobalTablesResponse.ReadOnly
            ]                                                                = proxy(ListGlobalTables, request)
            def batchGetItem(request: BatchGetItemRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.BatchGetItemResponse.ReadOnly
            ]                                                                = proxy(BatchGetItem, request)
            def putResourcePolicy(request: PutResourcePolicyRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.PutResourcePolicyResponse.ReadOnly
            ]                                                                = proxy(PutResourcePolicy, request)
            def describeExport(request: DescribeExportRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.DescribeExportResponse.ReadOnly
            ]                                                                = proxy(DescribeExport, request)
            def listExports(request: ListExportsRequest): ZStream[
              Any,
              AwsError,
              zio.aws.dynamodb.model.ExportSummary.ReadOnly
            ]                                                                =
              zio.Unsafe.unsafe { implicit u =>
                rts.unsafe.run {
                  proxy(ListExports, request)
                }
                  .getOrThrowFiberFailure()
              }
            def listExportsPaginated(request: ListExportsRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.ListExportsResponse.ReadOnly
            ]                                                                = proxy(ListExportsPaginated, request)
            def disableKinesisStreamingDestination(
              request: DisableKinesisStreamingDestinationRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.DisableKinesisStreamingDestinationResponse.ReadOnly
            ]                                                                = proxy(DisableKinesisStreamingDestination, request)
            def describeGlobalTableSettings(
              request: DescribeGlobalTableSettingsRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.DescribeGlobalTableSettingsResponse.ReadOnly
            ]                                                                = proxy(DescribeGlobalTableSettings, request)
            def describeTableReplicaAutoScaling(
              request: DescribeTableReplicaAutoScalingRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.DescribeTableReplicaAutoScalingResponse.ReadOnly
            ]                                                                = proxy(DescribeTableReplicaAutoScaling, request)
            def query(request: QueryRequest): ZStream[
              Any,
              AwsError,
              Map[AttributeName, zio.aws.dynamodb.model.AttributeValue.ReadOnly]
            ]                                                                =
              zio.Unsafe.unsafe { implicit u =>
                rts.unsafe.run {
                  proxy(Query, request)
                }
                  .getOrThrowFiberFailure()
              }
            def queryPaginated(
              request: QueryRequest
            ): IO[AwsError, zio.aws.dynamodb.model.QueryResponse.ReadOnly]   =
              proxy(QueryPaginated, request)
            def exportTableToPointInTime(
              request: ExportTableToPointInTimeRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.ExportTableToPointInTimeResponse.ReadOnly
            ]                                                                = proxy(ExportTableToPointInTime, request)
            def listTables(
              request: ListTablesRequest
            ): ZStream[Any, AwsError, TableName]                             =
              zio.Unsafe.unsafe { implicit u =>
                rts.unsafe.run {
                  proxy(ListTables, request)
                }
                  .getOrThrowFiberFailure()
              }
            def listTablesPaginated(request: ListTablesRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.ListTablesResponse.ReadOnly
            ]                                                                = proxy(ListTablesPaginated, request)
            def describeTable(request: DescribeTableRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.DescribeTableResponse.ReadOnly
            ]                                                                = proxy(DescribeTable, request)
            def restoreTableFromBackup(
              request: RestoreTableFromBackupRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.RestoreTableFromBackupResponse.ReadOnly
            ]                                                                = proxy(RestoreTableFromBackup, request)
            def updateContinuousBackups(
              request: UpdateContinuousBackupsRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.UpdateContinuousBackupsResponse.ReadOnly
            ]                                                                = proxy(UpdateContinuousBackups, request)
            def updateTable(request: UpdateTableRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.UpdateTableResponse.ReadOnly
            ]                                                                = proxy(UpdateTable, request)
            def createGlobalTable(request: CreateGlobalTableRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.CreateGlobalTableResponse.ReadOnly
            ]                                                                = proxy(CreateGlobalTable, request)
            def untagResource(
              request: UntagResourceRequest
            ): IO[AwsError, Unit]                                            = proxy(UntagResource, request)
            def listContributorInsights(
              request: ListContributorInsightsRequest
            ): ZStream[
              Any,
              AwsError,
              zio.aws.dynamodb.model.ContributorInsightsSummary.ReadOnly
            ]                                                                =
              zio.Unsafe.unsafe { implicit u =>
                rts.unsafe.run {
                  proxy(ListContributorInsights, request)
                }
                  .getOrThrowFiberFailure()
              }
            def listContributorInsightsPaginated(
              request: ListContributorInsightsRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.ListContributorInsightsResponse.ReadOnly
            ]                                                                = proxy(ListContributorInsightsPaginated, request)
            def describeImport(request: DescribeImportRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.DescribeImportResponse.ReadOnly
            ]                                                                = proxy(DescribeImport, request)
            def deleteResourcePolicy(request: DeleteResourcePolicyRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.DeleteResourcePolicyResponse.ReadOnly
            ]                                                                = proxy(DeleteResourcePolicy, request)
            def describeTimeToLive(request: DescribeTimeToLiveRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.DescribeTimeToLiveResponse.ReadOnly
            ]                                                                = proxy(DescribeTimeToLive, request)
            def describeEndpoints(
              request: DescribeEndpointsRequest
            ): IO[AwsError, ReadOnly]                                        = proxy(DescribeEndpoints, request)
            def transactWriteItems(request: TransactWriteItemsRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.TransactWriteItemsResponse.ReadOnly
            ]                                                                = proxy(TransactWriteItems, request)
            def deleteBackup(request: DeleteBackupRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.DeleteBackupResponse.ReadOnly
            ]                                                                = proxy(DeleteBackup, request)
            def describeContinuousBackups(
              request: DescribeContinuousBackupsRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.DescribeContinuousBackupsResponse.ReadOnly
            ]                                                                = proxy(DescribeContinuousBackups, request)
            def tagResource(request: TagResourceRequest): IO[AwsError, Unit] =
              proxy(TagResource, request)
            def transactGetItems(request: TransactGetItemsRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.TransactGetItemsResponse.ReadOnly
            ]                                                                = proxy(TransactGetItems, request)
            def updateItem(request: UpdateItemRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.UpdateItemResponse.ReadOnly
            ]                                                                = proxy(UpdateItem, request)
            def updateTableReplicaAutoScaling(
              request: UpdateTableReplicaAutoScalingRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.UpdateTableReplicaAutoScalingResponse.ReadOnly
            ]                                                                = proxy(UpdateTableReplicaAutoScaling, request)
            def getItem(
              request: GetItemRequest
            ): IO[AwsError, zio.aws.dynamodb.model.GetItemResponse.ReadOnly] =
              proxy(GetItem, request)
            def getResourcePolicy(request: GetResourcePolicyRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.GetResourcePolicyResponse.ReadOnly
            ]                                                                = proxy(GetResourcePolicy, request)
            def updateTimeToLive(request: UpdateTimeToLiveRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.UpdateTimeToLiveResponse.ReadOnly
            ]                                                                = proxy(UpdateTimeToLive, request)
            def describeLimits(request: DescribeLimitsRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.DescribeLimitsResponse.ReadOnly
            ]                                                                = proxy(DescribeLimits, request)
            def updateGlobalTable(request: UpdateGlobalTableRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.UpdateGlobalTableResponse.ReadOnly
            ]                                                                = proxy(UpdateGlobalTable, request)
            def batchWriteItem(request: BatchWriteItemRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.BatchWriteItemResponse.ReadOnly
            ]                                                                = proxy(BatchWriteItem, request)
            def scan(request: ScanRequest): ZStream[
              Any,
              AwsError,
              Map[AttributeName, zio.aws.dynamodb.model.AttributeValue.ReadOnly]
            ]                                                                =
              zio.Unsafe.unsafe { implicit u =>
                rts.unsafe.run {
                  proxy(Scan, request)
                }
                  .getOrThrowFiberFailure()
              }
            def scanPaginated(
              request: ScanRequest
            ): IO[AwsError, zio.aws.dynamodb.model.ScanResponse.ReadOnly]    =
              proxy(ScanPaginated, request)
            def listTagsOfResource(
              request: ListTagsOfResourceRequest
            ): ZStream[Any, AwsError, zio.aws.dynamodb.model.Tag.ReadOnly]   =
              zio.Unsafe.unsafe { implicit u =>
                rts.unsafe.run {
                  proxy(ListTagsOfResource, request)
                }
                  .getOrThrowFiberFailure()
              }
            def listTagsOfResourcePaginated(
              request: ListTagsOfResourceRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.ListTagsOfResourceResponse.ReadOnly
            ]                                                                = proxy(ListTagsOfResourcePaginated, request)
            def describeBackup(request: DescribeBackupRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.DescribeBackupResponse.ReadOnly
            ]                                                                = proxy(DescribeBackup, request)
            def deleteItem(request: DeleteItemRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.DeleteItemResponse.ReadOnly
            ]                                                                = proxy(DeleteItem, request)
            def importTable(request: ImportTableRequest): IO[
              AwsError,
              zio.aws.dynamodb.model.ImportTableResponse.ReadOnly
            ]                                                                = proxy(ImportTable, request)
            def describeKinesisStreamingDestination(
              request: DescribeKinesisStreamingDestinationRequest
            ): IO[
              AwsError,
              zio.aws.dynamodb.model.DescribeKinesisStreamingDestinationResponse.ReadOnly
            ]                                                                = proxy(DescribeKinesisStreamingDestination, request)
          }
        }
      }
    }
  }
}
