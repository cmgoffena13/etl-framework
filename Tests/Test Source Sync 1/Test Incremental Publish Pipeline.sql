USE ETL_Metadata
GO
SET NOCOUNT ON

/* Pipeline Parameters, provided by Pipeline itself */
DECLARE @PipelineName VARCHAR(150) = 'Test Incremental Publish SQL Pipeline',
		@PipelineType VARCHAR(150) = 'Publish',
		@ParentRunTimeID BIGINT, /* Grab from main pipeline */
		@RunTimeID BIGINT,
		@PipelineID INT,
		@Active BIT
DECLARE @Inserts INT,
		@Updates INT,
		@SoftDeletes INT,
		@TotalRows INT
DECLARE @SourceDatabaseName VARCHAR(100) = 'ETL_Metadata',
		@SourceSchemaName VARCHAR(100) = 'test',
		@SourceTableName VARCHAR(100) = 'TargetStage',
		@SourceFullAddress VARCHAR(200)
SET @SourceFullAddress = CONCAT_WS('.', @SourceDatabaseName, @SourceSchemaName, @SourceTableName)
DECLARE @SourceAddressType VARCHAR(150) = 'Database'

/* These components aren't necessary just easier to combine stuff */
DECLARE @TargetDatabaseName VARCHAR(100) = 'ETL_Metadata',
		@TargetSchemaName VARCHAR(100) = 'test',
		@TargetTableName VARCHAR(100) = 'TargetPublish'
DECLARE @TargetFullAddress VARCHAR(200)
SET @TargetFullAddress = CONCAT_WS('.', @TargetDatabaseName, @TargetSchemaName, @TargetTableName)
DECLARE @TargetAddressType VARCHAR(150) = 'Database'

EXEC etl.PipelineActive_S @PipelineName = @PipelineName,
						 @Active = @Active

IF @Active = 1
BEGIN
	/* First DAG */
	EXEC etl.RunTime_I @PipelineName = @PipelineName,
								@PipelineType = @PipelineType,
								@SourceFullAddress = @SourceFullAddress,
								@SourceAddressType = @SourceAddressType,
								@TargetFullAddress = @TargetFullAddress,
								@TargetAddressType = @TargetAddressType,
								@ParentRunTimeID = @ParentRunTimeID,
								@RunTimeID = @RunTimeID OUTPUT,
								@PipelineID = @PipelineID OUTPUT

	/* Publish incremental changes */
	EXEC test.test_TargetPublish_UI @RunTimeID = @RunTimeID,
									@PipelineID = @PipelineID,
									@Inserts = @Inserts OUTPUT,
									@Updates = @Updates OUTPUT,
									@SoftDeletes = @SoftDeletes OUTPUT,
									@TotalRows = @TotalRows OUTPUT

	/* Complete pipeline */
	EXEC etl.RunTime_U @RunTimeID = @RunTimeID,
								@Inserts = @Inserts,
								@Updates = @Updates,
								@SoftDeletes = @SoftDeletes,
								@TotalRows = @TotalRows
END

/* manual validation */
SELECT * FROM etl.Pipeline WHERE PipelineID = @PipelineID
SELECT * FROM etl.RunTime WHERE PipelineID = @PipelineID
ORDER BY RunTimeID DESC