THROW 51000, 'Make sure to designate the right database in @SourceDatabaseName and @TargetDatabaseName and then comment this out', 1;


DROP PROCEDURE IF EXISTS test.PublishPipeline
GO
CREATE PROCEDURE test.PublishPipeline @ParentRunTimeID BIGINT = NULL
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	/* Pipeline Parameters, hard-coded in Pipeline */
	DECLARE @PipelineName VARCHAR(150) = 'Test Incremental Publish SQL Pipeline',
			@PipelineType VARCHAR(150) = 'Publish',
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
							 @Active = @Active OUTPUT
	
	IF @Active = 1
	BEGIN
		/* Step 1: create Pipeline run time */
		EXEC etl.RunTime_I @PipelineName = @PipelineName,
									@PipelineType = @PipelineType,
									@SourceFullAddress = @SourceFullAddress,
									@SourceAddressType = @SourceAddressType,
									@TargetFullAddress = @TargetFullAddress,
									@TargetAddressType = @TargetAddressType,
									@ParentRunTimeID = @ParentRunTimeID,
									@RunTimeID = @RunTimeID OUTPUT,
									@PipelineID = @PipelineID OUTPUT

		/* Step 2: publish changes from queue */
		EXEC test.test_TargetPublish_UI @RunTimeID = @RunTimeID,
										@PipelineID = @PipelineID,
										@Inserts = @Inserts OUTPUT,
										@Updates = @Updates OUTPUT,
										@SoftDeletes = @SoftDeletes OUTPUT,
										@TotalRows = @TotalRows OUTPUT

		/* Step 3: complete Pipeline run time */
		EXEC etl.RunTime_U @RunTimeID = @RunTimeID,
									@Inserts = @Inserts,
									@Updates = @Updates,
									@SoftDeletes = @SoftDeletes,
									@TotalRows = @TotalRows
	END

END