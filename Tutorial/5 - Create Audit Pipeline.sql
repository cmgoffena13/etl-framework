THROW 51000, 'Make sure to designate the right database in @TargetDatabaseName and then comment this out', 1;


DROP PROCEDURE IF EXISTS test.AuditPipeline
GO
CREATE PROCEDURE test.AuditPipeline @ParentRunTimeID BIGINT = NULL,
								   @Debug BIT = 0,
								   @FullLoad BIT = 0
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	
	/* Pipeline Parameters, hard-coded in Pipeline */
	DECLARE @PipelineName VARCHAR(150) = 'Test Incremental Audit SQL Pipeline',
			@PipelineType VARCHAR(150) = 'Audit',
			@RunTimeID BIGINT,
			@PipelineID INT,
			@Active BIT
	DECLARE @TargetDatabaseName VARCHAR(100) = 'ETL_Metadata',
			@TargetSchemaName VARCHAR(100) = 'test',
			@TargetTableName VARCHAR(100) = 'TargetStage'
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
									@TargetFullAddress = @TargetFullAddress,
									@TargetAddressType = @TargetAddressType,
									@ParentRunTimeID = @ParentRunTimeID,
									@RunTimeID = @RunTimeID OUTPUT,
									@PipelineID = @PipelineID OUTPUT

		/* Step 2: Run audit checks and trigger alerts */
		EXEC etl.AuditLoad @ParentRunTimeID = @ParentRunTimeID,
							@RunTimeID = @RunTimeID,
							@PipelineID = @PipelineID,
							@FullLoad = @FullLoad,
							@Debug = @Debug,
							@AuditType = 'Completeness'
		EXEC etl.AuditLoad @ParentRunTimeID = @ParentRunTimeID,
							@RunTimeID = @RunTimeID,
							@PipelineID = @PipelineID,
							@FullLoad = @FullLoad,
							@Debug = @Debug,
							@AuditType = 'Accuracy'
		EXEC etl.AuditLoad @ParentRunTimeID = @ParentRunTimeID,
							@RunTimeID = @RunTimeID,
							@PipelineID = @PipelineID,
							@Debug = @Debug,
							@AuditType = 'Custom'
	
		/* Step 3: complete Pipeline run time */
		EXEC etl.RunTime_U @RunTimeID = @RunTimeID
	END

END