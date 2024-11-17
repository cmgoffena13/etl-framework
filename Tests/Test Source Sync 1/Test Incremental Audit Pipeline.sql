USE ETL_Metadata
GO
SET NOCOUNT ON

/* Pipeline Parameters, provided by Pipeline itself */
DECLARE @PipelineName VARCHAR(150) = 'Test Incremental Audit SQL Pipeline',
		@PipelineType VARCHAR(150) = 'Audit',
		@ParentRunTimeID BIGINT, /* Grab from incremental extraction pipeline */
		@RunTimeID BIGINT,
		@PipelineID INT,
		@Active BIT

/* These components aren't necessary just easier to combine stuff */
DECLARE @TargetDatabaseName VARCHAR(100) = 'ETL_Metadata',
		@TargetSchemaName VARCHAR(100) = 'test',
		@TargetTableName VARCHAR(100) = 'TargetStage'

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
								@TargetFullAddress = @TargetFullAddress,
								@TargetAddressType = @TargetAddressType,
								@ParentRunTimeID = @ParentRunTimeID,
								@RunTimeID = @RunTimeID OUTPUT,
								@PipelineID = @PipelineID OUTPUT


	/* Run checks and log results */
	/* Check quality rules for target address and EXEC column completeness and accuracy checks and log results */
	SELECT @RunTimeID AS RunTimeID, @PipelineID AS PipelineID
	EXEC etl.AuditLoad @RunTimeID = @RunTimeID,
						@PipelineID = @PipelineID,
						@AuditType = 'Completeness'
	EXEC etl.AuditLoad @RunTimeID = @RunTimeID,
						@PipelineID = @PipelineID,
						@AuditType = 'Accuracy'

	/* Complete pipeline */
	EXEC etl.RunTime_U @RunTimeID = @RunTimeID
END

/* testing results */
SELECT * FROM etl.AuditColumnRule WHERE PipelineID = @PipelineID AND Active=1
SELECT * FROM etl.AuditColumnLog WHERE RunTimeID = @RunTimeID