USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.AuditLoad
GO
CREATE PROCEDURE etl.AuditLoad @ParentRunTimeID BIGINT = NULL,
								@RunTimeID BIGINT,
								@PipelineID INT,
								@FullLoad BIT = 1,
								@Debug BIT = 0,
								@AuditType VARCHAR(150)
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @ErrorMessage VARCHAR(100)

	IF @FullLoad = 0 
		AND @ParentRunTimeID IS NULL 
		AND (@AuditType = 'Completeness' OR @AuditType = 'Accuracy')
	BEGIN
		SET @ErrorMessage = '@ParentRunTimeID is required for incremental (@FullLoad=0) Completeness/Accuracy audits';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Grab Pipeline args to determine if chunking should occur, only allowed for full loads */
	DECLARE @ChunkOut BIT
	SELECT @ChunkOut = ISNULL(JSON_VALUE(PipelineArgs, '$.info.audit.chunkout'), 1)
	FROM etl.Pipeline
	WHERE PipelineID = @PipelineID

	IF @AuditType = 'Completeness'
	BEGIN
		IF @ChunkOut=1
			BEGIN /* Recursive check that chunks out dataset and logs results */
				EXEC etl.AuditCompletenessBatch_I @ParentRunTimeID = @ParentRunTimeID,
												  @RunTimeID = @RunTimeID,
												  @PipelineID = @PipelineID,
												  @FullLoad = @FullLoad,
												  @Debug = @Debug
			END
		ELSE
			BEGIN
				/* Scans once and logs results */
				EXEC etl.AuditCompleteness_I @ParentRunTimeID = @ParentRunTimeID,
											 @RunTimeID = @RunTimeID,
											 @PipelineID = @PipelineID,
											 @FullLoad = @FullLoad,
											 @Debug = @Debug
			END
	END

	IF @AuditType = 'Accuracy'
	BEGIN
		IF @ChunkOut=1
			BEGIN /* Recursive check that chunks out dataset and logs results */
				EXEC etl.AuditAccuracyBatch_I @ParentRunTimeID = @ParentRunTimeID,
											  @RunTimeID = @RunTimeID,
											  @PipelineID = @PipelineID,
											  @FullLoad = @FullLoad,
											  @Debug = @Debug
			END
		ELSE
			BEGIN
				/* Scans once and logs results */
				EXEC etl.AuditAccuracy_I @ParentRunTimeID = @ParentRunTimeID,
										 @RunTimeID = @RunTimeID,
										 @PipelineID = @PipelineID,
										 @FullLoad = @FullLoad,
										 @Debug = @Debug
			END
	END

	IF (@AuditType = 'Accuracy' OR @AuditType = 'Completeness')
	BEGIN
		/* Check alerts against logged data */
		EXEC etl.AuditColumnLogCheck @RunTimeID = @RunTimeID,
									 @PipelineID = @PipelineID,
									 @AuditType = @AuditType
	END

	IF @AuditType = 'Timeliness'
	BEGIN
		/* Execute timeliness check against Pipelines */
		EXEC etl.AuditTimelinessRunTime_I @PipelineID = @PipelineID
		EXEC etl.AuditTimelinessPipelineCheck

	END

	IF @AuditType = 'Custom'
	BEGIN
		EXEC etl.AuditCustomCheck @ParentRunTimeID = @ParentRunTimeID,
								  @RunTimeID = @RunTimeID,
								  @PipelineID = @PipelineID,
								  @Debug = @Debug
	END

END