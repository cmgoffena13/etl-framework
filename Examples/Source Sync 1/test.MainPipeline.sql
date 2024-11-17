USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS test.MainPipeline
GO
CREATE PROCEDURE test.MainPipeline @FullLoad BIT = 0,
								   @AuditDebug BIT = 0
AS
BEGIN
SET NOCOUNT ON 
SET XACT_ABORT ON

	DECLARE @PipelineName VARCHAR(150) = 'Test Controller Source Sync',
			@PipelineType VARCHAR(150) = 'Controller',
			@RunTimeID BIGINT,
			@PipelineID INT,
			@ExtractionRunTimeID BIGINT,
			@Active BIT

	EXEC etl.PipelineActive_S @PipelineName = @PipelineName,
							 @Active = @Active OUTPUT

	IF @Active = 1
	BEGIN

		/* Step 1: create controller Pipeline run time */
		EXEC etl.RunTime_I @PipelineName = @PipelineName,
									@PipelineType = @PipelineType,
									@FullLoad = @FullLoad,
									@RunTimeID = @RunTimeID OUTPUT,
									@PipelineID = @PipelineID OUTPUT
									
		/* Step 2: Trigger extraction Pipeline */
		EXEC test.ExtractPipeline @ParentRunTimeID = @RunTimeID,
								  @FullLoad = @FullLoad,
								  @RunTimeID = @ExtractionRunTimeID OUTPUT

		/* Step 3: Trigger audit Pipeline */
		EXEC test.AuditPipeline @ParentRunTimeID = @ExtractionRunTimeID,
								@FullLoad = 0,
								@Debug=@AuditDebug

		/* Step 4: Trigger publish Pipeline */
		EXEC test.PublishPipeline @ParentRunTimeID = @RunTimeID

		/* Step 5: complete controller Pipeline run time */
		EXEC etl.RunTime_U @RunTimeID = @RunTimeID
	END
END