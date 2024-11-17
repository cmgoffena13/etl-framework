USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.RunTime_I
GO
CREATE PROCEDURE etl.RunTime_I @PipelineName VARCHAR(150),
									   @PipelineType VARCHAR(150),
									   @FullLoad BIT = NULL,
									   @SourceFullAddress VARCHAR(150) = NULL,
									   @SourceAddressType VARCHAR(150) = NULL,
									   @TargetFullAddress VARCHAR(150) = NULL,
									   @TargetAddressType VARCHAR(150) = NULL,
									   @NextWaterMark VARCHAR(50) = NULL,
									   @ParentRunTimeID BIGINT = NULL,
									   @WaterMark VARCHAR(50) = NULL OUTPUT,
									   @RunTimeID BIGINT OUTPUT,
									   @PipelineID INT = NULL OUTPUT,
									   @LoadLineage BIT = 0 OUTPUT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @Active BIT,
			@ErrorMessage VARCHAR(100)

	/* Create Pipeline if needed, increment watermark if needed. Grab PipelineID. */
	EXEC etl.Pipeline_UI @PipelineName = @PipelineName,
					    @PipelineType = @PipelineType,
						@SourceFullAddress = @SourceFullAddress,
						@SourceAddressType = @SourceAddressType,
						@TargetFullAddress = @TargetFullAddress,
						@TargetAddressType = @TargetAddressType,
						@NextWaterMark = @NextWaterMark,
						@WaterMark = @WaterMark OUTPUT,
					    @PipelineID = @PipelineID OUTPUT,
						@LoadLineage = @LoadLineage OUTPUT,
						@Active = @Active OUTPUT

	IF @Active = 0
	BEGIN
		SET @ErrorMessage = 'Pipeline ''' + @PipelineName + ''' is marked inactive, execution must be bypassed in the Pipeline if @Active = 0.' + CHAR(13) + 
							'You can achieve this using the etl.PipelineActive_S stored procedure to check the active flag before execution.';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Create Pipeline run time */
	INSERT INTO etl.RunTime (
		ParentRunTimeID, 
		PipelineID, 
		PipelineStart,
		FullLoad,
		WaterMark,
		NextWaterMark)
	VALUES (
		@ParentRunTimeID, 
		@PipelineID,
		SYSDATETIMEOFFSET(),
		@FullLoad,
		@WaterMark,
		@NextWaterMark)

	SET @RunTimeID = SCOPE_IDENTITY()

END