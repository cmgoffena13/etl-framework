USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.AuditTimeliness_U
GO
CREATE PROCEDURE etl.AuditTimeliness_U @PipelineName VARCHAR(150),
									   @PipelineType VARCHAR(150),
									   @TimelyNumber INT,
									   @TimelyDatePart VARCHAR(20),
									   @MuteTimelyCheck BIT = 0
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @PipelineID INT,
			@PipelineTypeID TINYINT,
			@ErrorMessage VARCHAR(100)

	IF @PipelineName IS NOT NULL AND @PipelineType IS NOT NULL
	BEGIN
		SET @ErrorMessage = 'Only one input allowed at a time: @PipelineName or @PipelineType';
			THROW 51000, @ErrorMessage, 1;
	END
	IF (@TimelyNumber IS NULL OR @TimelyDatePart IS NULL) AND @MuteTimelyCheck=0
	BEGIN
		SET @ErrorMessage = '@TimelyNumber and @TimelyDartPart are both required to properly update if not muting';
		THROW 51000, @ErrorMessage, 1;
	END

	IF @PipelineName IS NOT NULL
	BEGIN
		SELECT @PipelineID = PipelineID
		FROM etl.Pipeline
		WHERE PipelineName = @PipelineName

		IF @PipelineID IS NULL
		BEGIN
			SET @ErrorMessage = 'Pipeline ''' + @PipelineName + ''' unable to be found';
			THROW 51000, @ErrorMessage, 1;
		END

		UPDATE etl.Pipeline
		SET TimelyNumber = CASE WHEN @TimelyNumber IS NULL THEN TimelyNumber ELSE @TimelyNumber END,
			TimelyDatePart = CASE WHEN @TimelyDatePart IS NULL THEN TimelyDatePart ELSE @TimelyDatePart END,
			MuteTimelyCheck = @MuteTimelyCheck
		WHERE PipelineID = @PipelineID
	END

	IF @PipelineType IS NOT NULL
	BEGIN
		SELECT @PipelineTypeID = PipelineTypeID
		FROM etl.PipelineType
		WHERE PipelineType = @PipelineType

		IF @PipelineTypeID IS NULL
		BEGIN
			SET @ErrorMessage = 'PipelineType ''' + @PipelineType + ''' unable to be found';
			THROW 51000, @ErrorMessage, 1;
		END

		UPDATE etl.PipelineType
		SET TimelyNumber = CASE WHEN @TimelyNumber IS NULL THEN TimelyNumber ELSE @TimelyNumber END,
			TimelyDatePart = CASE WHEN @TimelyDatePart IS NULL THEN TimelyDatePart ELSE @TimelyDatePart END,
			MuteTimelyCheck = @MuteTimelyCheck
		WHERE PipelineTypeID = @PipelineTypeID
	END

END