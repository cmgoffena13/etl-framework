USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.AuditCustomRule_I
GO
CREATE PROCEDURE etl.AuditCustomRule_I @PipelineName VARCHAR(150),
										@CustomSQL NVARCHAR(MAX),
										@CustomAlertMessage VARCHAR(400),
										@InfoAlert BIT = NULL,
										@ErrorAlert BIT = NULL
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @ErrorMessage VARCHAR(200),
			@PipelineID INT

	/* Check inputs */
	IF (@InfoAlert IS NULL OR @ErrorAlert IS NULL) OR (@InfoAlert=1 AND @ErrorAlert=1) OR (@InfoAlert=0 AND @ErrorAlert=0)
	BEGIN
		SET @ErrorMessage = 'Must declare @InfoAlert and @ErrorAlert variables, one must be active';
		THROW 51000, @ErrorMessage, 1;
	END
	IF (@CustomSQL IS NULL OR @CustomAlertMessage IS NULL)
	BEGIN
		SET @ErrorMessage = '@CustomSQL and @CustomAlertMessage are both required';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Begin script */
	DECLARE @RuleStartDate DATE = CAST(SYSDATETIMEOFFSET() AS DATE),
			@RuleEndDate DATE = '9999-12-31',
			@QueryHash VARBINARY(32) = CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', @CustomSQL))

	SELECT @PipelineID = PipelineID
	FROM etl.Pipeline
	WHERE PipelineName = @PipelineName

	DROP TABLE IF EXISTS #Queries
	SELECT
	CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CustomSQL)) AS QueryHash
	INTO #Queries
	FROM etl.AuditCustomRule
	WHERE Active=1

	IF EXISTS (
		SELECT 1/0
		FROM #Queries
		WHERE QueryHash = @QueryHash
	)
	BEGIN
		SET @ErrorMessage = 'There is already an active custom rule with this query';
		THROW 51000, @ErrorMessage, 1;
	END

	INSERT INTO etl.AuditCustomRule (PipelineID, CustomSQL, CustomAlertMessage, InfoAlert, ErrorAlert, RuleStartDate, RuleEndDate)
	VALUES (
	@PipelineID,
	@CustomSQL,
	@CustomAlertMessage,
	@InfoAlert,
	@ErrorAlert,
	@RuleStartDate,
	@RuleEndDate
	)

END