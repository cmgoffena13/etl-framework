USE [ETL_Metadata]
GO
/****** Object:  StoredProcedure [etl].[AuditRule_I]    Script Date: 1/16/2024 10:17:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

DROP PROCEDURE IF EXISTS etl.AuditColumnRule_I
GO
CREATE PROCEDURE [etl].[AuditColumnRule_I] @PipelineName VARCHAR(150),
									@AuditType VARCHAR(150),
									@DatabaseName VARCHAR(50) = NULL,
									@SchemaName VARCHAR(50) = NULL,
									@TableName VARCHAR(50) = NULL,
									@PrimaryKey VARCHAR(50),
									@AuditColumn VARCHAR(50),
									@MinimumBound VARCHAR(50) = NULL,
									@MaximumBound VARCHAR(50) = NULL,
									@LookbackDays INT = 30,
									@StdDeviationFactor DECIMAL(17,2) = 1.0,
									@InfoAlert BIT = NULL,
									@ErrorAlert BIT = NULL,
									@Override BIT = 0
											 
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @ErrorMessage VARCHAR(200),
			@AuditTypeID TINYINT

	SELECT @AuditTypeID = AuditTypeID
	FROM etl.AuditType
	WHERE AuditType = @AuditType

	/* Check inputs and provide feedback for proper rule creation */
	IF @AuditTypeID IS NULL
	BEGIN
		SET @ErrorMessage = 'Unable to find AuditType: ' + @AuditType + '';
		THROW 51000, @ErrorMessage, 1;
	END
	IF (@AuditType = 'Accuracy' OR @AuditType = 'Completeness') AND (@PrimaryKey IS NULL OR @AuditColumn IS NULL)
	BEGIN
		SET @ErrorMessage = 'Accuracy and Completeness checks require the @PrimaryKey and @AuditColumn variables.';
		THROW 51000, @ErrorMessage, 1;
	END
	IF (@AuditType = 'Accuracy' OR @AuditType = 'Completeness') AND (@LookbackDays IS NULL OR @StdDeviationFactor IS NULL)
	BEGIN
		SET @ErrorMessage = 'Accuracy and Completeness checks require the @LookbackDays and @StdDeviationFactor variables.';
		THROW 51000, @ErrorMessage, 1;

	END
	IF @AuditType = 'Accuracy' AND @MinimumBound IS NULL AND @MaximumBound IS NULL 
	BEGIN
		SET @ErrorMessage = '@MinimumBound and @MaximumBound are both NULL, one is required for Accuracy check';
		THROW 51000, @ErrorMessage, 1;
	END
	IF @AuditType = 'Completeness' AND @Override = 1
	BEGIN
		SET @ErrorMessage = 'Cannot override a Completeness check';
		THROW 51000, @ErrorMessage, 1;
	END
	IF @AuditType = 'Completeness' AND (@MinimumBound IS NOT NULL OR @MaximumBound IS NOT NULL)
	BEGIN
		SET @ErrorMessage = '@MinimumBound and @MaximumBound are not utilized in Completeness checks';
		THROW 51000, @ErrorMessage, 1;
	END
	IF (@InfoAlert IS NULL OR @ErrorAlert IS NULL) OR (@InfoAlert=0 AND @ErrorAlert=0) OR (@InfoAlert=1 AND @ErrorAlert=1)
	BEGIN
		SET @ErrorMessage = 'Must declare @InfoAlert and @ErrorAlert variables, one must be active';
		THROW 51000, @ErrorMessage, 1;
	END
	IF @AuditType NOT IN ('Completeness', 'Accuracy')
	BEGIN
		SET @ErrorMessage = 'Column Audits currently only support Completeness and Accuracy checks';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Begin script */
	DECLARE @TargetAddressID INT,
			@PipelineID INT,
			@RuleStartDate DATE = CAST(SYSDATETIMEOFFSET() AS DATE),
			@RuleEndDate DATE = '9999-12-31'

	/* Grab target address from Pipeline */
	SELECT @TargetAddressID = TargetAddressID,
		   @PipelineID = PipelineID
	FROM etl.Pipeline AS p
	WHERE p.PipelineName = @PipelineName

	IF @PipelineID IS NULL
	BEGIN
		SET @ErrorMessage = 'Unable to find Pipeline: ' + @PipelineName;
		THROW 51000, @ErrorMessage, 1;
	END

	/* update target address with inputs if provided, ensures table can be audited */
	UPDATE etl.Address
	SET DatabaseName = CASE WHEN @DatabaseName IS NOT NULL THEN @DatabaseName ELSE DatabaseName END,
		SchemaName = CASE WHEN @SchemaName IS NOT NULL THEN @SchemaName ELSE SchemaName END,
		TableName = CASE WHEN @TableName IS NOT NULL THEN @TableName ELSE TableName END,
		PrimaryKey = @PrimaryKey
	WHERE AddressID = @TargetAddressID

	/* Check if there is already an active rule for the column/audittype */
	IF @Override = 0
	BEGIN
	DECLARE @Message VARCHAR(1000)
	SET @Message = 'Active rule for audit column ''' + @AuditColumn + ''' with type ' + @AuditType + ' already exists.'
	IF EXISTS (
		SELECT 1/0
		FROM etl.AuditColumnRule 
		WHERE PipelineID = @PipelineID
			AND AuditTypeID = @AuditTypeID
			AND AuditColumn = @AuditColumn
			AND Active = 1
	)
		THROW 51000, @Message, 1;
	END
	ELSE
	BEGIN
		/* De-activate current rule if override = 1 */
		UPDATE etl.AuditColumnRule
		SET Active = 0,
			RuleEndDate = CAST(SYSDATETIMEOFFSET() AS DATE)
		WHERE PipelineID = @PipelineID
			AND AuditTypeID = @AuditTypeID
			AND AuditColumn = @AuditColumn
			AND Active = 1
	END

	/* Insert new active rule information */
	INSERT INTO etl.AuditColumnRule(
	PipelineID, 
	AuditTypeID,
	AuditColumn,
	MinimumBound,
	MaximumBound,
	LookbackDays,
	StdDeviationFactor,
	InfoAlert,
	ErrorAlert,
	RuleStartDate, 
	RuleEndDate, 
	Active
	)
	VALUES (
	@PipelineID,
	@AuditTypeID,
	@AuditColumn,
	@MinimumBound,
	@MaximumBound,
	@LookbackDays,
	@StdDeviationFactor,
	@InfoAlert,
	@ErrorAlert,
	@RuleStartDate,
	@RuleEndDate,
	1
	)

END