USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.AuditCustomCheck
GO
CREATE PROCEDURE etl.AuditCustomCheck @ParentRunTimeID BIGINT = NULL,
									  @RunTimeID BIGINT,
									  @PipelineID INT,
									  @Debug BIT = 0
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DROP TABLE IF EXISTS #CustomRules
	;WITH CTE AS (
	SELECT
	AuditCustomRuleID
	FROM etl.AuditCustomRule
	WHERE PipelineID = @PipelineID
		AND Active = 1
	)
	SELECT
	a.AuditCustomRuleID,
	a.PipelineID,
	p.PipelineName,
	a.CustomSQL,
	a.CustomAlertMessage,
	a.InfoAlert,
	a.ErrorAlert,
	0 AS Complete /* For cursor work */
	INTO #CustomRules
	FROM etl.AuditCustomRule AS a
	INNER JOIN CTE
		ON CTE.AuditCustomRuleID = a.AuditCustomRuleID
	INNER JOIN etl.Pipeline AS p
		ON p.PipelineID = a.PipelineID

	DROP TABLE IF EXISTS #FailResults
	CREATE TABLE #FailResults (
		PipelineID INT,
		PipelineName VARCHAR(150),
		AuditCustomRuleID INT,
		CustomAlertMessage VARCHAR(400),
		InfoAlert BIT,
		ErrorAlert BIT
	)

	DECLARE @AuditCustomRuleID INT, 
			@PipelineName VARCHAR(150), 
			@SQLQuery NVARCHAR(MAX), 
			@CustomAlertMessage VARCHAR(400),
			@InfoAlert BIT,
			@ErrorAlert BIT,
			@Success BIT
	DECLARE custom_cursor CURSOR FOR
		SELECT
		AuditCustomRuleID,
		PipelineName,
		CustomSQL,
		CustomALertMessage,
		InfoAlert,
		ErrorAlert
		FROM #CustomRules
		WHERE Complete = 0
		ORDER BY AuditCustomRuleID

	OPEN custom_cursor
	FETCH NEXT FROM custom_cursor INTO
	@AuditCustomRuleID, @PipelineName, @SQLQuery, @CustomAlertMessage, @InfoAlert, @ErrorAlert

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @Success = NULL
		
		IF @Debug = 1
		BEGIN
			PRINT @SQLQuery
		END
		ELSE
		BEGIN

			INSERT INTO etl.AuditCustomLog (RunTimeID, AuditCustomRuleID, CustomQueryStart)
			VALUES (@RunTimeID, @AuditCustomRuleID, SYSDATETIMEOFFSET())

			SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
			EXEC sys.sp_executesql @SQLQuery, 
				N'@Success BIT OUTPUT', 
				@Success = @Success OUTPUT
			SET TRANSACTION ISOLATION LEVEL READ COMMITTED

			IF @Success IS NULL
				SET @Success = 0

			;WITH CTE AS (
			SELECT
			AuditCustomLogID
			FROM etl.AuditCustomLog
			WHERE RunTimeID = @RunTimeID
				AND AuditCustomRuleID = @AuditCustomRuleID
			)
			UPDATE acl
			SET CustomQueryEnd = SYSDATETIMEOFFSET(),
				RunTimeSeconds = DATEDIFF(SECOND, CustomQueryStart, SYSDATETIMEOFFSET()),
				Completed = 1,
				Success = @Success
			FROM etl.AuditCustomLog acl
			INNER JOIN CTE 
				ON CTE.AuditCustomLogID = acl.AuditCustomLogID

		END

		IF (@Success = 0)
		BEGIN
			INSERT INTO #FailResults (PipelineID, AuditCustomRuleID, PipelineName, CustomAlertMessage, InfoAlert, ErrorAlert)
			VALUES (@PipelineID, @AuditCustomRuleID, @PipelineName, @CustomAlertMessage, @InfoAlert, @ErrorAlert)
		END
		
		UPDATE #CustomRules
		SET Complete=1
		WHERE AuditCustomRuleID = @AuditCustomRuleID

		FETCH NEXT FROM custom_cursor INTO
		@AuditCustomRuleID, @PipelineName, @SQLQuery, @CustomAlertMessage, @InfoAlert, @ErrorAlert

	END

	CLOSE custom_cursor
	DEALLOCATE custom_cursor

	DROP TABLE #CustomRules

	IF EXISTS (SELECT 1/0 FROM #FailResults WHERE InfoAlert = 1)
	BEGIN

		DECLARE @InfoMessage VARCHAR(4000) = 'The following custom rules for ' + CAST(@PipelineID AS VARCHAR(50)) + ': ''' + @PipelineName + ''' have failed info checks:' + CHAR(13) + ''
		SELECT @InfoMessage += STRING_AGG('AuditCustomRuleID: ' + CAST(AuditCustomRuleID AS VARCHAR(50)) + ' - CustomInfoMessage: ' + CAST(CustomAlertMessage AS VARCHAR(400)) + '', CHAR(13))
		FROM #FailResults
		WHERE InfoAlert = 1

		/* TODO: Configure Database Mail */
		PRINT @InfoMessage
	END

	IF EXISTS (SELECT 1/0 FROM #FailResults WHERE ErrorAlert = 1)
	BEGIN

		DECLARE @ErrorMessage VARCHAR(4000) = 'The following custom rules for ' + CAST(@PipelineID AS VARCHAR(50)) + ': ''' + @PipelineName + ''' have failed error checks:' + CHAR(13) + ''
		SELECT @ErrorMessage += STRING_AGG('AuditCustomRuleID: ' + CAST(AuditCustomRuleID AS VARCHAR(50)) + ' - CustomErrorMessage: ' + CAST(CustomAlertMessage AS VARCHAR(400)) + '', CHAR(13))
		FROM #FailResults
		WHERE InfoAlert = 1;

		/* TODO: Configure Database Mail */
		
		/* Throw alert to stop Pipeline/pipeline */
		THROW 51000, @ErrorMessage, 1;

	END
END