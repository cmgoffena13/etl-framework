USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.AuditColumnLogCheck
GO
CREATE PROCEDURE etl.AuditColumnLogCheck @RunTimeID BIGINT,
										 @PipelineID INT,
										 @AuditType VARCHAR(150)
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @Success BIT = 1,
			@PipelineStart DATETIMEOFFSET(2),
			@DateRecorded DATE,
			@HourRecorded TINYINT,
			@SamplesNeeded INT,
			@AuditTypeID TINYINT

	/* Cannot depend on specific ID, so seek on text */
	SELECT @AuditTypeID = AuditTypeID
	FROM etl.AuditType
	WHERE AuditType = @AuditType

	/* Using timestamp of audit Pipeline for date/hour comparison */
	SELECT @PipelineStart = PipelineStart
	FROM etl.RunTime
	WHERE RunTimeID = @RunTimeID

	/* Grab required number of samples before alerting starts */
	SELECT @SamplesNeeded = ISNULL(JSON_VALUE(PipelineArgs, '$.info.audit.samples_needed'), 10)
	FROM etl.Pipeline
	WHERE PipelineID = @PipelineID

	SET @DateRecorded = CAST(@PipelineStart AS DATE)
	SET @HourRecorded = DATEPART(HOUR, @PipelineStart)

	/* Grab alert information from rules for audit type */
	DROP TABLE IF EXISTS #Alerts
	;WITH CTE AS (
	SELECT
	AuditColumnRuleID
	FROM etl.AuditColumnRule
	WHERE PipelineID = @PipelineID
		AND AuditTypeID = @AuditTypeID
		AND Active=1
	)
	SELECT
	acr.AuditColumnRuleID,
	acr.LookbackDays,
	acr.StdDeviationFactor,
	acr.InfoAlert,
	acr.ErrorAlert,
	acr.LastAuditColumnLogID,
	0 AS Complete
	INTO #Alerts
	FROM etl.AuditColumnRule AS acr
	INNER JOIN CTE
		ON CTE.AuditColumnRuleID = acr.AuditColumnRuleID

	DECLARE @AuditColumnRuleID INT,
			@LookbackDays INT,
			@StdDeviationFactor DECIMAL(17,2),
			@InfoAlert BIT,
			@ErrorAlert BIT,
			@LastAuditColumnLogID BIGINT

	DECLARE alert_cursor CURSOR FOR
		SELECT
		AuditColumnRuleID,
		LookbackDays,
		StdDeviationFactor,
		InfoAlert,
		ErrorAlert,
		LastAuditColumnLogID
		FROM #Alerts
		WHERE Complete=0
		ORDER BY AuditColumnRuleID

	OPEN alert_cursor
	FETCH NEXT FROM alert_cursor INTO
		@AuditColumnRuleID, 
		@LookbackDays,
		@StdDeviationFactor, 
		@InfoAlert, 
		@ErrorAlert, 
		@LastAuditColumnLogID

	DECLARE @Date DATE,
			@ChangeThreshold DECIMAL(20,4),
			@ResultScore DECIMAL(20,4),
			@Count INT,
			@Threshold DECIMAL(20,4)

	DROP TABLE IF EXISTS #Results 
	CREATE TABLE #Results (
		AuditColumnRuleID INT,
		AuditColumnLogID BIGINT,
		ResultScore DECIMAL(20,4),
		Threshold DECIMAL(20,4),
		Success BIT,
		InfoAlert BIT,
		ErrorAlert BIT
	)
				
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		/* Grab result score we're evaluating */
		SELECT @ResultScore = ResultScore
		FROM etl.AuditColumnLog
		where AuditColumnLogID = @LastAuditColumnLogID

		/* Set how far back to get samples */
		SET @Date = DATEADD(DAY, -(@LookbackDays), @DateRecorded)
		
		/* Calculate change threshold and number of samples, default is 1 standard deviation */
		SELECT @ChangeThreshold = ISNULL(STDEV(ResultScore) * @StdDeviationFactor, 0),
		       @Count = COUNT(ResultScore)
		FROM etl.AuditColumnLog
		WHERE AuditColumnLogID != @LastAuditColumnLogID
			AND AuditColumnRuleID = @AuditColumnRuleID
			AND HourRecorded = @HourRecorded
			AND DateRecorded >= @Date

		/* If a change threshold of .2, then anything below .8 should be out of norm */
		SET @Threshold = 1 - @ChangeThreshold
		IF @ResultScore < @Threshold
			SET @Success = 0

		/* If not enough run samples, not enough to evaluate */
		IF @Count <= @SamplesNeeded
			SET @Success = NULL

		INSERT INTO #Results (
		AuditColumnRuleID,
		AuditColumnLogID,
		ResultScore,
		Threshold,
		Success,
		InfoAlert,
		ErrorAlert
		)
		VALUES (
		@AuditColumnRuleID,
		@LastAuditColumnLogID,
		@ResultScore,
		@Threshold,
		@Success,
		@InfoAlert,
		@ErrorAlert
		)

		/* Update log with evaluation result */
		UPDATE etl.AuditColumnLog
		SET Threshold = @Threshold,
			Success = @Success
		WHERE AuditColumnLogID = @LastAuditColumnLogID

		UPDATE #Alerts
		SET Complete=1
		WHERE AuditColumnRuleID = @AuditColumnRuleID

		FETCH NEXT FROM alert_cursor INTO
			@AuditColumnRuleID, 
			@LookbackDays,
			@StdDeviationFactor, 
			@InfoAlert, 
			@ErrorAlert, 
			@LastAuditColumnLogID

	END

	CLOSE alert_cursor
	DEALLOCATE alert_cursor

	/* Email alerts */
	DECLARE @Message VARCHAR(4000)
	IF EXISTS (SELECT 1/0 FROM #Results WHERE Success = 0 AND InfoAlert = 1)
	BEGIN

		SELECT @Message = MAX(CONCAT_WS('.', ap.DatabaseName, ap.SchemaName, ap.TableName)) + 
		' - failed info checks on these columns: ' + CHAR(13) + 
		STRING_AGG('' + @AuditType + ': ' + acr.AuditColumn, CHAR(13))
		FROM #Results AS r
		INNER JOIN etl.AuditColumnRule AS acr
			ON acr.AuditColumnRuleID = r.AuditColumnRuleID
		INNER JOIN etl.Pipeline AS p
			ON p.PipelineID = acr.PipelineID
		INNER JOIN etl.Address AS ap
			ON ap.AddressID = p.TargetAddressID
		WHERE r.Success = 0
			AND r.InfoAlert = 1

		/* TODO: Configure Database Mail */
		PRINT @Message

	END
	IF EXISTS (SELECT 1/0 FROM #Results WHERE Success = 0 AND ErrorAlert = 1)
	BEGIN
		SELECT @Message = MAX(CONCAT_WS('.', ap.DatabaseName, ap.SchemaName, ap.TableName)) + 
		' - failed error checks on these columns: ' + CHAR(13) + 
		STRING_AGG('' + @AuditType + ': ' + acr.AuditColumn, CHAR(13))
		FROM #Results AS r
		INNER JOIN etl.AuditColumnRule AS acr
			ON acr.AuditColumnRuleID = r.AuditColumnRuleID
		INNER JOIN etl.Pipeline AS p
			ON p.PipelineID = acr.PipelineID
		INNER JOIN etl.Address AS ap
			ON ap.AddressID = p.TargetAddressID
		WHERE r.Success = 0
			AND r.ErrorAlert = 1;


		/* TODO: Configure Database Mail */

		/* Throw alert to stop Pipeline/pipeline */
		THROW 51000, @Message, 1;

	END
END