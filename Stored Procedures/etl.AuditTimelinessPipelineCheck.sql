USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.AuditTimelinessPipelineCheck
GO
CREATE PROCEDURE etl.AuditTimelinessPipelineCheck
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @Now DATETIMEOFFSET(2) = SYSDATETIMEOFFSET(),
			@MaxDML DATETIMEOFFSET(2),
			@DatePart VARCHAR(20),
			@Number INT,
			@CalculatedTime DATETIMEOFFSET(2)

	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	DROP TABLE IF EXISTS #Pipelines
	;WITH CTE AS (
	SELECT
	p.PipelineID,
	pt.TimelyNumber,
	pt.TimelyDatePart
	FROM etl.PipelineType AS pt
	INNER JOIN etl.Pipeline AS p
		ON p.PipelineTypeID = pt.PipelineTypeID
	WHERE pt.MuteTimelyCheck = 0
	)
	SELECT
	p.PipelineID,
	p.PipelineName,
	p.LastTargetInsert,
	p.LastTargetUpdate,
	p.LastTargetDelete,
	p.TimelyNumber AS ChildTimelyNumber,
	p.TimelyDatePart AS ChildTimelyDatePart,
	pt.TimelyNumber AS ParentTimelyNumber,
	pt.TimelyDatePart AS ParentTimelyDatePart,
	0 AS Complete
	INTO #Pipelines
	FROM etl.Pipeline AS p
	INNER JOIN CTE AS pt
		ON pt.PipelineID = p.PipelineID
	WHERE p.MuteTimelyCheck = 0
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED

	DROP TABLE IF EXISTS #FailResults
	CREATE TABLE #FailResults (
		PipelineID INT,
		PipelineName VARCHAR(150),
		LastDML DATETIMEOFFSET(2),
		TimelyNumber INT,
		TimelyDatePart VARCHAR(20)
	)

	DECLARE @PipelineID INT,
			@PipelineName VARCHAR(150),
			@LastTargetInsert DATETIMEOFFSET(2),
			@LastTargetUpdate DATETIMEOFFSET(2),
			@LastTargetDelete DATETIMEOFFSET(2),
			@ChildTimelyNumber INT,
			@ChildTimelyDatePart VARCHAR(20),
			@ParentTimelyNumber INT,
			@ParentTimelyDatePart VARCHAR(20)

	DECLARE timely_cursor CURSOR FOR
		SELECT
		PipelineID,
		PipelineName,
		LastTargetInsert,
		LastTargetUpdate,
		LastTargetDelete,
		ChildTimelyNumber,
		ChildTimelyDatePart,
		ParentTimelyNumber,
		ParentTimelyDatePart
		FROM #Pipelines
		WHERE Complete=0
		ORDER BY PipelineID

	OPEN timely_cursor
	FETCH NEXT FROM timely_cursor INTO
	@PipelineID, @PipelineName, @LastTargetInsert, @LastTargetUpdate, @LastTargetDelete, 
	@ChildTimelyNumber, @ChildTimelyDatePart, @ParentTimelyNumber, @ParentTimelyDatePart 

	WHILE @@FETCH_STATUS = 0
	BEGIN

		SELECT @MaxDML = MAX(TimelyValue) 
		FROM (VALUES(@LastTargetInsert), (@LastTargetUpdate), (@LastTargetDelete)) AS SUB(TimelyValue)

		SET @DatePart = ISNULL(@ChildTimelyDatePart, @ParentTimelyDatePart)
		SET @Number = ISNULL(@ChildTimelyNumber, @ParentTimelyNumber)

		SET @CalculatedTime = CASE WHEN @DatePart = 'MINUTE' THEN DATEADD(MINUTE, @Number, @MaxDML)
								   WHEN @DatePart = 'HOUR'   THEN DATEADD(HOUR, @Number, @MaxDML)
								   WHEN @DatePart = 'DAY'    THEN DATEADD(DAY, @Number, @MaxDML)
								   WHEN @DatePart = 'WEEK'   THEN DATEADD(WEEK, @Number, @MaxDML)
								   WHEN @DatePart = 'MONTH'  THEN DATEADD(MONTH, @Number, @MaxDML)
								   WHEN @DatePart = 'YEAR'   THEN DATEADD(YEAR, @Number, @MaxDML)
								   END

		IF @CalculatedTime < @Now
			INSERT INTO #FailResults (PipelineID, PipelineName, LastDML, TimelyNumber, TimelyDatePart)
			VALUES (@PipelineID, @PipelineName, @MaxDML, @Number, CASE WHEN @Number = 1 THEN LOWER(@DatePart) ELSE CONCAT(LOWER(@DatePart), 's') END)

		UPDATE #Pipelines
		SET Complete=1
		WHERE PipelineID = @PipelineID

		FETCH NEXT FROM timely_cursor INTO
		@PipelineID, @PipelineName, @LastTargetInsert, @LastTargetUpdate, @LastTargetDelete, 
		@ChildTimelyNumber, @ChildTimelyDatePart, @ParentTimelyNumber, @ParentTimelyDatePart 

	END
			
	CLOSE timely_cursor
	DEALLOCATE timely_cursor

	DROP TABLE #Pipelines

	IF EXISTS (SELECT 1/0 FROM #FailResults)
	BEGIN

		DECLARE @ErrorMessage VARCHAR(4000) = 'The following Pipelinees failed their timeliness checks: ' + CHAR(13) + ''
		SELECT @ErrorMessage += STRING_AGG('' + CAST(PipelineID AS VARCHAR(20)) + ': ''' + 
						  PipelineName + ''' has not had a DML operation within the timeframe: ' + CAST(TimelyNumber AS VARCHAR(10)) + ' ' + TimelyDatePart + 
						  '; Last DML Operation: ' + CAST(LastDML AS VARCHAR(50)) + ';', CHAR(13))
		FROM #FailResults
		
		DROP TABLE #FailResults;

		/* TODO: Configure Database Mail */

		/* Throw alert to stop Pipeline/pipeline */
		THROW 51000, @ErrorMessage, 1;
	END
END