USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.AuditCompleteness_I
GO
CREATE PROCEDURE etl.AuditCompleteness_I @ParentRunTimeID BIGINT = NULL,
										 @RunTimeID BIGINT,
										 @PipelineID INT,
										 @FullLoad BIT,
										 @Debug BIT = 0

AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @Timestamp DATETIMEOFFSET(2),
			@DateRecorded DATE,
			@HourRecorded TINYINT,
			@ChunkOut BIT = 0,
			@AuditType VARCHAR(150) = 'Completeness',
			@AuditTypeID TINYINT,
			@ErrorMessage VARCHAR(100)

	IF @FullLoad = 0 AND @ParentRunTimeID IS NULL
	BEGIN
		SET @ErrorMessage = '@ParentRunTimeID is required for incremental audits, @FullLoad=0';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Can't depend on certain ID, so seek based upon text */
	SELECT @AuditTypeID = AuditTypeID
	FROM etl.AuditType
	WHERE AuditType = @AuditType

	/* Use Pipeline start of the audit Pipeline to determine date/hour comparisons */
	SELECT @Timestamp = PipelineStart
	FROM etl.RunTime
	WHERE RunTimeID = @RunTimeID

	SET @DateRecorded = CAST(@Timestamp AS DATE)
	SET @HourRecorded = DATEPART(HOUR, @Timestamp)

	/* Grab necessary completeness rule information */
	DROP TABLE IF EXISTS #CompletenessRules
	;WITH CTE AS (
	SELECT
	AuditColumnRuleID
	FROM etl.AuditColumnRule
	WHERE PipelineID = @PipelineID
		AND AuditTypeID = @AuditTypeID
		AND Active = 1
	)
	SELECT
	a.AuditColumnRuleID,
	ap.DatabaseName,
	ap.SchemaName,
	ap.TableName,
	ap.PrimaryKey,
	a.AuditColumn
	INTO #CompletenessRules
	FROM etl.AuditColumnRule AS a
	INNER JOIN CTE ON
		CTE.AuditColumnRuleID = a.AuditColumnRuleID
	INNER JOIN etl.Pipeline AS p
		ON p.PipelineID = a.PipelineID
	INNER JOIN etl.Address AS ap
		ON ap.AddressID = p.TargetAddressID

	/* Begin string construction for dynamic query */
	DECLARE @TargetTable NVARCHAR(150) = (SELECT DISTINCT CONCAT_WS('.', DatabaseName, SchemaName, TableName) FROM #CompletenessRules),
			@PrimaryKey NVARCHAR(50) = (SELECT DISTINCT PrimaryKey FROM #CompletenessRules), 
			@ColumnList NVARCHAR(MAX),
			@Agg_List NVARCHAR(MAX),
			@RecordCount INT

	SELECT
	@ColumnList = STRING_AGG(AuditColumn, ',') WITHIN GROUP (ORDER BY AuditColumnRuleID),
	@Agg_List = STRING_AGG( 'SUM( CASE WHEN ' + AuditColumn + ' IS NULL THEN 1 ELSE 0 END) AS ' + AuditColumn + '', ',') WITHIN GROUP (ORDER BY AuditColumnRuleID)
	FROM #CompletenessRules

	DECLARE @SQLQuery NVARCHAR(MAX)

	DROP TABLE IF EXISTS #Results
	CREATE TABLE #Results (
		RecordCount BIGINT,
		ColumnName VARCHAR(50),
		RecordViolationCount BIGINT
	)

	IF @FullLoad=0
	BEGIN
		/* Only audit the last run time of the parent */
		SET @SQLQuery = '
		;WITH CTE AS (
		SELECT
		' + @PrimaryKey + '
		FROM ' + @TargetTable + '
		WHERE RunTimeID = ' + CAST(@ParentRunTimeID AS VARCHAR(50)) + '
		)

		INSERT INTO #Results (RecordCount, ColumnName, RecordViolationCount)
		SELECT
		RecordCount,
		ColumnName,
		RecordViolationCount
		FROM (
		SELECT
		COUNT(*) AS RecordCount,
		{agg_list}
		FROM ' + @TargetTable + '
		INNER JOIN CTE
			ON CTE.' + @PrimaryKey + ' = ' + @TargetTable + '.' + @PrimaryKey + '
		) AS SubQuery
		UNPIVOT
		( RecordViolationCount FOR ColumnName IN (
		{column_list}
		)) AS pvt'
	END
	ELSE
	BEGIN
		/* simple unpivot */
		SET @SQLQuery = '
		INSERT INTO #Results (RecordCount, ColumnName, RecordViolationCount)
		SELECT
		RecordCount,
		ColumnName,
		RecordViolationCount
		FROM (
		SELECT
		COUNT(*) AS RecordCount,
		{agg_list}
		FROM ' + @TargetTable + '
		) AS SubQuery
		UNPIVOT
		( RecordViolationCount FOR ColumnName IN (
		{column_list}
		)) AS pvt'
	END

	/* inject constructed column list and agg list into string */
	SET @SQLQuery = REPLACE(@SQLQuery, '{agg_list}', @Agg_List)
	SET @SQLQuery = REPLACE(@SQLQuery, '{column_list}', @ColumnList)

	IF @Debug=1
	BEGIN
		PRINT @SQLQuery
	END
	ELSE
	BEGIN
		SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
		EXEC sys.sp_executesql @SQLQuery
		SET TRANSACTION ISOLATION LEVEL READ COMMITTED
	END

	IF @Debug=1
	BEGIN

		SELECT
		@RunTimeID AS RunTimeID,
		cr.AuditColumnRuleID,
		@DateRecorded AS DateRecorded,
		@HourRecorded AS HourRecorded,
		r.RecordCount,
		r.RecordViolationCount,
		CAST(CAST((r.RecordCount - r.RecordViolationCount) AS DECIMAL) / CAST(r.RecordCount AS DECIMAL) AS DECIMAL(20,4)) AS ResultScore
		FROM #Results AS r
		INNER JOIN #CompletenessRules AS cr
			ON cr.AuditColumn = r.ColumnName

	END
	ELSE
	BEGIN
		/* Used to update LastAuditColumnLogID */
		DECLARE @CompletenessTable TABLE (
			AuditColumnRuleID INT,
			AuditColumnLogID BIGINT
		)

		INSERT INTO etl.AuditColumnLog (
			RunTimeID,
			AuditColumnRuleID,
			DateRecorded,
			HourRecorded,
			RecordCount,
			RecordViolationCount,
			ResultScore,
			FullLoad,
			ChunkOut
		)
		OUTPUT inserted.AuditColumnRuleID, inserted.AuditColumnLogID
		INTO @CompletenessTable
		SELECT
		@RunTimeID,
		cr.AuditColumnRuleID,
		@DateRecorded,
		@HourRecorded,
		r.RecordCount,
		r.RecordViolationCount,
		CAST(CAST((r.RecordCount - r.RecordViolationCount) AS DECIMAL) / CAST(r.RecordCount AS DECIMAL) AS DECIMAL(20,4)) AS ResultScore,
		@FullLoad,
		@ChunkOut
		FROM #Results AS r
		INNER JOIN #CompletenessRules AS cr
			ON cr.AuditColumn = r.ColumnName

		DROP TABLE #CompletenessRules
		DROP TABLE #Results

		UPDATE A
		SET LastAuditColumnLogID = t.AuditColumnLogID
		FROM etl.AuditColumnRule AS A
		INNER JOIN @CompletenessTable AS t
			ON t.AuditColumnRuleID = A.AuditColumnRuleID

	END

END