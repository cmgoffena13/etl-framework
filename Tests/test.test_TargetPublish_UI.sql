USE ETL_Metadata
GO



ALTER PROCEDURE test.test_TargetPublish_UI @RunTimeID BIGINT,
											@PipelineID INT,
											@Inserts INT OUTPUT,
											@Updates INT OUTPUT,
											@SoftDeletes INT OUTPUT,
											@TotalRows INT OUTPUT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @BatchSize INT,
			@MergeLimit BIGINT,
			@ProcedureName VARCHAR(150) = 'test.test_TargetPublish_UI',
			@PipelineUserID INT,
			@DefaultBatchSize INT = 500000,
			@DefaultMergeLimit INT = 2000000000

	SELECT 
	@BatchSize = ISNULL(JSON_VALUE(PipelineArgs, '$.info.publish.batch_size'), @DefaultBatchSize),
	@MergeLimit = ISNULL(JSON_VALUE(PipelineArgs, '$.info.publish.merge_limit'), @DefaultMergeLimit) /* 2 billion */
	FROM etl.Pipeline
	WHERE PipelineID = @PipelineID

	EXEC etl.PipelineUser_SI @UserName = @ProcedureName,
							@PipelineUserID = @PipelineUserID OUTPUT

	/* Grab queue information */
	DECLARE @MinID BIGINT, @MaxID BIGINT
	SELECT TOP 1 @MaxID = TargetStageID FROM test.TargetStage ORDER BY TargetStageID DESC
	SELECT TOP 1 @MinID = TargetStageID FROM test.TargetStage ORDER BY TargetStageID ASC

	DECLARE @QueueSize INT, @TotalChunks INT
	SET @QueueSize = (@MaxID - @MinID + 1) /* Assumption that there's no gaps in identity col */
	IF @QueueSize > @MergeLimit
		SET @QueueSize = @MergeLimit
	SET @TotalChunks = CEILING( ( CAST(@QueueSize AS DECIMAL) / CAST(@BatchSize AS DECIMAL) ) )
	
	DROP TABLE IF EXISTS #Changes
	CREATE TABLE #Changes (
		RowKey BIGINT,
		TargetStageID BIGINT,
		EventID INT,
		EventValue DECIMAL(17,2),
		EventReason VARCHAR(50),
		CreatedDate DATETIMEOFFSET(2),
		CTOperation CHAR(1),
		RowHash VARBINARY(32),
		RecordCount INT
	)

	DROP TABLE IF EXISTS #Updates
	CREATE TABLE #Updates (
		ActiveInSourceSystem BIT
	)

	SET @Inserts = 0
	SET @Updates = 0
	SET @SoftDeletes = 0
	SET @TotalRows = 0

	WHILE @TotalChunks > 0
	BEGIN

		/* Grab Min/Max of chunk */
		DECLARE @MaxChangeID BIGINT, @MinChangeID BIGINT
		;WITH CTE AS (
			SELECT TOP (@BatchSize)
			TargetStageID
			FROM test.TargetStage
			ORDER BY TargetStageID
		)
		SELECT /* Small enough you don't need failsafe Order Bys */
		@MaxChangeID = MAX(TargetStageID),
		@MinChangeID = MIN(TargetStageID)
		FROM CTE

		TRUNCATE TABLE #Changes
		;WITH CTE AS (
			SELECT
			TargetStageID,
			EventID,
			EventValue,
			EventReason,
			CreatedDate,

			RowHash,
			CTOperation,
			ROW_NUMBER() OVER (PARTITION BY EventID ORDER BY TargetStageID DESC) AS RowNumber,
			COUNT(*) OVER (ORDER BY (SELECT NULL)) AS RecordCount
			FROM test.TargetStage
			WHERE TargetStageID BETWEEN @MinChangeID AND @MaxChangeID
		)
		INSERT INTO #Changes (
			RowKey,
			TargetStageID,
			EventID,
			EventValue,
			EventReason,
			CreatedDate,

			RowHash,
			CTOperation,
			RecordCount
		)
		SELECT
		t.RowKey,
		CTE.TargetStageID,
		CTE.EventID,
		CTE.EventValue,
		CTE.EventReason,
		CTE.CreatedDate,

		CTE.RowHash,
		CTE.CTOperation,
		CTE.RecordCount
		FROM CTE
		LEFT JOIN test.TargetPublish AS t
			ON t.EventID = CTE.EventID
		WHERE CTE.RowNumber = 1

		/* Grab total records in chunk */
		SELECT @TotalRows += (SELECT MAX(RecordCount) FROM #Changes)

		/* Updates in chunk */
		TRUNCATE TABLE #Updates
		UPDATE landing
		SET EventValue = stage.EventValue,
			EventReason = stage.EventReason,
			CreatedDate = stage.CreatedDate,

			ModifiedOn = SYSDATETIMEOFFSET(),
			WaterMarkDate = CASE WHEN stage.CTOperation = 'D' THEN WaterMarkDate ELSE SYSDATETIMEOFFSET() END,
			ModifiedBy = @PipelineUserID,
			RunTimeID = @RunTimeID,
			ActiveInSourceSystem = CASE WHEN stage.CTOperation = 'D' THEN 0 ELSE ActiveInSourceSystem END
		OUTPUT inserted.ActiveInSourceSystem
		INTO #Updates
		FROM test.TargetPublish AS landing
		INNER JOIN #Changes AS stage
			ON stage.RowKey = landing.RowKey
		WHERE stage.RowHash != landing.RowHash

		SET @Updates += (SELECT COUNT(*) FROM #Updates WHERE ActiveInSourceSystem = 1)
		SET @SoftDeletes += (SELECT COUNT(*) FROM #Updates WHERE ActiveInSourceSystem = 0)

		/* Insert where not exists in target */
		INSERT INTO test.TargetPublish (
			EventID,
			EventValue,
			EventReason,
			CreatedDate,

			RowHash,
			CreatedOn,
			CreatedBy,
			RunTimeID
		)
		SELECT
		stage.EventID,
		stage.EventValue,
		stage.EventReason,
		stage.CreatedDate,

		stage.RowHash,
		SYSDATETIMEOFFSET(),
		@PipelineUserID,
		@RunTimeID
		FROM #Changes AS stage
		WHERE stage.RowKey IS NULL

		SET @Inserts += @@ROWCOUNT

		/* Delete out chunk from queue */
		DELETE stage
		FROM test.TargetStage AS stage
		WHERE stage.TargetStageID <= @MaxChangeID

		SET @TotalChunks -= 1
	END

END