THROW 51000, 'Make sure to designate the right database and then comment this out', 1;
THROW 51000, 'Enable ChangeTracking on the database and then comment this out', 1;
THROW 51000, 'I would recommend changing the database to Simple logging if localhost, then comment this out', 1;

/* Create test Schema */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'test')
BEGIN
	EXEC('CREATE SCHEMA test AUTHORIZATION dbo')
END

/* Create source table, stage table, and publish table */
DROP TABLE IF EXISTS test.SourceTable
CREATE TABLE test.SourceTable (
	EventID BIGINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_test_SourceTable_EventID PRIMARY KEY,
	EventValue DECIMAL(17,2) NOT NULL,
	EventReason VARCHAR(50) NOT NULL,
	CreatedDate DATETIMEOFFSET(2) NOT NULL
)
ALTER TABLE test.SourceTable
	ENABLE CHANGE_TRACKING

DROP TABLE IF EXISTS test.TargetStage
CREATE TABLE test.TargetStage (
	TargetStageID INT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_test_TargetStage_TargetStageID PRIMARY KEY,
	EventID BIGINT,
	EventValue DECIMAL(17,2),
	EventReason VARCHAR(50),
	CreatedDate DATETIMEOFFSET(2),
	CTOperation CHAR(1) NOT NULL,
	CTOperationContext VARCHAR(128),
	RowHash VARBINARY(32) NOT NULL,
	RunTimeID BIGINT NOT NULL
)
CREATE NONCLUSTERED INDEX IX_test_TargetStage_RunTimeID
	ON test.TargetStage (RunTimeID)

DROP TABLE IF EXISTS test.TargetPublish
CREATE TABLE test.TargetPublish (
	RowKey BIGINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_test_TargetPublish_RowKey PRIMARY KEY,
	EventID BIGINT,
	EventValue DECIMAL(17,2),
	EventReason VARCHAR(50),
	CreatedDate DATETIMEOFFSET(2),
	RowHash VARBINARY(32) NOT NULL,
	CreatedOn DATETIMEOFFSET(2) NOT NULL
		CONSTRAINT DF_test_TargetPublish_CreatedOn DEFAULT(SYSDATETIMEOFFSET()),
	CreatedBy INT NOT NULL
		CONSTRAINT DF_test_TargetPublish_CreatedBy DEFAULT(-1),
	ModifiedOn DATETIMEOFFSET(2) NULL,
	ModifiedBy INT NULL,
	WaterMarkDate DATETIMEOFFSET(2) NOT NULL
		CONSTRAINT DF_test_TargetPublish_WaterMarkDate DEFAULT(SYSDATETIMEOFFSET()),
	RunTimeID BIGINT NOT NULL,
	ActiveInSourceSystem BIT NOT NULL
		CONSTRAINT DF_test_TargetPublish_ActiveInSourceSystem DEFAULT(1)
)
CREATE NONCLUSTERED INDEX IX_test_TargetPublish_EventID
	ON test.TargetPublish (EventID)
CREATE NONCLUSTERED INDEX IX_test_TargetPublish_RunTimeID
	ON test.TargetPublish (RunTimeID)


/* Create Merge procedure */
DROP PROCEDURE IF EXISTS test.test_TargetPublish_UI
GO
CREATE PROCEDURE test.test_TargetPublish_UI @RunTimeID BIGINT,
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
			@PipelineUserID INT

	SELECT 
	@BatchSize = ISNULL(JSON_VALUE(PipelineArgs, '$.info.publish.batch_size'), 500000),
	@MergeLimit = ISNULL(JSON_VALUE(PipelineArgs, '$.info.publish.merge_limit'), 2000000000) /* 2 billion */
	FROM etl.Pipeline
	WHERE PipelineID = @PipelineID

	EXEC etl.PipelineUser_SI @UserName = @ProcedureName,
							@PipelineUserID = @PipelineUserID OUTPUT

	/* Grab queue information */
	DECLARE @MinID BIGINT, @MaxID BIGINT
	SELECT @MaxID = MAX(TargetStageID) FROM test.TargetStage
	SELECT @MinID = MIN(TargetStageID) FROM test.TargetStage

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

	DECLARE @UpdatesTable TABLE (
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
		SELECT
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
		DELETE FROM @UpdatesTable
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
		INTO @UpdatesTable
		FROM test.TargetPublish AS landing
		INNER JOIN #Changes AS stage
			ON stage.RowKey = landing.RowKey
		WHERE stage.RowHash != landing.RowHash

		SET @Updates += (SELECT COUNT(*) FROM @UpdatesTable WHERE ActiveInSourceSystem = 1)
		SET @SoftDeletes += (SELECT COUNT(*) FROM @UpdatesTable WHERE ActiveInSourceSystem = 0)

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