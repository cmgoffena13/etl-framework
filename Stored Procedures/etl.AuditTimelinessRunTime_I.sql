USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.AuditTimelinessRunTime_I
GO
CREATE PROCEDURE etl.AuditTimelinessRunTime_I @PipelineID INT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @SecondsThreshold INT,
			@NextWaterMark BIGINT,
			@WaterMark BIGINT,
			@PipelineArgsVar VARCHAR(4000),
			@Timestamp DATETIMEOFFSET(2) = SYSDATETIMEOFFSET()

	DECLARE @PipelineArgs TABLE (
		WaterMark BIGINT,
		PipelineArgs VARCHAR(4000)
	)

	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	SELECT @NextWaterMark = MAX(RunTimeID) 
	FROM etl.RunTime 
	WHERE PipelineEnd <= @Timestamp
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED

	UPDATE etl.Pipeline
	SET NextWaterMark = @NextWaterMark
	OUTPUT deleted.WaterMark, deleted.PipelineArgs
	INTO @PipelineArgs
	WHERE PipelineID = @PipelineID

	SET @WaterMark = (SELECT ISNULL(WaterMark, 0) FROM @PipelineArgs)
	SET @PipelineArgsVar = (SELECT PipelineArgs FROM @PipelineArgs)

	/* Default alert threshold, any run time over 30 minutes */
	SET @SecondsThreshold =ISNULL(JSON_VALUE(@PipelineArgsVar, '$.info.audit.timeliness_run_time_check_seconds'), 1800)

	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	DROP TABLE IF EXISTS #Results
	;WITH CTE AS (
	SELECT
	RunTimeID
	FROM etl.RunTime
	WHERE RunTimeID > @WaterMark
		AND RunTimeID <= @NextWaterMark
	)
	SELECT
	rt.RunTimeID,
	rt.PipelineID,
	rt.RunTimeSeconds
	INTO #Results
	FROM etl.RunTime AS rt
	INNER JOIN CTE
		ON CTE.RunTimeID = rt.RunTimeID
	WHERE rt.RunTimeSeconds > @SecondsThreshold
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED

	INSERT INTO etl.TimelinessRunTimeLog (RunTimeID, PipelineID, RunTimeSeconds, SecondsThreshold)
	SELECT
	RunTimeID,
	PipelineID,
	RunTimeSeconds,
	@SecondsThreshold
	FROM #Results AS r
	WHERE NOT EXISTS (
		SELECT 1/0
		FROM etl.TimelinessRunTimeLog AS t
		WHERE t.RunTimeID = r.RunTimeID
	)

END