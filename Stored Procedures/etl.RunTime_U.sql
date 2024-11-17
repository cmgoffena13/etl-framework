USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.RunTime_U
GO
CREATE PROCEDURE etl.RunTime_U @RunTimeID BIGINT,
										@Inserts INT = NULL,
										@Updates INT = NULL,
										@SoftDeletes INT = NULL,
										@TotalRows INT = NULL
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @PipelineEnd DATETIMEOFFSET(2) = SYSDATETIMEOFFSET()
	DECLARE @PipelineTable TABLE (PipelineID INT)

	/* Complete Pipeline run time with DML metadata */
	UPDATE etl.RunTime
	SET PipelineEnd = @PipelineEnd,
		RunTimeSeconds = DATEDIFF(SECOND, PipelineStart, @PipelineEnd),
		Completed = 1,
		Inserts = @Inserts,
		Updates = @Updates,
		SoftDeletes = @SoftDeletes,
		TotalRows = @TotalRows
	OUTPUT deleted.PipelineID INTO @PipelineTable
	WHERE RunTimeID = @RunTimeID

	/* Update Pipeline timeliness for DML operations */
	UPDATE etl.Pipeline
	SET WaterMark = NextWaterMark,
		LastTargetInsert = CASE WHEN @Inserts > 0 THEN @PipelineEnd ELSE LastTargetInsert END,
		LastTargetUpdate = CASE WHEN @Updates > 0 THEN @PipelineEnd ELSE LastTargetUpdate END,
		LastTargetDelete = CASE WHEN @SoftDeletes > 0 THEN @PipelineEnd ELSE LastTargetDelete END,
		LoadLineage = 0
	WHERE PipelineID = (SELECT PipelineID FROM @PipelineTable)

END