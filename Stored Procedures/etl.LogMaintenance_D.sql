USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.LogMaintenance_D
GO
CREATE PROCEDURE etl.LogMaintenance_D @RetentionDays INT,
									  @BatchSize INT = 10000,
									  @TotalAuditColumnLogRowsDeleted INT OUTPUT,
									  @TotalRunTimeRowsDeleted INT OUTPUT,
									  @TotalTimelinessRunTimeLogRowsDeleted INT OUTPUT,
									  @TotalAuditCustomLogRowsDeleted INT OUTPUT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @CurrentDate DATE = CAST(SYSDATETIMEOFFSET() AS DATE),
			@StartingDate DATETIME,
			@StartingDateTimeOffset DATETIMEOFFSET(2),
			@MaxAuditColumnLogID BIGINT,
			@MaxAuditCustomLogID BIGINT,
			@MaxRunTimeID BIGINT,
			@RowCount INT

	SET @TotalAuditColumnLogRowsDeleted = 0
	SET @TotalRunTimeRowsDeleted = 0
	SET @TotalTimelinessRunTimeLogRowsDeleted = 0
	SET @TotalAuditCustomLogRowsDeleted = 0

	SET @StartingDate = DATEADD(DAY, -@RetentionDays, @CurrentDate)
	/*SQL Server 2019 has CURRENT_TIMEZONE() that can be used here instead */
	SET @StartingDateTimeOffset = @StartingDate AT TIME ZONE 'Central Standard Time'
	PRINT @StartingDateTimeOffset

	SELECT @MaxRunTimeID = MAX(pe.RunTimeID)
	FROM  etl.RunTime AS pe
	WHERE pe.PipelineStart <= @StartingDate

	SELECT @MaxAuditColumnLogID = MAX(al.AuditColumnLogID)
	FROM etl.AuditColumnLog AS al
	WHERE RunTimeID <= @MaxRunTimeID

	SELECT @MaxAuditCustomLogID = MAX(al.AuditCustomLogID)
	FROM etl.AuditCustomLog AS al
	WHERE RunTimeID <= @MaxRunTimeID

	/* Delete out audit logs associated with Pipeline run times past cutoff point */
	IF @MaxAuditColumnLogID IS NOT NULL
	BEGIN
		WHILE 1=1
		BEGIN
			DELETE TOP (@BatchSize)
			FROM etl.AuditColumnLog
			WHERE AuditColumnLogID <= @MaxAuditColumnLogID

			SET @RowCount = @@ROWCOUNT
			SET @TotalAuditColumnLogRowsDeleted += @RowCount

			IF @RowCount < @BatchSize
				BREAK
		END
	END

	/* Delete out Pipeline run times timeliness logs past cutoff point */
	IF @MaxRunTimeID IS NOT NULL
	BEGIN
		WHILE 1=1
		BEGIN
			DELETE TOP (@BatchSize)
			FROM etl.TimelinessRunTimeLog
			WHERE RunTimeID <= @MaxRunTimeID

			SET @RowCount = @@ROWCOUNT
			SET @TotalTimelinessRunTimeLogRowsDeleted += @RowCount

			IF @RowCount < @BatchSize
				BREAK
		END
	END

	IF @MaxAuditCustomLogID IS NOT NULL
	BEGIN
		WHILE 1=1
		BEGIN
			DELETE TOP (@BatchSize)
			FROM etl.AuditCustomLog
			WHERE RunTimeID <= @MaxRunTimeID

			SET @RowCount = @@ROWCOUNT
			SET @TotalAuditCustomLogRowsDeleted += @RowCount

			IF @RowCount < @BatchSize
				BREAK
		END
	END

	/* Delete out Pipeline run times past cutoff point */
	IF @MaxRunTimeID IS NOT NULL
	BEGIN
		WHILE 1=1
		BEGIN
			DELETE TOP (@BatchSize)
			FROM etl.RunTime
			WHERE RunTimeID <= @MaxRunTimeID

			SET @RowCount = @@ROWCOUNT
			SET @TotalRunTimeRowsDeleted += @RowCount

			IF @RowCount < @BatchSize
				BREAK
		END
	END
END