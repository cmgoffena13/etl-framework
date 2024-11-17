USE ETL_Metadata
GO

DECLARE @TotalAuditColumnLogRowsDeleted INT,
		@TotalRunTimeRowsDeleted INT,
		@TotalTimelinessRunTimeLogRowsDeleted INT,
		@TotalAuditCustomLogRowsDeleted INT

EXEC etl.LogMaintenance_D @RetentionDays = 90,
						  @TotalAuditColumnLogRowsDeleted = @TotalAuditColumnLogRowsDeleted OUTPUT,
						  @TotalRunTimeRowsDeleted = @TotalRunTimeRowsDeleted OUTPUT,
						  @TotalTimelinessRunTimeLogRowsDeleted = @TotalTimelinessRunTimeLogRowsDeleted OUTPUT,
						  @TotalAuditCustomLogRowsDeleted = @TotalAuditCustomLogRowsDeleted OUTPUT