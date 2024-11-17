USE ETL_Metadata
GO

EXEC test.MainPipeline @FullLoad = 0,
					   @AuditDebug = 0

SELECT * FROM etl.Pipeline