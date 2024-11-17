USE ETL_Metadata
GO

EXEC etl.DeclareDeprecation_UID @FullAddress = 'ETL_Metadata.test.TargetStage',
								@Revert = 0

SELECT * FROM etl.DeprecationDeclared

EXEC etl.DeprecationFinal_Notify @WaitingDays = 0
/*
EXEC etl.DeprecationFinal_D @FullAddress = 'ETL_Metadata.test.TargetStage'
--EXEC etl.Pipeline_D @PipelineName = 'Test Controller Source Sync'
SELECT * FROM etl.Pipeline
*/
