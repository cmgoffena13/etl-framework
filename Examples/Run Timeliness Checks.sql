USE ETL_Metadata
GO

DECLARE @PipelineName VARCHAR(150) = 'Timeliness Checks',
		@PipelineType VARCHAR(150) = 'Audit',
		@RunTimeID BIGINT,
		@PipelineID INT

EXEC etl.RunTime_I @PipelineName = @PipelineName,
							@PipelineType = @PipelineType,
							@RunTimeID = @RunTimeID OUTPUT,
							@PipelineID = @PipelineID OUTPUT

EXEC etl.Audit_Load @RunTimeID = @RunTimeID,
					@PipelineID = @PipelineID,
					@AuditType = 'Timeliness'

EXEC etl.RunTime_U @RunTimeID = @RunTimeID