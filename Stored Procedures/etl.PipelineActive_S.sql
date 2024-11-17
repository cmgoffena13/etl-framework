USE ETL_Metadata
GO


DROP PROCEDURE IF EXISTS etl.PipelineActive_S
GO
CREATE PROCEDURE etl.PipelineActive_S @PipelineName VARCHAR(150),
									 @Active BIT OUTPUT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	SELECT
	@Active = Active
	FROM etl.Pipeline
	WHERE PipelineName = @PipelineName

	/* If the Pipeline can't be found, assume not created yet and will be created */
	IF @Active IS NULL
		SET @Active = 1

END