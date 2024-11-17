USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.PipelineUser_SI
GO
CREATE PROCEDURE etl.PipelineUser_SI @UserName VARCHAR(150),
									@PipelineUserID INT OUTPUT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	/* Grab PipelineUserID */
	SELECT @PipelineUserID = PipelineUserID
	FROM etl.PipelineUser
	WHERE UserName = @UserName

	/* If cannot be found, create one */
	IF @PipelineUserID IS NULL
	BEGIN
		INSERT INTO etl.PipelineUser (UserName)
		VALUES (@UserName)

		SET @PipelineUserID = SCOPE_IDENTITY()
	END

END