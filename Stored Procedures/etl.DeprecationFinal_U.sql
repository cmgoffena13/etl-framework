USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.DeprecationFinal_U
GO
CREATE PROCEDURE etl.DeprecationFinal_U @FullAddress VARCHAR(150)
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @AddressID INT,
			@Message VARCHAR(100)

	SELECT
	@AddressID = AddressID
	FROM etl.Address
	WHERE FullAddress = @FullAddress

	IF @AddressID IS NULL
	BEGIN
		SET @Message = 'Address ''' + @FullAddress + ''' is unable to be found.';
		THROW 51000, @Message, 1;
	END

	IF NOT EXISTS (
		SELECT 1/0
		FROM etl.DeprecationDeclared
		WHERE AddressID = @AddressID
	)
	BEGIN
		SET @Message = 'Address ''' + @FullAddress + ''' is unable to be found in etl.DeprecationDeclared';
		THROW 51000, @Message, 1;
	END

	UPDATE etl.DeprecationDeclared
	SET TicketsCreated = 1
	WHERE AddressID = @AddressID

	SET @Message = 'Address ''' + @FullAddress + ''' marked as TicketsCreated = 1'
	PRINT @Message

	SELECT
	'etl.DeprecationDeclared' AS TableName,
	*
	FROM etl.DeprecationDeclared
	WHERE AddressID = @AddressID

END