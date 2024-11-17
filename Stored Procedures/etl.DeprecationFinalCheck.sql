USE ETL_Metadata
GO


DROP PROCEDURE IF EXISTS etl.DeprecationFinalCheck
GO
CREATE PROCEDURE etl.DeprecationFinalCheck @WaitingDays INT
AS
BEGIN
SET NOCOUNT ON 
SET XACT_ABORT ON

	DECLARE @Now DATETIMEOFFSET(2) = SYSDATETIMEOFFSET(),
			@Message VARCHAR(1000)

	IF @WaitingDays < 0
	BEGIN
		SET @Message = '@WaitingDays variable must be a positive value';
		THROW 51000, @Message, 1;
	END

	/* If any deprecation records past waiting, throw alert */
	IF EXISTS (
		SELECT 1/0
		FROM etl.DeprecationDeclared
		WHERE DATEADD(DAY, @WaitingDays, MarkedForDeprecation) < @Now
			AND TicketsCreated = 0
	)
	BEGIN
		DROP TABLE IF EXISTS #Addresses
		;WITH CTE AS (
		SELECT
		DeprecationDeclaredID
		FROM etl.DeprecationDeclared
		WHERE DATEADD(DAY, @WaitingDays, MarkedForDeprecation) < @Now
			AND TicketsCreated = 0
		)
		SELECT DISTINCT
		FullAddress
		INTO #Addresses
		FROM etl.DeprecationDeclared AS d
		INNER JOIN CTE
			ON CTE.DeprecationDeclaredID = d.DeprecationDeclaredID
		INNER JOIN etl.Address AS a
			ON a.AddressID = d.AddressID;

		/* semantics */
		DECLARE @Day VARCHAR(10)
		IF @WaitingDays = 1
		BEGIN SET @Day = 'day' END
		ELSE BEGIN SET @Day = 'days' END

		SELECT
		@Message = 'The following addresses are ready for final deprecation after waiting ' + CAST(@WaitingDays AS VARCHAR(20)) + ' ' + @Day + ':' + CHAR(13) + STRING_AGG(FullAddress,', ')
		FROM #Addresses;

		/* TODO: Configure Database Mail */

		PRINT @Message
	END
END