USE [ETL_Metadata]
GO
/****** Object:  StoredProcedure [etl].[AddressDependency_DI]    Script Date: 1/17/2024 10:13:55 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

DROP PROCEDURE IF EXISTS etl.AddressDependency_DI
GO
CREATE PROCEDURE [etl].[AddressDependency_DI] @TargetFullAddress VARCHAR(150),
											 @TargetAddressType VARCHAR(150),
											 @SourceAddresses etl.SourceAddresses READONLY
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @TargetAddressID INT,
			@SourceFullAddress VARCHAR(150),
			@SourceAddressType VARCHAR(150),
			@SourceAddressID INT

	DECLARE @SourceIDs TABLE (SourceAddressID INT)

	/* Insert source addresses that we don't have yet */
	DECLARE address_cursor CURSOR FAST_FORWARD FOR
		SELECT
		FullAddress,
		AddressType
		FROM @SourceAddresses
		ORDER BY FullAddress

	OPEN address_cursor
	FETCH NEXT FROM address_cursor INTO
	@SourceFullAddress, @SourceAddressType
	
	WHILE @@FETCH_STATUS = 0
	BEGIN
		/* This is required to have the variable be overwritten correctly */
		SET @SourceAddressID = NULL 

		EXEC etl.Address_SI @FullAddress = @SourceFullAddress,
							@AddressType = @SourceAddressType,
							@AddressID = @SourceAddressID OUTPUT

		INSERT INTO @SourceIDs (SourceAddressID)
		VALUES (@SourceAddressID)

		FETCH NEXT FROM address_cursor INTO
		@SourceFullAddress, @SourceAddressType
	END

	CLOSE address_cursor
	DEALLOCATE address_cursor

	/* Grab target address */
	SELECT @TargetAddressID = AddressID
	FROM etl.Address
	WHERE FullAddress = @TargetFullAddress

	/* If target address can't be found, create it */
	IF @TargetAddressID IS NULL
	BEGIN
		EXEC etl.Address_SI @FullAddress = @TargetFullAddress,
							 @AddressType = @TargetAddressType,
							 @AddressID = @TargetAddressID OUTPUT
	END

	/* Create table so we can cross join for combinations */
	DECLARE @TargetAddressTable TABLE (TargetAddressID INT)
	INSERT INTO @TargetAddressTable (TargetAddressID) VALUES (@TargetAddressID)

	/* Create combinations */
	DROP TABLE IF EXISTS #TableDependency
	CREATE TABLE #TableDependency (
		SourceAddressID INT NOT NULL,
		TargetAddressID INT NOT NULL
	)
	INSERT INTO #TableDependency (SourceAddressID, TargetAddressID)
	SELECT
	s.SourceAddressID,
	t.TargetAddressID
	FROM @SourceIDs AS s
	CROSS JOIN @TargetAddressTable AS t

	/* Delete any source addresses that are not found in inputted list */
	DELETE td
	FROM etl.AddressDependency AS td
	WHERE td.TargetAddressID = @TargetAddressID
		AND NOT EXISTS (
			SELECT 1/0
			FROM #TableDependency AS d
			WHERE d.SourceAddressID = td.SourceAddressID
		)

	/* Input given combinations into dependency table */
	INSERT INTO etl.AddressDependency (SourceAddressID, TargetAddressID)
	SELECT
	SourceAddressID,
	TargetAddressID
	FROM #TableDependency AS td
	WHERE NOT EXISTS (
		SELECT 1/0
		FROM etl.AddressDependency AS ad
		WHERE ad.SourceAddressID = td.SourceAddressID
			AND ad.TargetAddressID = td.TargetAddressID
		)

END