USE [ETL_Metadata]
GO
/****** Object:  StoredProcedure [etl].[AddressPath_SI]    Script Date: 1/16/2024 10:10:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

DROP PROCEDURE IF EXISTS etl.Address_SI
GO
CREATE PROCEDURE [etl].[Address_SI] @FullAddress VARCHAR(150),
								   @AddressType VARCHAR(150),
								   @AddressID INT OUTPUT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	SELECT @AddressID = AddressID
	FROM etl.Address
	WHERE FullAddress = @FullAddress

	/* If address cannot be found, create it */
	IF @AddressID IS NULL
	BEGIN
		DECLARE @AddressTypeID TINYINT,
				@DatabaseName VARCHAR(50) = NULL,
				@SchemaName VARCHAR(50) = NULL,
				@TableName VARCHAR(50) = NULL

		SELECT @AddressTypeID = AddressTypeID
		FROM etl.AddressType
		WHERE AddressType = @AddressType

		IF @AddressTypeID IS NULL
		BEGIN
			DECLARE @ErrorMessage VARCHAR(100) = 'Unable to find AddressType: ' + @AddressType + '';
			THROW 51000, @ErrorMessage, 1;
		END

		IF @AddressType = 'Database'
		BEGIN
			SET @DatabaseName = PARSENAME(@FullAddress, 3)
			SET @SchemaName = PARSENAME(@FullAddress, 2)
			SET @TableName = PARSENAME(@FullAddress, 1)
		END

		INSERT INTO etl.Address (FullAddress, AddressTypeID, DatabaseName, SchemaName, TableName)
		VALUES (@FullAddress, @AddressTypeID, @DatabaseName, @SchemaName, @TableName)

		SET @AddressID = SCOPE_IDENTITY()
	END
END