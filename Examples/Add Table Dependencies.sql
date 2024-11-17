USE ETL_Metadata
GO

DECLARE @SourceAddresses AS etl.SourceAddresses
INSERT INTO @SourceAddresses (FullAddress, AddressType)
VALUES 
('ETL_Metadata.dbo.Package', 'Database')

EXEC [etl].[AddressDependency_DI] @TargetFullAddress = 'ETL_Metadata.fact.test',
							  @TargetAddressType = 'Database',
							  @SourceAddresses = @SourceAddresses

/* Validate */
SELECT * FROM etl.Address
SELECT 
s.FullAddress,
'>>>>>>>>>>>>>>>>',
t.FullAddress
FROM etl.AddressDependency AS ad
INNER JOIN etl.Address AS s
	ON s.AddressID = ad.SourceAddressID
INNER JOIN etl.Address AS t
	ON t.AddressID = ad.TargetAddressID