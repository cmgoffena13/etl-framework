USE ETL_Metadata
GO
DROP PROCEDURE IF EXISTS etl.AddressDependency_DI
DROP TYPE IF EXISTS etl.SourceAddresses
CREATE TYPE etl.SourceAddresses AS TABLE (
	FullAddress VARCHAR(150) NOT NULL,
	AddressType VARCHAR(150) NOT NULL
)