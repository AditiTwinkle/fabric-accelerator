-- Function: ELT.uf_GetTabularTranslatorMappingJson
-- Purpose: Wraps a provided JSON mapping string in the standard Azure Data Factory TabularTranslator schema.
-- Replaces the <MAPPINGS> placeholder in the template with the input mapping JSON, returning a valid TabularTranslator mapping object.
-- Useful for generating explicit schema mappings for ADF copy activities.
CREATE FUNCTION [ELT].[uf_GetTabularTranslatorMappingJson]
(
	@DataMappingJson VARCHAR(MAX)
)
RETURNS VARCHAR(MAX)
AS
BEGIN
	-- Uses standard ADF Explicit Schema Mapping
	-- https://docs.microsoft.com/en-us/azure/data-factory/copy-activity-schema-and-type-mapping
	DECLARE @TabularTranslatorJson VARCHAR(MAX) = '{
		"type": "TabularTranslator",
		"mappings": <MAPPINGS>
	}'
	RETURN REPLACE(@TabularTranslatorJson, '<MAPPINGS>', @DataMappingJson)
END

GO

