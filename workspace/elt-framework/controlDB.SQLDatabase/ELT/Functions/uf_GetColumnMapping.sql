-- Function: ELT.uf_GetColumnMapping
-- Purpose: Returns a JSON mapping string for column mappings based on IngestID, L1TransformID, or L2TransformID.
-- The function queries the [ELT].[ColumnMapping] table and constructs a JSON array of source/target column pairs for use in tabular data translation.
-- Only active mappings (ActiveFlag = 1) are included. Returns NULL if no mappings are found.
CREATE FUNCTION [ELT].[uf_GetColumnMapping]
(
	@IngestID int NULL,
	@L1TransformID int NULL,
	@L2TransformID int NULL
	)
RETURNS varchar(max)
AS BEGIN
	DECLARE @mapping NVARCHAR(MAX)
		BEGIN
			WITH 
				cte
			AS
			(
				select 	
					Mapping = CASE 
								WHEN SourceName IS NULL THEN NULL
								ELSE '{"source":{"name":"' + SourceName + '"},"sink":{"name":"' + TargetName + '"}},'
							END
				from
					[ELT].[ColumnMapping]
				WHERE
					IngestID = @IngestID
					or L1TransformID =  @L1TransformID
					or L2TransformID =  @L2TransformID
					and ActiveFlag = 1
			)
			
			SELECT 
				@mapping = CASE 
							WHEN STRING_AGG (Mapping, '') IS NULL THEN NULL
							ELSE COALESCE(concat('{"type":"TabularTranslator","mappings":[', SUBSTRING(STRING_AGG (Mapping, ''),1,LEN(STRING_AGG (Mapping, ''))-1),  ']}'), NULL)
						END
			FROM cte;
		END
				
 -- Return the result of the function
	Return @mapping

END

GO

