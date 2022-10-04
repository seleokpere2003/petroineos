
--1. Type casting and some data cleaning based on data analysis

DROP TABLE IF EXISTS dbo._1_Airbnb_Open_Data_clean_type_cast
SELECT 
	 is_duplicate = CASE WHEN sq.duplicate_order = 1 THEN 'No' ELSE 'Yes' END
	,SQ.*
	INTO dbo._1_Airbnb_Open_Data_clean_type_cast
FROM (
SELECT 
	listing_id = CAST(base.id AS INT),
    listing_name = ISNULL(NULLIF(base.NAME,''), 'Unknown') ,
    [host_id] = CAST(base.[host id] AS BIGINT),
    host_identity_verified =  ISNULL(NULLIF(base.host_identity_verified,''), 'Unknown') ,
    [host_name] = ISNULL(NULLIF( base.[host name],''), 'Unknown') ,
    neighbourhood_group = ISNULL(NULLIF(
								 CASE base.[neighbourhood group] 
									  WHEN 'brookln' THEN 'Brooklyn'
									  WHEN 'manhatan' THEN 'Manhattan'
									  ELSE base.[neighbourhood group] 
								  END	
									,'')
								, 'Unknown'
							   ) ,
    neighbourhood = ISNULL(NULLIF(base.neighbourhood,''), 'Unknown') ,
    latitude = IIF(ISNUMERIC(base.lat) = 1,CAST(base.lat AS DECIMAL(10,5)),NULL),
    longitude = IIF(ISNUMERIC(base.long) = 1,CAST(base.long AS DECIMAL(10,5)),NULL),
    country = ISNULL(NULLIF(IIF(base.country = '' AND base.[country code] = 'US', 'United States', base.country),''), 'Unknown'),
    country_code = ISNULL(NULLIF(base.[country code],''), 'Unknown'),
    instant_bookable = CASE base.instant_bookable
							WHEN 'TRUE' THEN 'Yes'
							WHEN 'FALSE' THEN 'No'
					   ELSE 'Unknown'
					   END,
    cancellation_policy = CASE base.cancellation_policy
								WHEN 'strict' THEN 'Strict'
								WHEN 'moderate' THEN 'Moderate'
								WHEN 'flexible' THEN 'Flexible'
						   ELSE 'Unknown'
						   END,
    room_type = base.[room type],
    construction_year = CAST(base.[Construction year] AS INT),
    room_price_in_$ = CAST(REPLACE(TRANSLATE(base.price,'$,','  '),' ','') AS INT),
    service_fee_in_$ = CAST(REPLACE(TRANSLATE(base.[service fee],'$,','  '),' ','') AS INT),
    minimum_nights = CAST(base.[minimum nights] AS INT),
    number_of_reviews = CAST(base.[number of reviews] AS INT),
    last_review_date = IIF(ISDATE(base.[last review]) = 1, CAST([last review] AS DATE), NULL),	
    reviews_per_month = IIF(ISNUMERIC(base.[reviews per month]) = 1, CAST([reviews per month] AS DECIMAL(10,2)),0),
    review_rate_number = IIF(ISNUMERIC(base.[review rate number]) = 1,  CAST(base.[review rate number] AS INT), -1),
    host_listings_count = CAST(base.[calculated host listings count] AS INT),
    availability_365 = CAST(base.[availability 365] AS INT),
    house_rules = ISNULL(NULLIF(base.house_rules,''),'No Rules'),
    license = ISNULL(NULLIF(base.license,''),'No License'),
	-- Data Quality Checks (Duplicates)
	duplicate_counter = COUNT(1) OVER (PARTITION BY base.id),
	duplicate_order = ROW_NUMBER() OVER (PARTITION BY base.id ORDER BY base.id)

FROM dbo.Airbnb_Open_Data base    
) sq
;

-- 2. Additional Columns derivations and transformation

DROP TABLE IF EXISTS _2_Airbnb_Open_Data_transformed
SELECT sq.*
		-- Banded columns order
	,construction_period_order = CASE construction_period
									WHEN 'Period Before 2010' THEN 1
									WHEN 'Period 2010 - 2015' THEN 2
									WHEN 'Period 2015 - 2020' THEN 3
									WHEN 'Period After 2020' THEN 4
									ELSE  5
								END	
	,minimum_nights_band_order = CASE sq.minimum_nights_band
									WHEN 'Less than or equal to 0 nights' THEN 1
									WHEN '1 - 5 nights' THEN 2
									WHEN '5 - 10 nights' THEN 3
									WHEN '10 - 15 nights' THEN 4
									WHEN '15 - 20 nights' THEN 5
									WHEN '20 - 30 nights' THEN 6
									WHEN '30+ nights' THEN 7
									ELSE 8 
								 END
	,number_of_reviews_band_order = CASE number_of_reviews_band 
										WHEN 'No Reviews' THEN 1
										WHEN '1 - 10 reviews' THEN 2
										WHEN '10 - 20 reviews' THEN 3
										WHEN '20 - 30 reviews' THEN 4
										WHEN '30 - 40 reviews' THEN 5
										WHEN '40 - 50 reviews' THEN 6
										WHEN '50 - 100 reviews' THEN 7
										ELSE 8
									END
	,reviews_per_month_band_order = CASE reviews_per_month_band 
										WHEN 'No reviews' THEN 1
										WHEN '0.01 - 1.00 reviews' THEN 2
										WHEN '1 - 10 reviews' THEN 3
										WHEN '10 - 50 reviews' THEN 4
										WHEN '50+ reviews' THEN 5
										ELSE 6
									END	
	,availability_365_band_order = CASE sq.availability_365_band	
										WHEN 'Less than or equal to 0 avail.' THEN 1
										WHEN '1 - 50 days avail.' THEN 2
										WHEN '50 - 100 days avail.' THEN 3
										WHEN '100 - 200 days avail.' THEN 4
										WHEN '200 - 366 days avail.' THEN 5
										WHEN '366+ days avail.' THEN 6
										ELSE 7
									END
	INTO _2_Airbnb_Open_Data_transformed
FROM (
SELECT ctc.*,
    -- Enhanced columns
	review_rate = IIF(ctc.review_rate_number = -1, 'No Rating',  CONCAT(ctc.review_rate_number,' Star')),
   -- Banding Columns
	construction_period = CASE WHEN ctc.construction_year > 0 AND ctc.construction_year  < 2010 THEN 'Period Before 2010'
							  WHEN ctc.construction_year BETWEEN 2010 AND 2015 THEN 'Period 2010 - 2015'
							  WHEN ctc.construction_year BETWEEN 2015 AND 2020 THEN 'Period 2015 - 2020'
							  WHEN ctc.construction_year >= 2020 THEN 'Period After 2020'
						 ELSE 'Unknown' 
						 END,
	minimum_nights_band = CASE WHEN ctc.minimum_nights <= 0 THEN 'Less than or equal to 0 nights'
							   WHEN ctc.minimum_nights BETWEEN 1 AND 5 THEN '1 - 5 nights'
							   WHEN ctc.minimum_nights BETWEEN 5 AND 10 THEN '5 - 10 nights'
							   WHEN ctc.minimum_nights BETWEEN 10 AND 15 THEN '10 - 15 nights'
							   WHEN ctc.minimum_nights BETWEEN 15 AND 20 THEN '15 - 20 nights'
							   WHEN ctc.minimum_nights BETWEEN 20 AND 30 THEN '20 - 30 nights'
							   WHEN ctc.minimum_nights >= 30 THEN '30+ nights'
						  ELSE 'Unknown'
						  END,
	number_of_reviews_band = CASE WHEN ctc.number_of_reviews = 0 THEN 'No Reviews'
								  WHEN ctc.number_of_reviews BETWEEN 1 AND 10 THEN '1 - 10 reviews'
								  WHEN ctc.number_of_reviews BETWEEN 10 AND 20 THEN '10 - 20 reviews'
								  WHEN ctc.number_of_reviews BETWEEN 20 AND 30 THEN '20 - 30 reviews'
								  WHEN ctc.number_of_reviews BETWEEN 30 AND 40 THEN '30 - 40 reviews'
								  WHEN ctc.number_of_reviews BETWEEN 40 AND 50 THEN '40 - 50 reviews'
								  WHEN ctc.number_of_reviews BETWEEN 50 AND 100 THEN '50 - 100 reviews'
								  WHEN ctc.number_of_reviews >= 100 THEN '100+ reviews'
								  ELSE 'Unknown'
							  END,
	reviews_per_month_band = CASE WHEN ctc.reviews_per_month <= 0 THEN 'No reviews'
							      WHEN ctc.reviews_per_month BETWEEN 0.01 AND 1.00 THEN '0.01 - 1.00 reviews'								
								  WHEN ctc.reviews_per_month BETWEEN 1 AND 10 THEN '1 - 10 reviews'
								  WHEN ctc.reviews_per_month BETWEEN 10 AND 50 THEN '10 - 50 reviews'
								  WHEN ctc.reviews_per_month >= 50 THEN '50+ reviews'
								  ELSE 'Unknown'
							 END,
   availability_365_band = CASE WHEN ctc.availability_365 <= 0  THEN 'Less than or equal to 0 avail.'
								WHEN ctc.availability_365 BETWEEN 1 AND 50 THEN '1 - 50 days avail.'
								WHEN ctc.availability_365 BETWEEN 50 AND 100 THEN '50 - 100 days avail.'
								WHEN ctc.availability_365 BETWEEN 100 AND 200 THEN '100 - 200 days avail.'
								WHEN ctc.availability_365 BETWEEN 200 AND 366 THEN '200 - 366 days avail.'
								WHEN ctc.availability_365 >= 366 THEN '366+ days avail.'
								ELSE 'Unknown'
							END,
	has_listing_rules = CASE WHEN ctc.house_rules = 'No Rules' THEN 'No' ELSE 'Yes' END


FROM dbo._1_Airbnb_Open_Data_clean_type_cast ctc
) sq

-- add dimension IDs

DROP TABLE IF EXISTS dbo._3_Airbnb_Open_Data_Load
SELECT *
	,d_listing_profile_id = CONVERT(VARCHAR(100),
								HASHBYTES('SHA2_256',
								CONCAT(neighbourhood_group
									 ,neighbourhood
									 ,country
									 ,country_code
									 ,instant_bookable
									 ,cancellation_policy
									 ,room_type
									 ,has_listing_rules
									 ,construction_period
									 ,construction_period_order
									 ,minimum_nights_band
									 ,minimum_nights_band_order
									 ,availability_365_band
									 ,availability_365_band_order
									 ,latitude
									 ,longitude
								)),2)
	,d_listing_details_profile_id = CONVERT(VARCHAR(100),
										HASHBYTES('SHA2_256',
										CONCAT(listing_name
												,construction_year	
												,minimum_nights
												,availability_365	
										)),2)
	 ,d_host_profile_id = CONVERT(VARCHAR(100),
										HASHBYTES('SHA2_256',
										CONCAT(host_identity_verified
												,review_rate
												,review_rate_number
												,number_of_reviews_band
												,number_of_reviews_band_order
												,reviews_per_month_band
												,reviews_per_month_band_order	
									)),2)
				INTO dbo._3_Airbnb_Open_Data_Load			
FROM dbo._2_Airbnb_Open_Data_transformed


/************
These equivalents are created inside of Power query of the Power BI file
**********/


-- D_Listing_Profile
SELECT *
	,cnt = COUNT(1) OVER (PARTITION BY sq.d_listing_profile_id)
FROM (
SELECT DISTINCT
	  d_listing_profile_id
	 ,neighbourhood_group
	 ,neighbourhood
	 ,country
	 ,country_code
	 ,instant_bookable
	 ,cancellation_policy
	 ,room_type
	 ,has_listing_rules
	 ,construction_period
	 ,construction_period_order
	 ,minimum_nights_band
	 ,minimum_nights_band_order
	 ,availability_365_band
	 ,availability_365_band_order
	 ,latitude
	 ,longitude
FROM dbo._3_Airbnb_Open_Data_Load	
) sq
ORDER BY cnt desc;


-- d_Listing_details
SELECT DISTINCT
     d_listing_details_profile_id
	,listing_name
	,construction_year	
	,minimum_nights
	,availability_365	
FROM dbo._3_Airbnb_Open_Data_Load;


-- d_host_profile
SELECT	
	DISTINCT
	 d_host_profile_id
	,host_identity_verified
	,review_rate
	,review_rate_number
	,number_of_reviews_band
	,number_of_reviews_band_order
	,reviews_per_month_band
	,reviews_per_month_band_order
FROM dbo._3_Airbnb_Open_Data_Load

-- f_listings
SELECT 
	 is_duplicate
	,listing_id
	,[host_id]
	,d_listing_profile_id
	,d_host_profile_id
	,d_listing_details_profile_id
	,[room_price_in_$]
	,[service_fee_in_$]
	,host_listings_count
FROM dbo._3_Airbnb_Open_Data_Load






SELECT TOP(10) *
FROM dbo._2_Airbnb_Open_Data_transformed
WHERE host_id = 29531702698
;

SELECT host_id, COUNT(listing_id)
FROM dbo._2_Airbnb_Open_Data_transformed
WHERE is_duplicate = 'No'
GROUP BY HOST_ID
ORDER BY 2 DESC