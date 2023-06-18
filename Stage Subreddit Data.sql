-- DESCRIPTION: Create stage table, perform QC checks, and convert datatypes in analysis table


--======================================================--
--  1. Select all data into stage table for QC Checks   -- 
--======================================================--
SELECT * 
FROM `SL_Intern_Project_2022.orig_subreddit_data_20220207` 
-- 403,281 records
-- results saved in `SL_Intern_Project_2022.stage_subreddit_data_20220208` 


--======================================================--
--          2a. Perform Quality Checks on Data           -- 
--======================================================--

-- Total Record Count
SELECT COUNT(*)
FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` 
-- 403,281 records

-- Check for duplicate records
SELECT COUNT(*)
FROM (
    SELECT DISTINCT *
    FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208`)
-- 399,237 records -> 4,044 duplicate records... need to investigate dupes

-- gather column names
SELECT column_name
FROM `SL_Intern_Project_2022.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'stage_subreddit_data_20220208'

-- find num dupes
SELECT author,
    created_utc,
    domain,
    full_link,
    id,
    is_crosspostable,
    is_original_content,
    is_reddit_media_domain,
    is_video,
    link_flair_type,
    media_only,
    num_comments,
    num_crossposts,
    retrieved_on,
    score,
    selftext,
    spoiler,
    stickied,
    subreddit,
    subreddit_subscribers,
    subreddit_type,
    title,
    total_awards_received,
    url,
    edited,
    updated_utc,
    gilded,
    upvote_ratio,
    author_id,
    approved_at_utc,
    author_cakeday,
    view_count,
    author_created_utc,
    source_file,
    COUNT(*) AS num_rows
FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208`
GROUP BY author,
    created_utc,
    domain,
    full_link,
    id,
    is_crosspostable,
    is_original_content,
    is_reddit_media_domain,
    is_video,
    link_flair_type,
    media_only,
    num_comments,
    num_crossposts,
    retrieved_on,
    score,
    selftext,
    spoiler,
    stickied,
    subreddit,
    subreddit_subscribers,
    subreddit_type,
    title,
    total_awards_received,
    url,
    edited,
    updated_utc,
    gilded,
    upvote_ratio,
    author_id,
    approved_at_utc,
    author_cakeday,
    view_count,
    author_created_utc,
    source_file
HAVING num_rows > 1
-- 4,044 records returned
-- these 4,044 records can be removed from the final dataset
-- the 985 unique records from r/ArtisanBread will be removed as well
-- expecting 398,252 records for final analysis dataset

-- Identify number of distinct values for each column 
SELECT "author", COUNT(DISTINCT author) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "created_utc", COUNT(DISTINCT created_utc) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "domain", COUNT(DISTINCT domain) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "full_link", COUNT(DISTINCT full_link) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "id", COUNT(DISTINCT id) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "is_crosspostable", COUNT(DISTINCT is_crosspostable) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "is_original_content", COUNT(DISTINCT is_original_content) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "is_reddit_media_domain", COUNT(DISTINCT is_reddit_media_domain) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "is_video", COUNT(DISTINCT is_video) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "link_flair_type", COUNT(DISTINCT link_flair_type) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "media_only", COUNT(DISTINCT media_only) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "num_comments", COUNT(DISTINCT num_comments) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "num_crossposts", COUNT(DISTINCT num_crossposts) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "retrieved_on", COUNT(DISTINCT retrieved_on) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "score", COUNT(DISTINCT score) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "selftext", COUNT(DISTINCT selftext) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "spoiler", COUNT(DISTINCT spoiler) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "stickied", COUNT(DISTINCT stickied) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "subreddit", COUNT(DISTINCT subreddit) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "subreddit_subscribers", COUNT(DISTINCT subreddit_subscribers) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "subreddit_type", COUNT(DISTINCT subreddit_type) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "title", COUNT(DISTINCT title) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "total_awards_received", COUNT(DISTINCT total_awards_received) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "url", COUNT(DISTINCT url) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "edited", COUNT(DISTINCT edited) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "updated_utc", COUNT(DISTINCT updated_utc) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "gilded", COUNT(DISTINCT gilded) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "upvote_ratio", COUNT(DISTINCT upvote_ratio) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "author_id", COUNT(DISTINCT author_id) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "approved_at_utc", COUNT(DISTINCT approved_at_utc) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "author_cakeday", COUNT(DISTINCT author_cakeday) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "view_count", COUNT(DISTINCT view_count) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "author_created_utc", COUNT(DISTINCT author_created_utc) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` UNION ALL
SELECT "source_file", COUNT(DISTINCT source_file) AS unique_values FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208`
-- view_count and approved_at_utc only have 'nan' values, so can drop them from the table
-- only 985 posts for r/ArtisanBread and only one non-null value for "upvote_ratio"
-- *******going to drop all subreddit = ArtisanBread due to weak data, low number of records, and lack of subreddit activity******


/* Column names and their desired data types
author	String
created_utc	INT64 -> DATE
domain	String
full_link	String
id	String
is_crosspostable	String
is_original_content	 String
is_reddit_media_domain	String
is_video	String
link_flair_type	String
media_only	String
num_comments	INT64
num_crossposts	FLOAT64 -> INT64
retrieved_on	FLOAT64 -> INT64 -> DATE
score	INT64
selftext	String
spoiler	String
stickied	String
subreddit	String
subreddit_subscribers	FLOAT64 -> INT64
subreddit_type	String
title	String
total_awards_received	FLOAT64 -> INT64
url	String
edited	String
updated_utc	FLOAT64 -> INT64 -> DATE
gilded	FLOAT64 -> INT64
upvote_ratio	FLOAT64
author_id	String
author_cakeday	String
author_created_utc	FLOAT64 -> INT64 -> Date
source_file	String
*/

-- Perform safe_cast on columns that need new data types to identify any errors in the data
SELECT * FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` WHERE SAFE_CAST(created_utc AS INT64) IS NULL
SELECT * FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` WHERE SAFE_CAST(num_comments AS INT64) IS NULL
SELECT * FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` WHERE SAFE_CAST(num_crossposts AS FLOAT64) IS NULL
SELECT * FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` WHERE SAFE_CAST(retrieved_on AS FLOAT64) IS NULL
SELECT * FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` WHERE SAFE_CAST(score AS FLOAT64) IS NULL
SELECT * FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` WHERE SAFE_CAST(subreddit_subscribers AS FLOAT64) IS NULL
SELECT * FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` WHERE SAFE_CAST(total_awards_received AS FLOAT64) IS NULL
SELECT * FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208` WHERE SAFE_CAST(upvote_ratio AS FLOAT64) IS NULL
-- all data is able to safely cast to desired data type

-- noticed that ampersand symbols are being imported as "%amp;", need to replace that in title field
-- "&amp;", "&amp;amp;", "&amp;nbsp;", and "&amp;amp;nbsp;" should all be replaced as "&"
-- "&lt;" should be replaced with "<"
-- "&gt;" should be replaced with ">"
-- "&ndash;" should be replaced with "–"
-- "&#8211;" should be replaced with "–"
-- "&#039;" and "&#39;"should be replaced with "'"
-- "&reg;" should be replaced with "" since this represents the registered logo and cannot translate that into text


--======================================================--
--        2b. Retroactive data integrity check          -- 
--======================================================--
/*          ********EDIT 3/30/2022*********
Started visualization process and realized I had not checked to see if "id" column was unique or not
Ran the following process to validate
*/

-- Check total count versus count of unique values for "id" field after deduplication
SELECT COUNT(*), COUNT(DISTINCT id)
FROM (
    SELECT DISTINCT * EXCEPT(view_count, approved_at_utc)
    FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208`
    WHERE subreddit <> "ArtisanBread"
    -- 398,252 unique rows
)
-- 398,252 total rows
-- 398,251 unique "id" values --> investigate to find the one "id" that is duplicated

-- collect all "id" values and a count of their frequency, then grou pby id and restrict to "id" values occurring more than once
SELECT id, COUNT(id)
FROM (
    SELECT DISTINCT * EXCEPT(view_count, approved_at_utc)
    FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208`
    WHERE subreddit <> "ArtisanBread"
    -- 398,252 unique rows
)
GROUP BY id
HAVING COUNT(id) > 1
-- "id" = "96hdaz" occurs twice... why? let's look at the records for this id

SELECT *
FROM (
    SELECT DISTINCT * EXCEPT(view_count, approved_at_utc)
    FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208`
    WHERE subreddit <> "ArtisanBread"
    -- 398,252 unique rows
)
WHERE id = "96hdaz"
/* 
seems like most fields are identical except for a few
For instance, the "retrieved_on" dates are different
Given how this data is collected (API requests), it seems that this post was active when first retrieved
but either the user or post was since then deleted and captured again by the data hosting site
This second record can be ignored and should be dropped from the final analysis table
The series of CTE tables used to create the analysis table will be altered appropriately
*/

--======================================================-- 
--   3. Cast and Clean data into final anaylsis table   -- 
--======================================================--

WITH UNIQUE
AS (
    -- only unique rows
    SELECT DISTINCT * EXCEPT(view_count, approved_at_utc)
    FROM `SL_Intern_Project_2022.stage_subreddit_data_20220208`
    WHERE subreddit <> "ArtisanBread"
    AND NOT (id ="96hdaz" AND retrieved_on = "1591721886.0")
),
ADD_NULLS
AS (
    SELECT CASE WHEN author = "nan" THEN NULL ELSE author END AS author,
            CASE WHEN created_utc = "nan" THEN NULL ELSE created_utc END AS created_utc,
            CASE WHEN domain = "nan" THEN NULL ELSE domain END AS domain,
            CASE WHEN full_link = "nan" THEN NULL ELSE full_link END AS full_link,
            CASE WHEN id = "nan" THEN NULL ELSE id END AS id,
            CASE WHEN is_crosspostable = "nan" THEN NULL ELSE is_crosspostable END AS is_crosspostable,
            CASE WHEN is_original_content = "nan" THEN NULL ELSE is_original_content END AS is_original_content,
            CASE WHEN is_reddit_media_domain = "nan" THEN NULL ELSE is_reddit_media_domain END AS is_reddit_media_domain,
            CASE WHEN is_video = "nan" THEN NULL ELSE is_video END AS is_video,
            CASE WHEN link_flair_type = "nan" THEN NULL ELSE link_flair_type END AS link_flair_type,
            CASE WHEN media_only = "nan" THEN NULL ELSE media_only END AS media_only,
            CASE WHEN num_comments = "nan" THEN NULL ELSE num_comments END AS num_comments,
            CASE WHEN num_crossposts = "nan" THEN NULL ELSE num_crossposts END AS num_crossposts,
            CASE WHEN retrieved_on = "nan" THEN NULL ELSE retrieved_on END AS retrieved_on,
            CASE WHEN score = "nan" THEN NULL ELSE score END AS score,
            CASE WHEN selftext = "nan" THEN NULL ELSE selftext END AS selftext,
            CASE WHEN spoiler = "nan" THEN NULL ELSE spoiler END AS spoiler,
            CASE WHEN stickied = "nan" THEN NULL ELSE stickied END AS stickied,
            CASE WHEN subreddit = "nan" THEN NULL ELSE subreddit END AS subreddit,
            CASE WHEN subreddit_subscribers = "nan" THEN NULL ELSE subreddit_subscribers END AS subreddit_subscribers,
            CASE WHEN subreddit_type = "nan" THEN NULL ELSE subreddit_type END AS subreddit_type,
            CASE WHEN title = "nan" THEN NULL 
                WHEN (title LIKE "%&amp;%" OR title LIKE "%&amp;amp;%" OR title LIKE "%&amp;nbsp;%" OR title LIKE "%&amp;amp;nbsp;%")
                    THEN REPLACE(REPLACE(title, "amp;", ""), "nbsp;", "")
                ELSE title END AS title,
            CASE WHEN total_awards_received = "nan" THEN NULL ELSE total_awards_received END AS total_awards_received,
            CASE WHEN url = "nan" THEN NULL ELSE url END AS url,
            CASE WHEN edited = "nan" THEN NULL ELSE edited END AS edited,
            CASE WHEN updated_utc = "nan" THEN NULL ELSE updated_utc END AS updated_utc,
            CASE WHEN gilded = "nan" THEN NULL ELSE gilded END AS gilded,
            CASE WHEN upvote_ratio = "nan" THEN NULL ELSE upvote_ratio END AS upvote_ratio,
            CASE WHEN author_id = "nan" THEN NULL ELSE author_id END AS author_id,
            CASE WHEN author_cakeday = "nan" THEN NULL ELSE author_cakeday END AS author_cakeday,
            CASE WHEN author_created_utc = "nan" THEN NULL ELSE author_created_utc END AS author_created_utc,
            CASE WHEN source_file = "nan" THEN NULL ELSE source_file END AS source_file
    FROM UNIQUE
),
CAST_TO
AS(
    SELECT CAST(author AS STRING) AS author,
        timestamp_seconds(CAST(created_utc AS INT64)) AS created_utc,
        CAST(domain AS STRING) AS domain,
        CAST(full_link AS STRING) AS full_link,
        CAST(id AS STRING) AS id,
        CAST(is_crosspostable AS STRING) AS is_crosspostable,
        CAST(is_original_content AS STRING) AS is_original_content,
        CAST(is_reddit_media_domain AS STRING) AS is_reddit_media_domain,
        CAST(is_video AS STRING) AS is_video,
        CAST(link_flair_type AS STRING) AS link_flair_type,
        CAST(media_only AS STRING) AS media_only,
        CAST(num_comments AS INT64) AS num_comments,
        CAST(CAST(num_crossposts AS FLOAT64) AS INT64) AS num_crossposts,
        timestamp_seconds(CAST(CAST(retrieved_on AS FLOAT64) AS INT64)) AS retrieved_on,
        CAST(score AS INT64) AS score,
        CAST(selftext AS STRING) AS selftext,
        CAST(spoiler AS STRING) AS spoiler,
        CAST(stickied AS STRING) AS stickied,
        CAST(subreddit AS STRING) AS subreddit,
        CAST(CAST(subreddit_subscribers AS FLOAT64) AS INT64) AS subreddit_subscribers,
        CAST(subreddit_type AS STRING) AS subreddit_type,
        CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(title, "&lt;", "<"), "&gt;", ">"), "&ndash;", "–"), "&#8211", "–"), "&#039", "'"), "&#39", "'"), "&reg;", "") AS STRING) AS title,
        CAST(CAST(total_awards_received AS FLOAT64) AS INT64) AS total_awards_received,
        CAST(url AS STRING) AS url,
        CAST(edited AS STRING) AS edited,
        timestamp_seconds(CAST(CAST(updated_utc AS FLOAT64) AS INT64)) AS updated_utc,
        CAST(CAST(gilded AS FLOAT64) AS INT64) AS gilded,
        CAST(upvote_ratio AS FLOAT64) AS upvote_ratio,
        CAST(author_id AS STRING) AS author_id,
        CAST(author_cakeday AS STRING) AS author_cakeday,
        timestamp_seconds(CAST(CAST(author_created_utc AS FLOAT64) AS INT64)) AS author_created_utc,
        CAST(source_file AS STRING) AS source_file
    FROM ADD_NULLS 
),
QC_Values
AS (
SELECT subreddit,
    MIN(created_utc) AS First_Post,
    MAX(created_utc) AS Last_Post,
    MIN(num_comments) AS Min_Comments,
    MAX(num_comments) AS Max_Comments,
    MIN(num_crossposts) AS Min_Crossposts,
    MAX(num_crossposts) AS Max_Crossposts,
    MIN(subreddit_subscribers) AS Min_Subscribers,
    MAX(subreddit_subscribers) AS Max_Subscribers,
    MIN(total_awards_received) AS Min_Awards,
    MAX(total_awards_received) AS Max_Awards,
    MIN(upvote_ratio) AS Min_Upvote_Ratio,
    MAX(upvote_ratio) AS MAX_Upvote_Ratio,
    MIN(author_created_utc) AS Min_Author_Create_Date,
    MAX(author_created_utc) AS Max_Author_Create_Date,
    COUNT(*) AS Total_Posts
    FROM CAST_TO
GROUP BY subreddit
),
HTML_CHECK
AS (
    SELECT title
    FROM CAST_TO 
    WHERE title LIKE '%&%' AND title NOT LIKE '% & %' AND LOWER(title) NOT LIKE '%m&m%' AND LOWER(title) NOT LIKE '%pb&j%'
)
SELECT *
FROM CAST_TO
-- 398,251 rows -> as expected
-- results saved in `SL_Intern_Project_2022.analysis_subreddit_data_20220330`















