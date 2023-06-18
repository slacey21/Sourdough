-- DESCRIPTION: Create calculated columns for analysis table, perform text analysis

--======================================================--
--    1. Gather summary data from prepared dataset      -- 
--======================================================--


SELECT *
FROM `SL_Intern_Project_2022.analysis_subreddit_data_20220330`
LIMIT 100

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
FROM `SL_Intern_Project_2022.analysis_subreddit_data_20220330`
GROUP BY subreddit
-- Summary data table saved in excel workbook for reference

--======================================================--
--    2. Identify posts related to sourdough content    -- 
--======================================================--
/* Keywords used to identify bread/sourdough related posts:
sourdough
levain
leaven
leavened
starter 
boule
batard
banneton
oven spring 
tartine
scoring
open crumb
bulk ferment
stretch and fold
baguette
chad robertson
*/

/*
Added "is_sourdough" column manually to schema with initial values set to NULL
Updating "is_sourdough" column in analysis table to indicate whether content is sourdough related
The rows with is_sourdough = "Yes" will be sueful for visualizations centered on sourdough related content
*/

UPDATE `SL_Intern_Project_2022.analysis_subreddit_data_20220330`
SET is_sourdough = 
    CASE WHEN subreddit = "Sourdough" THEN "Yes"
        WHEN subreddit <> "Sourdough" AND REGEXP_CONTAINS(LOWER(title), 
            r"sourdough|levain|leaven|leavened|naturally leavened|starter|boule|batard|oven spring|tartine|scoring|open crumb|bulk ferment|stretch and fold|baguette|chad robertson") THEN "Yes" 
        WHEN subreddit <> "Sourdough" AND REGEXP_CONTAINS(LOWER(selftext), 
            r"sourdough|levain|leaven|leavened|naturally leavened|starter|boule|batard|oven spring|tartine|scoring|open crumb|bulk ferment|stretch and fold|baguette|chad robertson") THEN "Yes" 
        ELSE "No"
        END 
WHERE is_sourdough IS NULL
-- This statement modified 398,251 rows in analysis_subreddit_data_20220330.


-- Check to see if update was made correctly, should have ~96000 "Yes" and the rest should be "No"
SELECT is_sourdough, COUNT(*) AS Count
FROM `SL_Intern_Project_2022.analysis_subreddit_data_20220330`
GROUP BY is_sourdough
-- No: 302,145
-- Yes: 96,106

--======================================================--
--         3. Send final data table to CSV file         -- 
--======================================================--

SELECT *
FROM `SL_Intern_Project_2022.analysis_subreddit_data_20220330`












