# Sourdough Exploration
## Investigating the effect of COVID-19 lockdown on the interest in sourdough baking

## Languages / Technologies Used
* Python (Pandas, Requests, Pandas_GBQ, BigQuery API)
* SQL (BigQuery)
* Tableau
* Google Cloud Platform

## General Process Overview
* Identify the subreddits to scrape from (r/Breadit, r/sourdough, r/baking, r/ArtisanBread)
* Connect to Reddit API and pushshift.io API to collect 400k+ posts from subreddits using python
* Clean and normalize data using pandas and load into BigQuery table for further transformations
* Investigate data and clean as necessary, putting into stage table in BigQuery
* Perform further analyses (keyword search through post text) to identify content directly related
  to sourdough baking in BigQuery
* Connect data to Tableau and create visualizations to display trends in time vs. popularity

## Final [Visualization](https://public.tableau.com/views/SLInternPOC2022/SourdoughMetrics?:language=en-US&:display_count=n&:origin=viz_share_link) 
<img width="1411" alt="Screen Shot 2023-06-18 at 12 42 50 PM" src="https://github.com/slacey21/Sourdough/assets/92884504/f4426c66-f768-49cb-8508-db0dda95dbca">

