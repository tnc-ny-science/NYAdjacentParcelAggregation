# NYAdjacentParcelAggregation

Statewide adjacent parcel aggregation efforts.

NOTE: These methods were developed for an originally implemented on statewide CoreLogic Parcel Point data for NY State.

Step 1:
Filename: Step1_AnalyzeAdjacency.sql

Purpose: Create table of adjacency for all polygons in the NY State CoreLogic Parcel polygon data that are within 50 m of one another. Note: 50 m was chosen as the threshold for adjacency to account for small separations across right of ways, stream channels, etc.

Language: SQL (BigQuery)

Output: table in BigQuery that was be exported as csv file for reading in Step 2

Step 2:
Filename: Step2_PerformAggregation.py

Purpose: Identify and group adjacent parcels by common ownership (defined as very similar owner names) using fuzzy string matching, writing results as a new attribute in the CoreLogic Parcel Point polygon data.

Language: Python

Output: new attribute in input polygon data
