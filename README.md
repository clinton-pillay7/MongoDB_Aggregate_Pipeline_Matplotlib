# MongoDB_Aggregate_Pipeline_Matplotlib
Demo code - showing querying the Alpha Vantage API, storing the data in a mongoDB collection, and I touch on visualizing averages in a bar graph

Here I show the following: 

- Querying data from an API.
- Replacing "-" with "_" in API JSON result. 
- Inserting the json in MongoDB:
  - making sure I do not insert duplicates.
  - fixing the date in the correct format - removing inverted commas from the dates, So MongoDB can see the dates as dates and not strings. 
  - fixing the intergers - removing the inverted commas from the integers, so MongoDB can see the intergers as intergers and not strings. 
- Using MongoDB's aggregation pipeline to calculate and show averages and plotting them in a Matplotlib bar graph. ($match stage only).
- Using MongoDb's aggregation pipeline to calculate and show averges, but this time I have only selected a certain date range. ($match and $group stages).
