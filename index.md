# New York City Taxi and Limousine Commission (TLC) Trip Record Data Analysis

## Motivation
You’ll know which city I am talking about if I start describing it as the most populous city in the States, one of the biggest and the best places to be, the Big Apple, The City that never sleeps (literally). You got it right! Its NYC! I am very sure, that out of the many things that pop in your mind when you think of New York, the yellow taxicab 🚖 definitely has been in the list. New York is the home of this iconic yellow cab and can be commonly identified as a symbol of the city. The history of these cabs licensed under New York City Taxi and Limousine Commission (TLC) goes way back to the 70’s [[1]](https://en.wikipedia.org/wiki/Taxicabs_of_New_York_City). These cabs can be hailed from the streets anywhere and serve as a means of commute for thousands of New Yorkers on a daily basis. Where technology has advanced to an extent that almost everything is just a click away, this cab system has functioned in a conventional non-tech manner, until the recent launch of an app. This spiked my interest to take a look into the backend of this system. Being a techie, I could not resist the temptation of uncovering what lies beneath the humungous heap of data that must have accumulated over all these years. Like, what modes of payment were the largely preferred by the customers, how generously would they tip their cabbies, what were the vendor preferences, [[2]](https://www.marketwatch.com/story/this-chart-shows-how-uber-rides-sped-past-nyc-yellow-cabs-in-just-six-years-2019-08-09) why are people preferring Lyft and Uber when a taxicab system was already in place? I have managed to acquire a small data set of the past few years which will help me gain deep meaningful insights about this popular system. Since, such a large valuable amount of data is offered in a very basic format, its essential that it be processed meticulously and represented in a way which is easiest to understand and pleases the eye.


## About the Data
During the search for data set I found that Amazon have open dataset on AWS. When data is shared on AWS, anyone can analyze it and build service on top of it using a broad range of compute and data analytics product. This helps me a lot as it allows me to spend more time on data analysis rather than data acquisition. The [Registry of Open Data](https://registry.opendata.aws/) on AWS makes help me to find the dataset which are made publicly available through AWS services. [New York City Taxi and Limousine Commission (TLC) Trip Record Data](https://registry.opendata.aws/nyc-tlc-trip-records-pds/) is one of those open datasets on AWS which I choose for this project. Below are few details about this dataset:
### Description
  - This [data set](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) consist of data of trips taken by taxis and for-hire vehicles in New York City.
  - It consist of [TLC trip record](https://www1.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf) related to Yellow, Green and For-Hire vehicles trip data from year 2009-2019. The yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.
  - In this project I am going to analyze the 3 years data (2017-2019) and the size of dataset is 53.51 GB.
  - This data set have NYC TLC trip data in form of CSV file, for each year it has 12 CSV files (per month one file) for each type of taxis services
  - Below is the structure of taxis services trip records:
  
      1. [Yellow Taxis](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
      
      2. [Green Taxis](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf)
      
  - Each of the CSV file for every taxi will have a comma separated data as shown below:
  ```markdown
For example:
VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,PULocationID,DOLocationID,payment_type,fare_amount,tip_amount,tolls_amount,total_amount
1,2017-01-09 11:13:28,2017-01-09 11:25:45,1,3.3,1,263,161,1,12.5,2.0,0.0,15.3
1,2017-01-09 11:32:27,2017-01-09 11:36:01,1,0.9,1,186,234,1,5.0,1.45,0.0,7.25
1,2017-01-09 11:38:20,2017-01-09 11:42:05,1,1.1,1,164,161,1,5.5,1.0,0.0,7.3
1,2017-01-09 11:52:13,2017-01-09 11:57:36,1,1.1,1,236,75,1,6.0,1.7,0.0,8.5
2,2017-01-01 00:00:00,2017-01-01 00:00:00,1,0.02,2,249,234,2,52.0,0.0,0.0,52.8
1,2017-01-01 00:00:02,2017-01-01 00:03:50,1,0.5,1,48,48,2,4.0,0.0,0.0,5.3
2,2017-01-01 00:00:02,2017-01-01 00:39:22,4,7.75,1,186,36,1,22.0,4.66,0.0,27.96
1,2017-01-01 00:00:03,2017-01-01 00:06:58,1,0.8,1,162,161,1,6.0,1.45,0.0,8.75
1,2017-01-01 00:00:05,2017-01-01 00:08:33,2,0.9,1,48,50,1,7.0,0.0,0.0,8.3
```

## Obtaining the Data & Preprocessing
- Step 1: Files Downloading and storing to on local disk
To download the CSV file I wrote the below Python script. In this script I am giving all the URLs in the form of array as an input to download_csv function and with the help of requests library I am downloading the csv file and write it to destination folder.

```markdown

import requests
from time import time
urls = ["https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-01.csv",
        "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-02.csv",
        "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-03.csv",
        "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-04.csv",
        "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-05.csv",
        "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-06.csv",
        "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-07.csv",
        "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-08.csv",
        "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-09.csv",
        "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-10.csv",
        "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-11.csv",
        "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-12.csv", ] 
  
 def download_csv(url):
    path = 'C:/Nilay/a.csv'
    print(url)
    r = requests.get(url, stream=True)
    with open(path, 'wb') as f:
        for ch in r:
            f.write(ch)
start = time()
for x in urls:
    download_csv(x)
    print(f"Time to download: {time() - start}")
code ref: https://likegeeks.com/downloading-files-using-python/

```

- Step 2: Data filtering
Every file was approximately size of 2 GB, so used the below Python script to drop some of its column before uploading it to Google cloud storage. It reduces uploading time consumption.

```markdown
import os
import pandas as pd
col_to_put = ['VendorID', 'tpep_pickup_datetime',
               'tpep_dropoff_datetime', 'passenger_count', 
               'trip_distance', 'RatecodeID', 'PULocationID',
               'DOLocationID', 'payment_type', 'fare_amount',
               'tip_amount', 'tolls_amount', 'total_amount'] 
add_header = True
chunksize = 10 ** 5
my_path = "C:/Nilay/Data Processing in Cloud/Personal Project DataSet/2019"

for r, d, f in os.walk(my_path):
    for curr_file in f:
      print(curr_file)
      fileName = "C:/Nilay/Data Processing in Cloud/Personal Project DataSet/2019/"
      for chunk in pd.read_csv(fileName + curr_file, chunksize=chunksize,usecols=col_to_put):
          chunk.to_csv("C:/Nilay/Data Processing in Cloud/" + curr_file, mode='a', index=False, header=add_header) 
          if add_header:
              # The header should not be printed more than one
              add_header = False
```

- Step 3: Data Storing
All the csv file of size 53.51 GB are stored on the google cloud storage with help of google command line instruction as given below:
```markdown
gsutil -o GSUtil:parallel_composite_upload_threshold=150M -m cp -r FOLDERNAME/FILENAME gs://BUCKET_PATH
```
You can find more details regarding file uploading on google storage through command [here](https://cloud.google.com/storage/docs/gsutil/commands/cp#synopsis).

- Step 4: Loading data to Big Query
With help of Big Query “[Loading data from cloud storage](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage#overview)” feature I have created the table in Big Query with auto schema detection option. More information under the next title.

## BigQuery
  - How to load data to Big Query?
    There are different ways to load the data on Big Query, one can load the data:
      1. From [Cloud Storage](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage#overview)
      2. From [Local Storage](https://cloud.google.com/bigquery/docs/loading-data-local#overview)
      3. [And many more](https://cloud.google.com/bigquery/docs/loading-data-local#overview)
    There is a limitation on loading data using the classic Big Query web UI, files loaded from a local data source must be 10 MB or less and must contain fewer than 16,000 rows. If this is the case one can load the data with help of Cloud storage and in this project each file is of size 1.5 GB so I have upload all the file to cloud storage and then loading it to Big Query.
  - How to [load data to Big Query](https://cloud.google.com/bigquery/docs/loading-data#overview) from cloud storage with Classic UI?
      - As describe in 3-step3 you can upload the files to cloud storage. And then re-direct to Big Query [web UI console](https://bigquery.cloud.google.com/welcome), in the navigation panel you can click on “create new table” and create the table page will open. In the create table section you can select the location from where you want to upload the file(for this project I am using google cloud storage) and then select the format of the file(CSV for this project).
      - After selecting the file location, it will ask you for [schema selection](https://cloud.google.com/bigquery/docs/loading-data-local#loading_data_from_a_local_data_source), you can select the “Automatically detect” option this will detect the schema on basis of first row column name of you CSV file. Otherwise you can define the schema on your own also.
      - After defining schema, you have option to [appending to or overwriting](https://cloud.google.com/bigquery/docs/loading-data-local#appending_to_or_overwriting_a_table_using_a_local_file) a table using a local file. If the schema of the data does not match the schema of the destination table or partition, you can update the schema when you append to it or overwrite it.
  - How the schema for the table and it contains look after loading data?
        - In this project I have created the table for Green, Yellow, FHV taxis in Big Query as below
        -   
            1. **Yellow Taxis Table schema and content preview:**
                    ![Image](yellow_trip_Table_schema.png)
                    ![Image](yellow_trip_Table_content.png)
            2. **Green Taxis Table schema and content preview:**
                    ![Image](green_trip_Table_schema.png)
                    ![Image](green_trip_Table_content.png)
                    

## Analytics

### Graphical Analysis with the help of Google Data Studio 

<iframe width="1000" height="2000" src="https://datastudio.google.com/embed/reporting/c9c707e7-9403-4d0a-9d82-b64c293a0d2d/page/1M" frameborder="0" style="border:0" allowfullscreen></iframe>

### Analysis of Data
 - After creating tables from the repository data, I selected few columns on which I analyzed the data by quering and visualizing it. [chart 1] shows the most popular payment method for NYC taxies and winner is Credit card. This shows that people use cards mostly to pay for taxi fare. I have one oberservation, for 0.5% of trips mode of payment shows "No charges". This can be due to multiple reasons so not really sure if that means ride was cancelled or something else but I feel if data would have been more specific I could have projected more metrics on it. 
 
- Lets talk about [chart 2], which is talking about how many passengers are served by each taxi type. Looking at the data, it shows that Green taxi served very less customers compare to Yellow Taxi from 2017-2019. Yellow taxi is clearly a winner in this case by serving 250M+ customers. Please note that I found few discripancies in the data for 2019 for yellow taxi so there may be some count issue for yellow taxi but still it is clearly a winner in customer count.
 
- [chart 3] shows that no. of customers for green taxi is kind of constant and there is no significant growth in ridership. Also Yellow taxi data shows that growth in ridership is increasing and decreasing over the years over the month. This data can be better viewed if it is shown per year but I wanted to get idea for last 3 years aggregately so I calculated that way. 
 
 - [chart 4] People say that night life is amazing in New York, I feel that my data is also the proof of that. Looking at yellow taxi data, taxi is mostly busy between 1800 and 2000 hours. This is usually dinner time and after office hours. Green taxi is not really showing anything special, looks like taxi is busy thorughout the day is same. 
 
 - In [chart 5] and [chart 6], my moto for this chart was check how generous people are towards taxis drives. I was looking for specific pattern like if there is specific day or month when people are more generous in tips but after looking at the data I didn't find such facts. Instead  I found that every year people have shown there generosity towards drives as there is no spike in the data graph. Also it shows that tip amount is growing over the year for green taxies as well as yellow taxis, but the spike in the yellow taxis cab shows the data descripancy which I found during loading data to Big Query.

- I found the two intresting fact while analzing thr data those are below: 
        1. Trends in Payment mode: Over the year "No charge" mode of payment is reduced for Green taxis[Table 1], but not the same case for yellow taxis[Table 2]. Also there is only one eletronic mode of payment available in NYC TLC, so here is the scope of development for NYC TLC cooroeration as well as fintech industry to include more electronic payment mode like e-wallets.
        2. Busiest pick up location: Another intresteing fact can help the department of transport to manage the traffic and parking problem as [Table 3] and [Table 4] show which are the most busiest pickup location ids for both the taxis.
  
### Challenges
    1. Skewed Data:
    
    2. In correct data types:
    
    3. Missing field values:
    
### Future Work

### Query Execution Details

## Why Big Query ? 

    - Faster and user friendly web UI for querying
    - UDFs
    - Easy to integrate with Google Data Studio
    - Loading data from many sources like cloud storage



