## PROBLEM STATEMENT
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I built an ETL pipeline that extracts their data from S3 onto an AWS EMR cluster running Apache Spark. Then, I transform data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 


## APPROACH TO SOLUTION
The problem was solved by doing the following:
1. I loaded data onto the AWS EMR cluster from AWS S3 bucket into DataFrame objects, `df`.
2. Next, I designed and created dimension tables `users_table`, `songs_table`, `artist_table`, and `time_table`  together with the fact table `songplays_table`. These five tables are used to define a star Schema. The choice of star schema is predominantly selected because it involves fewer joins which is great for data analytics. Because it involves few join, it is also fast to query.
3. Finally, I extracted the appropriate data from the DataFrame objects `df` into each of the five tables.

## HOW TO RUN THE SCRIPTS.
1. Fill out `dl.cfg` with your AWS credentials. If you don't have any, create IAM user from AWS, and save the credentials. Then use that information to fill `dl.cfg` out.
2. Run `etl.py` in the python shell e.g ``` python etl.py```

