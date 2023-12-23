# Flight-Delays
NYC Public Transportation Delays and Accidents — Flight Delays


### etl_code
Drop columns that I do not need;

Date formatting: create a new column for the corresponding date based on "month" and "day";

Create a binary column: delay status (0: no delay, 1: delay);

Drop rows with NA values

### profiling_code
Count the number of records;

Map and count based on carrier;

Find distinct values

### ana_code
initial_analysis: mean, median, mode, and standard deviation of numerical data;

best_5_carriers: top 5 carriers with the lowest probability of delays;

worst_5_carriers: top 5 carriers with the highest probability of delays;

rank_airports: rank NYC airports by the average length of departure delay;

dep_delay_by_hour: average length of departure delay by hour;

dep_delay_by_month: average length of departure delay by month

### test_code
unused code


---------------------------

Run ETL/cleaning code first

Clean -> cleaning 2 -> drop_na


Then run analytics code. The 6 analytics code files can be run in any order you wish.


---------------------------

*To run code:*

spark-shell --deploy-mode client -i  FILENAME.scala

*Results will be directly printed out to the console when you run the code.*
