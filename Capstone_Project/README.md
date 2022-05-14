


# Data Engineering Capstone Project

## Introduction


In the project, we have four datasets to complete the project. The main dataset will include data on immigration to the United States, and country dataset, U.S. city demographics, and temperature data. With these datasets we are going to enrich i94 immigrant_data for effective analysis.


## Project Description

In this project, we will work around Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## Project DataSet

-   **I94 Immigration Data:**  This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace.  [This](https://travel.trade.gov/research/reports/i94/historical/2016.html)  is where the data comes from. There's a sample file so we van take a look at the data in csv format before reading it all in. We are not using the entire dataset, just using what we need to accomplish the goal, set at the beginning of the project.<br/>Immigration data can be accessed under `/data/18-83510-I94-Data-2016/`.There's a file for each month of the year in the actual dataset provided by Udacity. An example file name is `i94_apr16_sub.csv` and `i94_jun16_sub.csv` are uploaded to s3. Each file has a three-letter abbreviation for the month name. So a full file path for June would look like this: `s3://udacityawsbucket01/capstone_datasource/18-83510-I94-Data-2016/i94_jun16_sub.csv`


-   **World Temperature Data:**  This dataset came from Kaggle. You can read more about it  [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).We will take the temaperatures after the year 2000.<br/> Source path : `s3://udacityawsbucket01/capstone_datasource/GlobalLandTemperaturesByCity.csv`


   - **U.S. City Demographic Data:**  This data comes from OpenSoft. You can read more about it  [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).<br/> Source path : `s3://udacityawsbucket01/capstone_datasource/us_cities_demographics.csv`


   - **ISO Country Codes:**  This data comes from Kaggle. You can read more about it  [here](https://www.kaggle.com/andradaolteanu/iso-country-codes-global/).<br/> Source path : `s3://udacityawsbucket01/capstone_datasource/wikipedia-iso-country-codes.csv`


## Project Repository files:
1.  `etl.ipynb` reads data from S3, processes that data using Spark, and writes them back to S3
2.  `utility.py`contains some helper function for `etl.ipynb`
3.  `README.md`  provides discussion on project.


## Database design
#### Schema for Immigrant Data Analysis

Using the four datasets, a star schema is created for optimized for queries on immigrant data analysis. This includes the following tables. `dim_visa` and `dim_travelmode` are two ULT dimension tables which are not coming from any dataset but we manually building for data enhancements.

#### Fact Table

1.  **fact_immigration**  - records in log data associated with immigrant details.
    -   _immigrant_id_, _state_code_, _origin_country_code_, _residence_country_code_, _port_id_, _arrival_date_, _departure_date_, _age_, _visa_code_, _travel_mode_, _count_, _fileadd_date_, _visapost_, _arrival_flag_, _departure_flag_, _match_flag_, _birthyear_, _admission_date_, _gender_, _airline_, _admission_no_, _flight_no_, _visatype_

#### Dimension Tables

2.  **dim_city**  - U.S city details.
    -   _state_code_, _city_, _state_, _median_age_, _male_population_, _female_population_, _total_population_ ,_veteran_population_, _foreigner_born_, _avg_household_size_, _race,count_
    
3.  **dim_country**  - country details around the world.
    -  _country_code_, _country_name_, _alpha2_code_, _alpha3_code_


4.  **dim_time**  - `arrival_time` attribute of **I94 Immigration Data:** broken down into specific units
    -  _date_, _year_, _month_, _day_, _dayofweek_, _weekofyear_
    
5.  **dim_temperature**  - temperature details of each day of cities around the world.
    -  _date_, _city_, _avg_temperature_, _avg_temperature_uncertainty_, _country_, _latitude_ ,_longitude_
4.  **dim_visa**  - visa details
    -  _visa_code_, _visa_description_
4.  **dim_travelmode**  - mode of travel to come to U.S 
    -   _travel_code_, _description

**ERD Diagram**:
    <img 
src="https://udacityawsbucket01.s3.us-west-1.amazonaws.com/dbdiagram.PNG" alt="ERD Diagram" width="800"/>


## ETL Process
Below are the few main points about etl load. Detailed step by step  description of etl is given in `etl.ipynb` For S3 read/write use spark 3 version. Alternatively, you can read from sample csv if want to avoid s3 connection.

- Creation of tables as per the above data model.
- Rename and cast datatypes as per the data model.
- Quality check for tables.
- Write each table in the Amazon s3 as sink.
- We are going to truncate and delete the dimension table and fact table will be written in append mode.


## How To Run the Project
Run the `etl.ipynb` file to extract, load and transform the data from four datasets residing in s3 and after processing store it in the s3 location.
