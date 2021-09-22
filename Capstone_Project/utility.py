from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import types as T
import datetime
import matplotlib.pyplot as plt
import logging


def renameAndcast(df,column_dictionary):
    """
          Description - This function rename and cast the attributes of given spark dataframe 
                        and returns the modified dataframe.

          Input - Spark DataFrame,nested list.

          Output - Spark DataFrame(Returns the modified dataframe).
    """
    attribute_list=[]

    for each in column_dictionary:
        if each[2] == 'date':
            attribute_list.append(f"coalesce(to_date(`{each[0]}`,'yyyy-MM-dd'),to_date(`{each[0]}`,'dd-MM-yyyy')) as {each[1]}")
        else:
            attribute_list.append(f"cast(`{each[0]}` as {each[2]}) as {each[1]}")


    final_df = df.selectExpr(attribute_list)
    return final_df

# COMMAND ----------

def visualize_nullColumns(df,sample_size,percent):
    """
      Description - This function takes spark dataframe and based on the sample_size provided
                    calculates null percentage for each columns. Put those columns 
                    in chart having null_percent > percent. provided as input.

      Input - Spark DataFrame, inetegr(sample size),
              inetger(percent of null values we interested to show in bar graph).

      Output - Returns nothing, Displays the bar chart having null percentages for columns.
    """
    column_list = []
    null_list = []
  
    for each in df.columns:

        data_count = df.limit(sample_size).select(each).filter(F.col(each).isNull()).count()
        #Calculating the percentage of null
        percentage_count = (data_count/float(sample_size))*100 

        #If null_percent > 30% then add to the list
        if(percentage_count>percent):
            column_list.append(each)
            null_list.append(percentage_count)

    #size the diagram
    #fig = plt.figure(figsize = (20, 5))

    # creating the bar plot
    plt.figure(figsize=(10, 3))  # width:20, height:3


    plt.bar(column_list, null_list, color ='blue',
                    width = 0.1)

    plt.xlabel("Columns")
    plt.ylabel("Percentage")
    plt.title(f"Null percentage in each column having greater than {percent}%")
    plt.show()

# COMMAND ----------

def casttodate(x):
  
    """
      Description - This function takes as SAS numeric date field as input and returns the date format.

      Input - int object.

      Output - Python date object.
    """
  
    try:
        return datetime.datetime.strptime('1960-01-01','%Y-%m-%d')+datetime.timedelta(days=x)
    except Exception as e:
        return None
    

# COMMAND ----------

def sasTodatetime(df,datetime_cols):
    """
          Description - Casts the columns provided as a list to spark datetype format 
                        from SAS numeric format for the input dataframe and returns the modified one.

          Input - Spark DataFrame,list of columns need conversion

          Output - Spark DataFrame.
    """
    attribute_list=[]
  
    for each in df.columns:
        if(each in datetime_cols):
            attribute_list.append(f"castdate({each}) as {each}")
        else:
            attribute_list.append(each)
            
    final_df = df.selectExpr(attribute_list)
    return final_df

# COMMAND ----------

#fact_immigration table schema
immigration_schema = StructType([StructField('immigrant_id',IntegerType(),False),                                
                                 StructField('state_code',StringType(),True),
                                 StructField('origin_country_code',IntegerType(),True),
                                 StructField('residence_country_code',IntegerType(),True),
                                 StructField('port_id',StringType(),True),
                                 StructField('arrival_date',DateType(),False),
                                 StructField('departure_date',DateType(),True),
                                 StructField('age',IntegerType(),True),
                                 StructField('visa_code',IntegerType(),True),
                                 StructField('travel_mode',IntegerType(),True),
                                 StructField('count',IntegerType(),True),
                                 StructField('fileadd_date',DateType(),True),
                                 StructField('visapost',StringType(),True),
                                 StructField('arrival_flag',StringType(),True),
                                 StructField('departure_flag',StringType(),True),
                                 StructField('match_flag',StringType(),True),
                                 StructField('birthyear',IntegerType(),True),
                                 StructField('admission_date',DateType(),True),
                                 StructField('gender',StringType(),True),
                                 StructField('airline',StringType(),True),
                                 StructField('admission_no',StringType(),True),
                                 StructField('flight_no',IntegerType(),True),
                                 StructField('visatype',StringType(),True)])

# COMMAND ----------

#dim_time table schema
time_schema = StructType([StructField('date',DateType(),False),
                          StructField('year',StringType(),True),
                          StructField('month',StringType(),True),
                          StructField('day',StringType(),True),
                          StructField('dayofweek',IntegerType(),True),
                          StructField('weekofyear',IntegerType(),True)])

# COMMAND ----------

#dim_temperature table schema
temperature_schema = StructType([StructField('date',DateType(),False),
                                 StructField('city',StringType(),False),
                                 StructField('avg_temperature',DoubleType(),True),
                                 StructField('avg_temperature_uncertainty',DoubleType(),True),
                                 StructField('country',StringType(),False),
                                 StructField('latitude',StringType(),True),
                                 StructField('longitude',StringType(),True)])

# COMMAND ----------

#dim_city table schema
city_schema = StructType([StructField('city',StringType(),False),
                          StructField('state',StringType(),True),
                          StructField('median_age',DoubleType(),True),
                          StructField('male_population',LongType(),True),
                          StructField('female_population',LongType(),True),
                          StructField('total_population',LongType(),True),
                          StructField('veteran_population',LongType(),True),
                          StructField('foreigner_born',LongType(),True),
                          StructField('avg_household_size',DoubleType(),True),
                          StructField('state_code',StringType(),True),
                          StructField('race',StringType(),True),
                          StructField('count',LongType(),True)])

# COMMAND ----------

def quality_cheker(spark,df,schema):
    """
      Description - Validates the data of a spark datframe against a schema provided
                    as input. Also checks if dataframe is empty or not.

      Input - SparkSession,Saprk DataFrame, Spark Schema

      Output - Spark DataFrame
    """
  
    df_final = spark.createDataFrame(df.rdd,schema=schema)
    count=1

    try:
        count = df_final.count()
    except Exception as e:
        print(str(e))

    if count == 0:
        raise ValueError("No record found!!")
