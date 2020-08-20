# In the example, we will first send the data from our Linux file system to the data storage unit of the Hadoop ecosystem (HDFS) (for example, Extraction).
# Then we will read the data we have written here with Spark and then we will apply a simple Transformation and write to Hive (Load). 

# Hive is a substructure that allows us to query #the data in the hadoop ecosystem, which is stored in this environment. 
# With this infrastructure, we can easily query the data in our big data environment using SQL language.

#The data set looks lke below

Column Name           	Data Type

ORDERNUMBER	            Number
QUANTITYORDERED	        Number
PRICEEACH	              Number
ORDERLINENUMBER       	Number
SALES	                  Number
ORDERDATE	              Date
STATUS	                String
QTR_ID	                Number
MONTH_ID	              Number
YEAR_ID               	Number
PRODUCTLINE	            String
MSRP	                  Number
PRODUCTCODE           	String
CUSTOMERNAME	          String
PHONE	                  Number  
ADDRESSLINE1	          String
ADDRESSLINE2	          String
CITY                  	String
STATE	                  String
POSTALCODE            	Number
COUNTRY	                String
TERRITORY             	String
CONTACTLASTNAME	        String
CONTACTFIRSTNAME       	String
DEALSIZE	              String

-------------------------------------------------Code example---------------------------------------

# First, copy the sample data that we downloaded from Kaggle to HDFS. For this, let's create a directory in HDFS.

hadoop fs -mkdir samplesales

#copy the sample data in our local directory to hdfs.

hadoop fs -copyFromLocal sales_sample_data.csv samplesales
hadoop fs -ls samplesales/

# start our PySpark interface and start processing the data with Spark
 /user/spark-2.1.0-bin-hadoop2.7/bin/pyspark --master yarn-client --num-executors 4 --driver-memory 2g --executor-memory 4g

#Let's import the libraries 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
hive_context = HiveContext(sc)
sqlContext = SQLContext(sc)

# let's create the line format in which we will keep our data.
RecSales = Row('ordernumber','quantityordered','priceeach','orderlinenumber','sales',
'orderdate','status','qtr_id','month_id','year_id','productline','msrp','productcode','customername',
'phone','addressline1','addressline2','city','state','postalcode','country','territory','contactlastname','contactfirstname','dealsize')

# Now it's time to read the data we received in HDFS and write it in a dataframe. Since the columns are sperated by the delimeter="," , 
# and we must specify this delimeter when loading the data.
# After data is in the data frame, we give a name and save it as a temp table. We will be able to use this name later when writing SQL in hive_context or sqlContext.


dataSales = sc.textFile("/user/samplesales/")
header = dataSales.first()
dataSales= dataSales.filter(lambda line: line != header)
recSales = dataSales.map(lambda l: l.split(","))
dataSales = recSales.map(lambda l: RecSales(l[0],l[1],l[2],l[3],l[4],l[5],l[6],l[7],l[8],l[9],l[10],l[11],l[12],l[13],l[14],l[15],l[16],l[17],l[18],l[19],l[20],l[21],l[22],l[23],l[24]))
dfRecSales = hive_context.createDataFrame(dataSales)
dfRecSales.registerTempTable("sales")

# We have successfully read the data from HDFS and have written it into a data frame object.
# Now let's have a few simple queries with the data we uploaded with Spark SQL.

hive_context.sql("select count(*) from sales").collect()
hive_context.sql("select * from sales").head()
hive_context.sql("select ordernumber,priceeach  from sales").head(2)

# Now let's group the sales by territory, and write the results to a Hive table.

dfterriroty = hive_context.sql("select territory,sum(priceeach) total from sales group by territory")
dfterriroty.registerTempTable("sumterr")
dfterriroty.show()

# Let's create a Hive table and write the result.

hive_context.sql("CREATE TABLE IF NOT EXISTS territory_sum (territory String, total INT)")
hive_context.sql("insert into territory_sum select * from sumterr")

# Finally, check the data written to Hive.

hive_context.sql("select * from territory_sum").show()
hive_context.sql("select count(*) from territory_sum").show()
hive_context.sql("select count(*) from sumterr").show()





