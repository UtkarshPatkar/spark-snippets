/*
The script loops through all tables in a particular hive database and outputs the schema of each table to a file.
*/
import org.apache.spark.sql.catalog
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row

val database = "YOUR Database NAME GOES HERE"

val schema = StructType(
  StructField("tableName", StringType, true) ::
  StructField("name", StringType, true) ::
  StructField("dataType", StringType, true) :: Nil)

var initialDF = spark.createDataFrame(sc.emptyRDD[Row], schema)

//show table statement below can take additional pattern matching criteria to filter tables. Look for SPARK documentation.
val tableNames = spark.sql(s"show tables in $database").select("tableName").collect.map(x => x(0))

tableNames.foreach(table => {
initialDF = initialDF.union(spark.catalog.listColumns(s"${database}.$table").withColumn("tableName", lit(table)).select("tableName", "name", "dataType"))
})

//Uncomment below line and change the file path
//initialDF.coalesce(1).write.csv(s"file:///FILE PATH") 

