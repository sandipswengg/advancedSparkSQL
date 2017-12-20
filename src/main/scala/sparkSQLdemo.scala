import org.apache.log4j.Logger
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._



// https://spark.apache.org/docs/latest/sql-programming-guide.html#starting-point-sparksession

object sparkSQLdemo {
  // Log4J
  // val logger = Logger.getLogger(sparkSQLdemo.getClass);
    
  def main(args: Array[String]) {
    val error_code = args(0);
    val inFilePath = args(1);
    
    val conf = new SparkConf().setAppName("dataSourceExample").setMaster("local");
    val sc = new SparkContext(conf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    
    val df_schema = sqlContext.read
    .format("com.databricks.spark.csv")
    .load(inFilePath)
    
    val MobFailSchema = StructType(
          StructField("loc_id", StringType, true) ::
          StructField("fail_time_sec", IntegerType, true) ::
          StructField("mob_no", StringType, true) ::
          StructField("error_code", StringType, true) :: Nil
    )
    
    
    val df_schema_1 = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("delimeter", ",")
    .option("schema", "MobFailSchema")
    .load(inFilePath)
    
    
    val df = df_schema.withColumnRenamed("in_sec", "_c1")
    .withColumn("_c1", df_schema.col("_c1").cast("Int").alias("Fail_in_sec"))
    
    df_schema.printSchema()
    df.printSchema()
    df_schema_1.printSchema()
    
    df
    .select(col("_c0").alias("location_id"), col("_c2").alias("mob"))
    .filter(df("_c1") > 4)
    .filter(df("_c3").equalTo(error_code))
    .show()
    
    
    
  }
}



