import org.apache.spark.sql.SparkSession

object sparkSQLdemo {
  def main(args: Array[String]) {
    val error_code = args(0);
    val inFilePath = args(1);
    
    val sparkSesion = SparkSession
    .builder
    .master("local")
    .appName("Advanced SQL")
    .getOrCreate() 
    
    val df = sparkSesion.read.csv(inFilePath);
    
    println(df.printSchema())
    
  }
}