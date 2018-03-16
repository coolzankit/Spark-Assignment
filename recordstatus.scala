package ankitproj1.pkg1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object recordstatus {
  def main(Args:Array[String])
  {
    val spark = new SparkSession.Builder()
      .appName("dataframe")
      .master("local")
      .getOrCreate()
     
      val dftype1 = spark.read.format("csv").option("header", true).option("inferschema", false).load("C:/Users/ankit/OneDrive/Desktop/ScalaD/type1.csv")
      val dftype1cols = dftype1.select("id","Name","Address","Date_time","mobile")
      
      val dfhistory = spark.read.format("csv").option("header", true).option("inferschema", false).load("C:/Users/ankit/OneDrive/Desktop/ScalaD/history.csv")
      val dfhistcols = dfhistory.select("id","Name","Address","Date_time","mobile")
      
      dftype1.registerTempTable("type1")
      dfhistory.registerTempTable("history")
      
     val sqlhquery = "Select * from history where Date_time > cast('2018-03-10' as date) "
     spark.sql(sqlhquery).registerTempTable("history2")
     
     val sqltq1 = "Select t.* , 'update' from type1 t, history2 h where t.id = h.id and t.mobile != h.mobile order By t.id"
     val sqltq2 = "Select * , 'insert' from type1  where id not in (Select id from history2) order By id"
     val sqltq3 = "Select t.* , ' ' from type1  t, history2 h where t.id = h.id and t.mobile = h.mobile order By t.id"
     val sqlresult = (spark.sql(sqltq3) union spark.sql(sqltq2) union spark.sql(sqltq1)).show()
     
   //  sqlresult.write.format("csv").save("C:/Users/ankit/OneDrive/Desktop/ScalaD/result.csv")
     
  }
}