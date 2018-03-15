package ankitproj1.pkg1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DFcrimecount {
  def main(Args:Array[String])
  {
    val spark = new SparkSession.Builder()
      .appName("dataframe")
      .master("local")
      .getOrCreate()
         
      val df = spark.read.format("csv").option("header", true).option("inferschema", false).load("C:/Users/ankit/OneDrive/Desktop/ScalaD/crimes.csv")
      val dfselectcols = df.select("IncidntNum","Category","PdDistrict")
      val dfgrpbyarea = dfselectcols.groupBy(dfselectcols.col("PdDistrict"),dfselectcols.col("Category")).count()
      val dfareasort = dfgrpbyarea.orderBy(dfgrpbyarea.col("PdDistrict"),dfgrpbyarea.col("count").desc).limit(3).show()
   //   val dftop3 = dfareasort.repartition(dfareasort.col("PdDistrict")).show(3)
  }
}