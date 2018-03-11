package ankitproj1.pkg1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object Filtermovie {
  def main(Args:Array[String])
  {
    val conf = new SparkConf()
    .setAppName("sampleprogram")
          .setMaster("local")
    val sc = new SparkContext(conf)
    val filerdd = sc.textFile("C:/Users/ankit/OneDrive/Desktop/Movie.csv")
    val wordrdd = filerdd.filter(line => line.contains("Robert"))
    wordrdd.collect().foreach(println)
    
  }
}