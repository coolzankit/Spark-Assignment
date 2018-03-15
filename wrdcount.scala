package ankitproj1.pkg1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object wrdcount {
  def main(Args:Array[String])
  {
    val conf = new SparkConf()
    .setAppName("sampleprogram")
          .setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("C:/Users/ankit/OneDrive/Desktop/test.txt")
    val splitrdd = rdd.flatMap(f => f.split(" "))
    val kprdd = splitrdd.map(f => (f,1))
    val countrdd = kprdd.reduceByKey((f,q) => (f+q))
    countrdd.collect().foreach(println)
 //   kprdd.collect().foreach(println)   
}
}