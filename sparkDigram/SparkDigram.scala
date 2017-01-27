import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf



object SparkDigram {
 def main(args: Array[String]) {
 val sc = new SparkContext(new SparkConf().setAppName("DiGram").setMaster("local[8]"))
 val slices = 4
 val textFile = sc.textFile("./livress") //emplacement du fichier, 1 ligne = 1 phrase
 val counts = textFile.flatMap(line => line.trim.split(" ").sliding(2))
		.map{ a => a.mkString("#") }
                .map{word => (word, 1)}
		.reduceByKey(_+_)



counts.saveAsTextFile("./digram.txt")

 }
}
