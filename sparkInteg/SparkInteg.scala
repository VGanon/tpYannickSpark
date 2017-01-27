/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
import scala.math.random
import org.apache.spark._
import org.apache.spark.SparkContext._

/* Computes an approximation to integral */
object SparkIntegral {
 
 def main(args: Array[String]) { 
    val conf = new SparkConf()
	.setMaster("local[8]")
	.setAppName("Spark Integral")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 8
    val n = math.min(100000L * slices, Int.MaxValue).toInt 
    val N = 1000000
    val h = 9.0/N
    val t0 = System.nanoTime()
   val count = spark.parallelize(0 until N-1, slices).map { i =>
         
	1/(1+(i*h))	
    }.reduce(_ + _)
    println("\n \n valeur intégrale " + count*h +"\n \n")
    val t1 = System.nanoTime()
    println("\n\n nb rectangles : " + N + "\n\n nb slices : " + slices + "\n\n  temps d'exécution : " + (t1 - t0)/1000000.0 + " ms \n \n")
    spark.stop()
  }
 
 
}
// scalastyle:on println
