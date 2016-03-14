import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.MutableList

/* Do not include any other Spark libraries.
 * e.g. import org.apache.spark.sql._ */

/* Part5
 * This program prints out the 10 pages with the highest pagerank. */

object Part5 {
    def main(args: Array[String]) {
        if (args.length <= 1 || 4 <= args.length) {
            System.err.println("Usage: Part5 <links-simple-sorted> <titles-sorted> <iterations>")
            System.exit(1)
        }

        val d = 0.85

        val iters = if (args.length == 3) args(2).toInt else 10

        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("Part5")
            //.set("spark.executor.memory", "4g")

        val sc = new SparkContext(conf)


        def expandValue(t1:String,t2:String): (Int,Array[Int]) = {
            val t3 = t2.split(" ").filter(_ != "").map(v => v.toInt)
            (t1.toInt,t3)
        }

        def convertJoinResult(t1:Int,t2:(Option[Array[Int]],String)):(Int, Array[Int]) = {
            if(t2._1 == None){  // (1, (None, A))
                return (t1, Array[Int]()) // (1, ())
            }else{  //(2,((1,3,5),B))
                return (t1, t2._1.get)  //(2,(1,3,5))
            }
        }

        def convertJoinResult2(t1:Int,t2:(Option[Int],String)):(Int, Double) = {
            if(t2._1 == None){  
                return (t1, 0) 
            }else{  
                return (t1, t2._1.get.toDouble) 
            }
        }

        def countInLinks(t1:Int, t2:Array[Int]): List[(Int, Int)] = {
            var l = MutableList[(Int, Int)]()
            t2.foreach( v => l+=((v.toInt, 1)))
            l.toList
        }


        val titles = sc
        //.textFile("/Users/wangtianyi/Downloads/hw3/hw3_template/sample_input/sample_titles-sorted.txt") // readin the input text file
        .textFile(args(1))
        .zipWithIndex()
        .map{case(k,v) => (v.toInt + 1,k)}  //(1,A) (2,B)


        val links = sc
        //.textFile("/Users/wangtianyi/Downloads/hw3/hw3_template/sample_input/sample_links-simple-sorted.txt") // readin the input text file
        .textFile(args(0)) // readin the input text file
        .map{line => 
            val s = line.split(":")
            (s(0), s(1))}
        .map{case(k,v) => expandValue(k,v)}  // (2,(3,6,7)), (3,(2,5))
        .rightOuterJoin(titles)  //(2, ((3,6,7),B)) (1,(None,A))
        .map{case(k,v) => convertJoinResult(k,v)} //(1,()),(2,(3,6,7))
        .persist()
        

        val N = sc
        .textFile(args(1))
        .count()


        var ranks = titles
        .mapValues(v => (100/N.toDouble)) //(1,100/N), (2,100/N)

        val no_inlinks = links  //count in links
        .flatMap{case(k,v) => countInLinks(k,v)} //create a function, for each line, e.g.:5: 2 4 6 => flatmap this to (2,1),(4,1),(6,1)
        .reduceByKey(_+_) //(5,3),(3,1),(2,7)
        .rightOuterJoin(titles) //(5,(3,A)), (4,(None,D))
        .map{case(k,v) => convertJoinResult2(k,v)}
        .filter(t => t._2 == 0)

        /* PageRank */
        for (i <- 1 to iters) {  
            var inlinks = links.join(ranks)  //(1,((),PR0)), (2,((3,6,7),PR0))
            .values   //((),PR0),((3,6,7),PR0)
            .flatMap{case(k,v) =>
                val outy = k.size
                if(outy != 0)
                    k.map{u => (u, v/outy)}
                else
                    (1:N).map{j => j/N.toDouble} //sink
            }
            
            ranks = inlinks
            .reduceByKey(_+_)
            .union(no_inlinks)
            .mapValues(v => v*d + (1-d)*100/N.toDouble)
        }


        var finalRanks = ranks //(1,0.88),(2,1.45),(5,8.23)
        .join(titles)  //(1,(0.88,A))
        .map{case(k,v) => (k, v._2,v._1)} //(1, A,0.88)
        .takeOrdered(10)(Ordering[Double].reverse.on(x => x._3))

        println("\n[ Page Rank Top 10 ]")
        finalRanks.foreach(println)


        
    }
}
