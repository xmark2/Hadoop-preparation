/**check spark version*/

spark-submit --version

/**spark schell*/

spark-shell --master yarn --conf spark.ui.port=12654


/**du datausage s summary h convert to MB*/
hdfs dfs -du -s -h /public/retail_db


spark-shell --master yarn \
	--conf spark.ui.port=12654 \
	--num-executors 1 \
	--executor-memory 512M

/*stop spark context*/
sc.stop



/**initialize programmatically*/
import org.apache.spark.{SparkConf, SparkContext}
val conf = new SparkConf().setAppName("Daily Revenue").setMaster("yarn-client")
val sc = new SparkContext(conf)
///****check later*/


spark-shell --master yarn \
	--conf spark.ui.port=12654 \
	--num-executors 1 \
	--executor-memory 512M
sc.getConf.getAll.foreach(println)


import org.apache.spark.{SparkConf, SparkContext}
val conf = new SparkConf().setAppName("Daily Revenue").setMaster("yarn-client")
val sc = new SparkContext(conf)
sc.getConf.getAll.foreach(println)
