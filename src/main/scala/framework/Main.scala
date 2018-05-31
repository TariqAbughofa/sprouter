package framework

import java.nio.charset.StandardCharsets

import com.datastax.spark.connector.cql.CassandraConnector
import org.graphframes.GraphFrame
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.nifi.spark._
import org.apache.nifi.remote.client._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import scd.GraphFrameOps._

/**
  * Created by tariq on 28/11/17.
  */

object Main {

  /* TEST:
    import org.apache.spark.graphx._
    import org.apache.spark.rdd.RDD
    val users: RDD[(VertexId, (String))] =
      sc.parallelize(Array((1L, "collab"),(2L, "collab"),
        (3L, "collab"),(4L, "collab"),(5L, "collab"),(6L, "collab")))
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(4L, 2L, "collab"),
        Edge(4L, 3L, "collab"),Edge(3L, 2L, "collab"),
        Edge(2L, 1L, "collab"),Edge(5L, 1L, "collab"),
        Edge(6L, 1L, "collab"), Edge(6L, 5L, "collab")))
    val graph = Graph(users, relationships)
   */

  val HDFS_TO_GRAPH = 0
  val TEST_QUERIES = 1
  val TEST_STREAM = 2
  val TEST_CASSANDRA_STREAM = 3

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CommunityDetection")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("spark://claas13.local:7077")
    }

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
//    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
//    }

    Logger.getRootLogger.warn("Getting context!!")

    // Load and partition Graph
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()

    CassandraConnector(spark.sparkContext).withSessionDo { session =>
      session.execute("""CREATE TABLE IF NOT EXISTS test.entities(
          |    name VARCHAR PRIMARY KEY,
          |    community VARCHAR,
          |    comm2 VARCHAR,
          |    fullrun BOOLEAN
          |  );""".stripMargin)
      session.execute("""CREATE TABLE IF NOT EXISTS test.relations(
          |    src VARCHAR,
          |    dst VARCHAR,
          |    weight COUNTER,
          |    PRIMARY KEY (src, dst)
          |  );""".stripMargin)
      session.execute("""CREATE TABLE IF NOT EXISTS test.sample_relations(
                        |    src VARCHAR,
                        |    dst VARCHAR,
                        |    weight COUNTER,
                        |    PRIMARY KEY (src, dst)
                        |  );""".stripMargin)
    }

    Logger.getRootLogger.warn("We have context!!")

    val operation = HDFS_TO_GRAPH

    operation match {
      case HDFS_TO_GRAPH =>
        GDELTGraph.hdfsToGraph(spark)

      case TEST_QUERIES =>
        GDELTGraph.testQueries(spark)

      case TEST_STREAM =>
        val graph = GDELTGraph.loadSampleGraph(spark, 87.5).cache()
        // stream data
        val conf = new SiteToSiteClient.Builder().url("http://claas15.local:9090/nifi").portName("spark").buildConfig()
        val stream = ssc.receiverStream(new NiFiReceiver(conf, StorageLevel.MEMORY_ONLY)).window(Seconds(10))
          .map(dataPacket => new String(dataPacket.getContent, StandardCharsets.UTF_8))
        stream.foreachRDD(rdd => {
          if (rdd.count > 0) {
            GDELTGraph.incrementStream(spark, graph, rdd)
          }
        })

      case TEST_CASSANDRA_STREAM =>
        val conf = new SiteToSiteClient.Builder().url("http://claas15.local:9090/nifi").portName("spark").buildConfig()
        val stream = ssc.receiverStream(new NiFiReceiver(conf, StorageLevel.MEMORY_ONLY)).window(Seconds(10))
          .map(dataPacket => new String(dataPacket.getContent, StandardCharsets.UTF_8))
        stream.foreachRDD(rdd => {
          if (rdd.count > 0) {
            GDELTGraph.incrementStreamCassandra(spark, rdd)
          }
        })
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
