package framework

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnector
import gdeltParsers.GKGParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import scd.IncrementalSCD
import utils.RelationBuilder

/**
  * Created by tariq on 02/05/18.
  */

object GDELTGraph {

  val cassandraParams = Map[String, String](
    "keySpace" -> "test",
    "table" -> "articles",
    "vertexTable" -> "entities",
    "edgeTable" -> "relations",
    "sampleEdgeTable" -> "sample_relations"
  )

  def loadGraph(spark: SparkSession) = {
    val entities = spark.read.cassandraFormat(
      cassandraParams("vertexTable"), cassandraParams("keySpace")
    ).load().drop("community", "comm2")
      .withColumn("community", functions.expr("name"))
      .withColumn("id", monotonically_increasing_id)
      .cache()
    val relations = spark.read.cassandraFormat(
      cassandraParams("edgeTable"), cassandraParams("keySpace")
    ).load().cache()

    Logger.getRootLogger.warn(s"raw vertices: ${entities.count}, raw edges: ${relations.count}")

    val edges = relations.join(entities, relations.col("src") === entities.col("name"))
      .drop("src", "name").selectExpr("id AS src", "weight", "dst")
      .join(entities, relations.col("dst") === entities.col("name"))
      .drop("dst", "name").selectExpr("src", "id AS dst", "weight")
      .filter(row => row.getAs[Long]("weight") > 1)
    GraphFrame(entities, edges)
  }

  def loadSampleGraph(spark: SparkSession, edgeCount: Double) = {
    val entities = spark.read.cassandraFormat(
      cassandraParams("vertexTable"), cassandraParams("keySpace")
    ).load().drop("community", "comm2")
      .withColumn("community", functions.expr("name"))
      .withColumn("id", monotonically_increasing_id)
      .cache()

    spark.read.cassandraFormat(
      cassandraParams("edgeTable"), cassandraParams("keySpace")
    ).load().limit((edgeCount * 1000000).toInt).write.cassandraFormat(
      cassandraParams("sampleEdgeTable"), cassandraParams("keySpace")
    ).save()

    val sampleRelations = spark.read.cassandraFormat(
      cassandraParams("sampleEdgeTable"), cassandraParams("keySpace")
    ).load().cache()

    Logger.getRootLogger.warn(s"raw vertices: ${entities.count}, raw edges: ${sampleRelations.count}")

    val edges = sampleRelations.join(entities, sampleRelations.col("src") === entities.col("name"))
      .drop("src", "name").selectExpr("id AS src", "weight", "dst")
      .join(entities, sampleRelations.col("dst") === entities.col("name"))
      .drop("dst", "name").selectExpr("src", "id AS dst", "weight").cache()

    GraphFrame(GraphFrame.fromEdges(edges).vertices.join(entities, "id"), edges)
  }

  def hdfsToGraph(spark: SparkSession) = {
    val rdd = spark.sparkContext.binaryFiles("hdfs://claas11.local/user/nifi/gkg_history/*.zip")
      .flatMap { case (zipFilePath, zipContent) =>
        val zipInputStream = new ZipInputStream(zipContent.open())
        Stream.continually(zipInputStream.getNextEntry)
          .takeWhile(_ != null)
          .flatMap { zipEntry =>
            val br = new BufferedReader(new InputStreamReader(zipInputStream))
            Stream.continually(br.readLine()).takeWhile(_ != null)
          }
      }

    val tupleRDD = parseRDD(rdd).cache()

    spark.createDataFrame(
      tupleRDD.flatMap(_._1).map{ case(name, index) => Row.fromSeq(Array(name, name)) },
      StructType(Seq(
        StructField(name = "name", dataType = StringType, nullable = false),
        StructField(name = "community", dataType = StringType, nullable = false)
      ))
    ).withColumn("fullrun", lit(false))
      .write.cassandraFormat(
      cassandraParams("vertexTable"),
      cassandraParams("keySpace")
    ).mode("append").save()

    spark.createDataFrame(
      tupleRDD.flatMap(_._2).map{ case(src, dst, w) => Row.fromSeq(Array(src, dst, w)) },
      StructType(Seq(
        StructField(name = "src", dataType = StringType, nullable = false),
        StructField(name = "dst", dataType = StringType, nullable = false),
        StructField(name = "weight", dataType = IntegerType, nullable = false)
      ))
    ).write.cassandraFormat(
      cassandraParams("edgeTable"),
      cassandraParams("keySpace")
    ).mode("append").save()

    tupleRDD.unpersist()
  }

  def persistStream(spark :SparkSession, rdd: RDD[String]) = {
    val tupleRDD = parseRDD(rdd).cache()

    spark.createDataFrame(
      tupleRDD.flatMap(_._1).map{ case(name, index) => Row.fromSeq(Array(name, name)) },
      StructType(Seq(
        StructField(name = "name", dataType = StringType, nullable = false),
        StructField(name = "comm2", dataType = StringType, nullable = false)
      ))
    ).withColumn("fullrun", lit(false))
      .write.cassandraFormat(
      cassandraParams("vertexTable"),
      cassandraParams("keySpace")
    ).mode("append").save()

    spark.createDataFrame(
      tupleRDD.flatMap(_._2).map{ case(src, dst, w) => Row.fromSeq(Array(src, dst, w)) },
      StructType(Seq(
        StructField(name = "src", dataType = StringType, nullable = false),
        StructField(name = "dst", dataType = StringType, nullable = false),
        StructField(name = "weight", dataType = IntegerType, nullable = false)
      ))
    ).write.cassandraFormat(
      cassandraParams("edgeTable"),
      cassandraParams("keySpace")
    ).mode("append").save()

    tupleRDD.unpersist()
  }

  def incrementStream(spark :SparkSession, graph: GraphFrame, rdd: RDD[String]) = {

    Logger.getRootLogger.warn(s"rdd ${rdd.count}")
    val tupleRDD = parseRDD(rdd).cache()
    Logger.getRootLogger.warn(s"tuples ${tupleRDD.count}")

    // NOTE make sure graph is canonical
    val newVs = spark.createDataFrame(
      tupleRDD.flatMap(_._1).map { case (name, index) => Row.fromSeq(Array(name)) },
      StructType(Seq(
        StructField(name = "name", dataType = StringType, nullable = false)
      ))
    ).cache()
    val newEs = spark.createDataFrame(
      tupleRDD.flatMap(_._2).map { case (src, dest, w) => Row.fromSeq(Array(src, dest, w)) },
      StructType(Seq(
        StructField(name = "src", dataType = StringType, nullable = false),
        StructField(name = "dst", dataType = StringType, nullable = false),
        StructField(name = "weight", dataType = IntegerType, nullable = false)
      ))
    ).cache()
    Logger.getRootLogger.warn(s"new vertices ${newVs.count}")
    Logger.getRootLogger.warn(s"new edges ${newEs.count}")

    appendGraph(graph, newVs, newEs, spark)

  }

  def incrementStreamCassandra(spark: SparkSession, rdd: RDD[String]) = {
    Logger.getRootLogger.warn(s"rdd ${rdd.count}")
    val tupleRDD = parseRDD(rdd).cache()
    Logger.getRootLogger.warn(s"tuples ${tupleRDD.count}")

    // NOTE make sure graph is canonical
    val newVs = spark.createDataFrame(
      tupleRDD.flatMap(_._1).map { case (name, index) => Row.fromSeq(Array(name, name)) },
      StructType(Seq(
        StructField(name = "name", dataType = StringType, nullable = false),
        StructField(name = "community", dataType = StringType, nullable = false)
      ))
    ).cache()
    val newEs = spark.createDataFrame(
      tupleRDD.flatMap(_._2).map { case (src, dst, w) => Row.fromSeq(Array(src, dst, w)) },
      StructType(Seq(
        StructField(name = "src", dataType = StringType, nullable = false),
        StructField(name = "dst", dataType = StringType, nullable = false),
        StructField(name = "weight", dataType = IntegerType, nullable = false)
      ))
    ).cache()
    Logger.getRootLogger.warn(s"new vertices ${newVs.count}")
    Logger.getRootLogger.warn(s"new edges ${newEs.count}")

    // updateCassandraGraph

    val before = System.currentTimeMillis()

    newVs.withColumn("fullrun", lit(false)).write.cassandraFormat(
      cassandraParams("vertexTable"), cassandraParams("keySpace")
    ).mode("append").save()

    newEs.write.cassandraFormat(
      cassandraParams("sampleEdgeTable"), cassandraParams("keySpace")
    ).mode("append").save()

    Logger.getRootLogger.warn(s"updating cassandra took ${(System.currentTimeMillis() - before) / 1000.0}")

    val graph = loadSampleGraph(spark, 78.5).cache()
    graph.vertices.count
    graph.edges.count

    Logger.getRootLogger.warn(s"loading to GraphX took ${(System.currentTimeMillis() - before) / 1000.0}")
  }

  def testQueries(spark: SparkSession) = {
    val entities = spark.read.cassandraFormat(
      cassandraParams("vertexTable"), cassandraParams("keySpace")
    ).load().drop("community", "comm2")
      .withColumn("community", functions.expr("name"))
      .withColumn("id", monotonically_increasing_id)
      .cache()
    val relations = spark.read.cassandraFormat(
      cassandraParams("edgeTable"), cassandraParams("keySpace")
    ).load().cache()

    val graph = GraphFrame(entities, relations)
    Logger.getRootLogger.warn(s"raw vertices: ${graph.vertices.count}, raw edges: ${graph.edges.count}")

    var before = System.currentTimeMillis()
    graph.vertices.filter("name = 'Phnom Penh Howes'").show()
    Logger.getRootLogger.warn(s"vertex lookup in GraphFrames took ${(System.currentTimeMillis() - before) / 1000.0}")

    before = System.currentTimeMillis()
    graph.edges.filter("src = 'Phnom Penh Howes'").show()
    Logger.getRootLogger.warn(s"neighbor lookup in GraphFrames took ${(System.currentTimeMillis() - before) / 1000.0}")
  }

  def persistGraph(regionalizedGraph: GraphFrame, communityDF: DataFrame): Unit = {
    regionalizedGraph.vertices.join(
      communityDF.selectExpr("id", "community"),
      regionalizedGraph.vertices.col("community") === communityDF.col("id"),
      joinType = "left"
    ).select("name", "community").withColumn("fullrun", lit(true)).write.cassandraFormat(
      cassandraParams("vertexTable"),
      cassandraParams("keySpace")
    ).mode("append").save()
  }

  private def parseRDD(rdd: RDD[String]) = {
    rdd.map(rawRecord => {
      // collect graph properties
      //      val gkgDoc = GKGParser.toCaseClass(rawRecord)
      //      val entities = gkgDoc.personsEnhanced.sortWith((x, y) => x._2 < y._2).toList
      // OR do it the optimized way!!
      val gkgDoc = GKGParser.toPersonsEnhanced(rawRecord)
      val entities = gkgDoc.sortWith((x, y) => x._2 < y._2).toList
      val relations = RelationBuilder.buildTuples(entities).map{ case(_1,_2) => (_1, _2, 1) }
      (entities, relations)
    })
  }

  def appendGraph(graph: GraphFrame, newVs: DataFrame, newEs: DataFrame, spark: SparkSession): GraphFrame = {
    val before = System.currentTimeMillis()

    val fullGraph = GraphFrame(
      graph.vertices.join(newVs, Seq("name"), "outer")
        .select("name", "community")
        .withColumn("id", monotonically_increasing_id),
      graph.edges.union(newEs)
    ).cache()
    fullGraph.vertices.count()
    fullGraph.edges.count()

    Logger.getRootLogger.warn(s"appending in GraphX took ${(System.currentTimeMillis() - before) / 1000}")

    fullGraph
  }

}
