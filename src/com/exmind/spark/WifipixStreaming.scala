package com.exmind.spark

import com.exmind.sql.ConnectionPool
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

/**
  * Created by 38376 on 2017/3/7.
  */
object WifipixStreaming {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Wifipix Streaming")
    val ssc = new StreamingContext(conf, Seconds(30))
    ssc.checkpoint("./checkpoint")

    val lines = ssc.socketTextStream(args(0), args(1).toInt)

    // e4:95:6e:4e:e9:01	2017-03-07 14:52:54	0c:1d:af:43:fd:cc	-84
    val wifipixUserMacPair = lines.map(line => {
      val cols = line.split("\t")
      val date = cols(1).substring(0, 10)
      // wifipix_mac#date    user_mac
      (cols(0) + "#" + date, 1)

    })

    val addFunc = (currentValues: Seq[Int], preValueState: Option[Int]) => {
      val currentCount = currentValues.sum
      val previousCount = preValueState.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val totalMacCounts = wifipixUserMacPair.updateStateByKey[Int](addFunc)

    totalMacCounts.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val conn = ConnectionPool.getConnection("jdbc:mysql://172.30.103.14:3306/test", "masa", "masa")
        conn.setAutoCommit(false)
        val stmt = conn.createStatement()
        partitionOfRecords.foreach (
          record => {
            val keyList = record._1.split("#")
            stmt.addBatch("insert into wifipix_online(wifipix_mac, load_date, user_mac_count) " +
              "values('"+ keyList(0) +"', '"+ keyList(1) +"', "+ record._2 +")  " +
              "on duplicate key update user_mac_count="+ record._2)
          })

        stmt.executeBatch()
        conn.commit()

        ConnectionPool.closeConn(null, stmt, conn)  // return to the pool for future reuse
      }
    }

    totalMacCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
