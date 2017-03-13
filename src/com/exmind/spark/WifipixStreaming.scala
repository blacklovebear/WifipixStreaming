package com.exmind.spark

import com.exmind.sql.ConnectionPool
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
  * Created by 38376 on 2017/3/13.
  */
object WifipixStreaming {

  def save_day_visit_user(lines: ReceiverInputDStream[String]) = {
    val mappingFunc= (key: (String, String, String), value: Option[Int], state:State[Int]) => {
      val sum = value.getOrElse(0) + state.getOption().getOrElse(0)
      state.update(sum)
      (key, sum)
    }

    lines
      .map(line => {
        val cols = line.split("\t")
        val date = cols(1).substring(0, 10)
        // wifipix_mac date user_mac
        ( (cols(0), date, cols(2)), 1)
      })
      // timeout function intent delete  over time key
      .mapWithState(StateSpec.function(mappingFunc).timeout(Minutes(60 * 24)))
        // change to (wifipix_mac, date) => (tatal, unique)
        .map( keySum => {
        val key = keySum._1
        ((key._1, key._2), (keySum._2, 1))
      })
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfRecords =>
          val conn = ConnectionPool.getConnection("jdbc:mysql://172.30.103.14:3306/test", "masa", "masa")
          conn.setAutoCommit(false)
          val stmt = conn.createStatement()
          partitionOfRecords.foreach (
            record => {
              val key = record._1
              val value = record._2
              stmt.addBatch("insert into wifipix_online(wifipix_mac, load_date, user_mac_count, unique_mac_count) " +
                "values('"+ key._1 +"', '"+ key._2 +"', "+ value._1 +", "+ value._2 +")  " +
                "on duplicate key update " +
                "user_mac_count = IF(user_mac_count < "+ value._1 +", "+ value._1 +", user_mac_count), " +
                "unique_mac_count = IF(unique_mac_count < "+ value._2 +", "+ value._2 +", unique_mac_count)")
            })

          stmt.executeBatch()
          conn.commit()
          ConnectionPool.closeConn(null, stmt, conn)  // return to the pool for future reuse
        }
      }
  }


  def save_hour_visit_user(lines: ReceiverInputDStream[String]) = {
    val mappingFunc= (key: (String, String, String, String), value: Option[Int], state:State[Int]) => {
      val sum = value.getOrElse(0) + state.getOption().getOrElse(0)
      state.update(sum)
      (key, sum)
    }

    lines
      .map(line => {
        val cols = line.split("\t")
        val date = cols(1).substring(0, 10)
        val hour = cols(1).substring(11, 13)
        // wifipix_mac date hour user_mac
        ( (cols(0), date, hour, cols(2)), 1)
      })
      // timeout function intent delete  over time key
      .mapWithState(StateSpec.function(mappingFunc).timeout(Minutes(60)))
      // change to (wifipix_mac, date, hour) => (tatal, unique)
      .map( keySum => {
        val key = keySum._1
        ((key._1, key._2, key._3), (keySum._2, 1))
      })
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfRecords =>
          val conn = ConnectionPool.getConnection("jdbc:mysql://172.30.103.14:3306/test", "masa", "masa")
          conn.setAutoCommit(false)
          val stmt = conn.createStatement()
          partitionOfRecords.foreach (
            record => {
              val key = record._1
              val value = record._2
              stmt.addBatch("insert into wifipix_online_hour(wifipix_mac, load_date, load_hour, user_mac_count, unique_mac_count) " +
                "values('"+ key._1 +"', '"+ key._2 +"', '"+ key._3 +"', "+ value._1 +", "+ value._2 +")  " +
                "on duplicate key update " +
                "user_mac_count = IF(user_mac_count < "+ value._1 +", "+ value._1 +", user_mac_count), " +
                "unique_mac_count = IF(unique_mac_count < "+ value._2 +", "+ value._2 +", unique_mac_count)")
            })

          stmt.executeBatch()
          conn.commit()
          ConnectionPool.closeConn(null, stmt, conn)  // return to the pool for future reuse
        }
      }
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Wifipix Streaming")
    val ssc = new StreamingContext(conf, Seconds(30))
    ssc.checkpoint("./checkpoint")

    // e4:95:6e:4e:e9:01	2017-03-07 14:52:54	0c:1d:af:43:fd:cc	-84
    val lines = ssc.socketTextStream(args(0), args(1).toInt)

    save_day_visit_user(lines)
    save_hour_visit_user(lines)

    ssc.start()
    ssc.awaitTermination()
  }

}
