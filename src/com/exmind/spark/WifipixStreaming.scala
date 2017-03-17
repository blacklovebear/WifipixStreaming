package com.exmind.spark

import java.text.SimpleDateFormat

import com.exmind.sql.ConnectionPool
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import scala.util.Try

/**
  * Created by 38376 on 2017/3/13.
  */
object WifipixStreaming {

  def save_day_visit_user_with_visit_time(lines: ReceiverInputDStream[String]): Unit = {
    val mappingFunc= (key: (String, String, String), value: Option[(Int, String)], state:State[(Int,String,String)]) => {
      val stateValue: (Int, String, String) = state.getOption().getOrElse[(Int,String,String)]((0,"",""))
      val newValue: (Int, String) = value.getOrElse[(Int,String)]((0, ""))
      val sum = stateValue._1 + newValue._1
      //           (sum, fist_time, last_time)
      var result = (sum, newValue._2, newValue._2)

      if (state.exists()) result = (sum, stateValue._2, newValue._2)
      state.update(result)
      (key, result)
    }

    lines
      .map(line => {
        // e4:95:6e:4e:e9:01	2017-03-07 14:52:54	0c:1d:af:43:fd:cc	-84
        val cols = line.split("\t")
        val date = cols(1).substring(0, 10)
        val time = cols(1).substring(11, 19)
        // (wifipix_mac date user_mac) -> (1, time)
        ( (cols(0), date, cols(2)), (1, time))
      })
      // timeout function intent delete  over time key
      .mapWithState(StateSpec.function(mappingFunc).timeout(Minutes(60 * 24)))
        // change to (wifipix_mac, date) => (客流量，到店客户，用户访问时长)
      .map{ case (key, value) =>
        val firstDate = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss").parse(key._2 + " " + value._2)
        val lastDate = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss").parse(key._2 + " " + value._3)

        val intervalMinute: Long = (lastDate.getTime - firstDate.getTime ) / 1000 / 60
        // 客流量，到店客户，用户访问时长（分钟）。 客流量:访问时长一分钟以上，到店客户:访问时长10分钟以上
        val resultValue = Array(0, 0, 0)
        if (intervalMinute >= 1) {
          resultValue.update(0, 1)
          if (intervalMinute >= 10 ) resultValue.update(1, 1)
          resultValue.update(2, intervalMinute.toInt)
        }

        ((key._1, key._2), resultValue match {case Array(a,b,c) => (a,b,c)} )
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
      .foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfRecords =>
          val conn = ConnectionPool.getConnection("jdbc:mysql://172.30.103.14:3306/test", "masa", "masa")
          conn.setAutoCommit(false)
          val stmt = conn.createStatement()
          partitionOfRecords.foreach { case (key, value) =>
              val avgDuration = Try( value._3/ value._1).getOrElse(0)

              stmt.addBatch("insert into wifipix_online_with_time(wifipix_mac, load_date, passenger_flow, shop_count, avg_duration_minute) " +
                "values('"+ key._1 +"', '"+ key._2 +"', "+ value._1 +", "+ value._2 +", "+ avgDuration +")  " +
                "on duplicate key update " +
                "passenger_flow = IF(passenger_flow < "+ value._1 +", "+ value._1 +", passenger_flow), " +
                "shop_count = IF(shop_count < "+ value._2 +", "+ value._2 +", shop_count), " +
                "avg_duration_minute = IF(avg_duration_minute < "+ avgDuration +", "+ avgDuration +", avg_duration_minute)"
              )
            }

          stmt.executeBatch()
          conn.commit()
          ConnectionPool.closeConn(null, stmt, conn)  // return to the pool for future reuse
        }
      }
  }

  def save_day_visit_user(lines: ReceiverInputDStream[String]): Unit = {
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
      .map { case (key, value) =>
        ((key._1, key._2), (value, 1))
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfRecords =>
          val conn = ConnectionPool.getConnection("jdbc:mysql://172.30.103.14:3306/test", "masa", "masa")
          conn.setAutoCommit(false)
          val stmt = conn.createStatement()
          partitionOfRecords.foreach { case (key, value) =>
              stmt.addBatch("insert into wifipix_online(wifipix_mac, load_date, user_mac_count, unique_mac_count) " +
                "values('"+ key._1 +"', '"+ key._2 +"', "+ value._1 +", "+ value._2 +")  " +
                "on duplicate key update " +
                "user_mac_count = IF(user_mac_count < "+ value._1 +", "+ value._1 +", user_mac_count), " +
                "unique_mac_count = IF(unique_mac_count < "+ value._2 +", "+ value._2 +", unique_mac_count)")
          }

          stmt.executeBatch()
          conn.commit()
          ConnectionPool.closeConn(null, stmt, conn)  // return to the pool for future reuse
        }
      }
  }


  def save_hour_visit_user(lines: ReceiverInputDStream[String]): Unit = {
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
      .map { case (key, value) => ((key._1, key._2, key._3), (value, 1)) }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfRecords =>
          val conn = ConnectionPool.getConnection("jdbc:mysql://172.30.103.14:3306/test", "masa", "masa")
          conn.setAutoCommit(false)
          val stmt = conn.createStatement()

          partitionOfRecords.foreach { case (key, value) =>
            stmt.addBatch("insert into wifipix_online_hour(wifipix_mac, load_date, load_hour, user_mac_count, unique_mac_count) " +
              "values('"+ key._1 +"', '"+ key._2 +"', '"+ key._3 +"', "+ value._1 +", "+ value._2 +")  " +
              "on duplicate key update " +
              "user_mac_count = IF(user_mac_count < "+ value._1 +", "+ value._1 +", user_mac_count), " +
              "unique_mac_count = IF(unique_mac_count < "+ value._2 +", "+ value._2 +", unique_mac_count)")
          }
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

//    save_day_visit_user(lines)
//    save_hour_visit_user(lines)
    save_day_visit_user_with_visit_time(lines)

    ssc.start()
    ssc.awaitTermination()
  }

}
