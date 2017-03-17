package com.exmind.spark

import java.text.SimpleDateFormat

import com.exmind.sql.ConnectionPool
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming._

import scala.util.Try

/**
  * Created by 38376 on 3/16/2017.
  */
object WifipixBloomfilterStreaming {

  def save_day_visit_user_with_visit_time(lines: ReceiverInputDStream[String]): Unit = {
    val mappingFunc= (key: (String, String, String), value: Option[(Int, String, Int)], state:State[(Int, String, String, Int)]) => {
      //              sum, first_time, last_time, is_new_mac
      val stateValue: (Int, String, String, Int) = state.getOption().getOrElse[(Int,String,String, Int)]((0,"","", 0))
      //             count, time, is_new_mac
      val newValue: (Int, String, Int) = value.getOrElse[(Int,String, Int)]((0, "", 0))
      val sum = stateValue._1 + newValue._1
      // 只要当天有一次表示为 new_mac 它就是 new_mac
      val isNewMac = stateValue._4 | newValue._3
      //           (sum, fist_time, last_time, is_new_mac)
      var result = (sum, newValue._2, newValue._2, isNewMac)

      if (state.exists()) result = (sum, stateValue._2, newValue._2, isNewMac)
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
      .mapPartitions (partition => {
        val userFilter = new UserMacBloomfilter(0.1, 1000000, "172.30.103.14", 6379,
                                                "jdbc:mysql://172.30.103.14:3306/test", "masa", "masa")
        val newPartition = partition.map {case (key, value) =>
          val insertSuccess = userFilter.insertIntoDB(key._1, key._3, key._2, value._2)
          //                 (count, time, is_new_mac)
          if (insertSuccess) (key, (value._1, value._2, 1))
          else (key, (value._1, value._2, 0))
        }
        newPartition
      })
      // timeout function intent delete  over time key
      .mapWithState(StateSpec.function(mappingFunc).timeout(Minutes(60 * 24)))
      // change to (wifipix_mac, date) => (客流量，到店客户，用户访问时长)
      .map{ case (key, value) =>
        //                  去重设备，新设备
        ((key._1, key._2),  (1, value._4))
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfRecords =>
          val conn = ConnectionPool.getConnection("jdbc:mysql://172.30.103.14:3306/test", "masa", "masa")
          conn.setAutoCommit(false)
          val stmt = conn.createStatement()

          partitionOfRecords.foreach { case (key, value) =>
            val oldUser = value._1 - value._2
            stmt.addBatch("insert into wifipix_online_new_old(wifipix_mac, load_date, new_user, old_user) " +
                          "values('"+ key._1 +"', '"+ key._2 +"', "+ value._2 +", "+ oldUser +")  " +
                          "on duplicate key update " +
                          "new_user = IF(new_user < "+ value._2 +", "+ value._2 +", new_user), " +
                          "old_user = IF(old_user < "+ oldUser +", "+ oldUser +", old_user) "
            )
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

    val userFilter = new UserMacBloomfilter(0.1, 1000000, "172.30.103.14", 6379,
      "jdbc:mysql://172.30.103.14:3306/test", "masa", "masa")
    // load history mac into redis
    userFilter.loadToBloomfilter()

    save_day_visit_user_with_visit_time(lines)

    ssc.start()
    ssc.awaitTermination()

  }

}
