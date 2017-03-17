package com.exmind.spark

import com.exmind.sql.ConnectionPool
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException
import orestes.bloomfilter.redis.BloomFilterRedis

/**
  * Created by 38376 on 3/16/2017.
  */
class UserMacBloomfilter(falsePositiveProbability: Double, expectedNumberOfElements: Int,
                         redisHost: String, redisPort: Int,
                         dbUrl: String, userName: String, password: String) {
  private val bloomFilter = new BloomFilterRedis[String](redisHost, redisPort, expectedNumberOfElements, falsePositiveProbability)
  ConnectionPool.init(dbUrl, userName, password)

  def loadToBloomfilter(): Unit = {
    val conn = ConnectionPool.getConnection()
    val stmt = conn.createStatement
    val resultSet = stmt.executeQuery("SELECT  wifipix_mac, user_mac from wifipix_history_mac")

    while(resultSet.next()) {
      val bloomKey = resultSet.getString(1) + resultSet.getString(2)
      bloomFilter.add(bloomKey)
    }

    ConnectionPool.closeConn(resultSet, stmt, conn)
  }


  def insertIntoDB(wifipixMac: String, userMac: String, addDate:String, addTime: String): Boolean = {
    var insertSuccess = false

    if ( bloomFilter.contains(wifipixMac + userMac) ) return insertSuccess

    val conn = ConnectionPool.getConnection
    val stmt = conn.createStatement()
    try {
      stmt.execute(" insert into wifipix_history_mac(wifipix_mac, user_mac, add_date, add_time) " +
        "VALUES ('"+ wifipixMac +"', '"+ userMac +"', '"+ addDate +"', '"+ addTime +"') ")
      insertSuccess = true
    } catch {
      case _: MySQLIntegrityConstraintViolationException => "do nothing"
      case ex:Throwable => ex.printStackTrace()

    } finally {
      ConnectionPool.closeConn(null, stmt, conn)
    }

    return insertSuccess
  }
}
