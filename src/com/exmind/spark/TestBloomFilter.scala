package com.exmind.spark

import java.sql.Connection

import com.exmind.sql.ConnectionPool
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException
import orestes.bloomfilter.BloomFilter
import orestes.bloomfilter.redis.BloomFilterRedis
import org.apache.commons.dbcp.BasicDataSource

/**
  * Created by 38376 on 3/16/2017.
  */
object TestBloomFilter {




  def main(args: Array[String]): Unit = {

//    val bf: BloomFilter[String] = new BloomFilter(1000, 0.1)
//
//    bf.add("Just")
//
//    bf.add("a")
//    bf.add("test.")
//
//s
//    //Test if they are contained
//    print(bf.contains("Just")); //true
//    print(bf.contains("a")); //true
//    print(bf.contains("test.")); //true



//    val first: BloomFilterRedis[String] = new BloomFilterRedis[String]("172.30.103.14", 6379, 1000, 0.01);

//    for ( i <- 1 to 100){
//
//      first.add("test" + i.toString);
//    }

//    print(first.contains("test2"))

    println( 0 | 0 )


  }

}
