package com.gaogf.dataloader

import java.util.Properties

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by pactera on 2018/3/6.
  */
object mysqlDB {

  case class zbh_test(day_id:String, prvnce_id:String,pv_cnts:Int)

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("mysql").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //sc.addJar("D:\\workspace\\sparkApp\\lib\\mysql-connector-java-5.0.8-bin.jar")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)



    //定义mysql信息
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url"->"jdbc:mysql://localhost:3306/db_ldjs",
        "dbtable"->"(select imei,region,city,company,name from tb_user_imei) as some_alias",
        "driver"->"com.mysql.jdbc.Driver",
        "user"-> "root",
        //"partitionColumn"->"day_id",
        "lowerBound"->"0",
        "upperBound"-> "1000",
        //"numPartitions"->"2",
        "fetchSize"->"100",
        "password"->"123456")).load()


    jdbcDF.collect().take(20).foreach(println)
    //jdbcDF.rdd.saveAsTextFile("C:/Users/zhoubh/Downloads/abi_sum")
    val url="jdbc:mysql://localhost:3306/gaogf"
    val prop=new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")
    //jdbcDF.write.mode(SaveMode.Overwrite).jdbc(url,"zfs_test",prop)
    jdbcDF.write.mode(SaveMode.Append).jdbc(url,"words",prop)
    JdbcUtils.jdbc(url,jdbcDF,"words",prop)

    //org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.saveTable(jdbcDF,url,"zbh_test",prop)
    ////    #然后进行groupby 操作,获取数据集合
    //    val abi_sum_area = abi_sum.groupBy("date_time", "area_name")
    //
    ////    #计算数目，并根据数目进行降序排序
    //    val sorted = abi_sum_area.count().orderBy("count")
    //
    ////    #显示前10条
    //    sorted.show(10)
    //
    ////    #存储到文件（这里会有很多分片文件。。。）
    //    sorted.rdd.saveAsTextFile("C:/Users/zhoubh/Downloads/sparktest/flight_top")
    //
    //
    ////    #存储到mysql表里
    //    //sorted.write.jdbc(url,"table_name",prop)


  }
}
