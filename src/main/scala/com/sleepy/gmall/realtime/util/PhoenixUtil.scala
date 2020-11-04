package com.sleepy.gmall.realtime.util


import java.sql._

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import org.apache.phoenix.spark._

object PhoenixUtil {
    val zookeeper: String = PropertiesUtil.zookeeperAddress

    /**
     * 根据查询语句查询 phoenix 封装为 List<{列名:值,列名:值,...}>
     *
     * @param sql
     * @return
     */
    def queryList(sql: String): List[JSONObject] = {
        val rsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
        //建立连接
        val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop22,hadoop23,hadoop24:2181")
        //创建数据库操作对象
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //执行SQL语句
        val rs: ResultSet = ps.executeQuery()
        //获取表中 列信息*
        val rsMetaData: ResultSetMetaData = rs.getMetaData
        //处理结果集
        while (rs.next()) {
            val userStatusJsonObj = new JSONObject()
            for (i <- 1 to rsMetaData.getColumnCount) {
                userStatusJsonObj.put(rsMetaData.getColumnName(i), rs.getObject(i))
            }
            rsList.append(userStatusJsonObj)
        }
        //释放资源
        rs.close()
        ps.close()
        conn.close()
        rsList.toList
    }

    /**
     * 直接封装返回对应的bean   pheenix查出的json 列名是大写的,但是不用处理
     * fast会自动进行大小写转化
     */
    def queryList[T](sql: String, clazz: Class[T]): List[T] = {
        val listJsonObj: List[JSONObject] = queryList(sql)
        val list: List[T] = listJsonObj.map(item => {
            JSON.toJavaObject(item, clazz)
        })
        list

    }

    def saveRdd(tableName: String, cols: Seq[String], rdd: RDD[_ <: Product]): Unit = {
        rdd.saveToPhoenix(
            tableName,
            cols,
            new Configuration,
            Some(zookeeper)
        )
    }
}
