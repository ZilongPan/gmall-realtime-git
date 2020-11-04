package com.sleepy.gmall.realtime.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer


object ClickhouseUtil {

    def main(args: Array[String]): Unit = {
        val list: List[JSONObject] = queryList("select * from t_order_wide")
        //println(list(0).toJSONString)


    }
    def queryList(sql: String): List[JSONObject] = {
        val rsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
        //注册驱动
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
        //建立连接
        val conn: Connection = DriverManager.getConnection("jdbc:clickhouse://hadoop22:8123/default")

        //println(conn.getMetaData().getDriverVersion())
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

    /*def save(rdd: RDD[_ <: Product]): Unit = {
        //向ClickHouse中保存数据
        //创建SparkSession对象
        val spark2: SparkSession = SparkSession.builder()
          .master("local[*]")
          .appName("sparkSqlOrderWide")
          .getOrCreate()
        implicit spark2.implicits._
        val df: DataFrame = rdd.toDF
        df.write.mode(SaveMode.Append)
          .option("batchsize", "100")
          .option("isolationLevel", "NONE") // 设置事务
          .option("numPartitions", "4") // 设置并发
          .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
          .jdbc("jdbc:clickhouse://hadoop22:8123/default", "t_order_wide", new Properties())
    }*/
}
