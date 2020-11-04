package com.sleepy.gmall.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object ESUtil {

    //声明Jest客户端工厂
    private var jestFactory: JestClientFactory = _
    //初始化jestFactory
    build()

    def build(): Unit = {
        val nodeList = new util.ArrayList[String]()
        PropertiesUtil.properties
          .getProperty("es.node.list")
          .split(",")
          .map(nodeList.add)

        jestFactory = new JestClientFactory
        jestFactory.setHttpClientConfig(
            //配置es集群地址
            new HttpClientConfig.Builder(nodeList)
              //开启多线程处理
              .multiThreaded(true)
              //最大连接数
              .maxTotalConnection(20)
              //连接等待时间 单位ms
              .connTimeout(10000)
              //操作es是的超时时间  单位ms
              .readTimeout(1000)
              .build()
        )
    }

    //提供获取Jest客户端的方法
    def getJestClient(): JestClient = {
        jestFactory.getObject
    }

    /**
     * 想ES中批量保存数据
     *
     * @param datas     List[id,一条数据]
     * @param indexName 指定要保存至ES的索引名称
     */
    def bulkInsert(datas: List[(String, Any)], indexName: String): Unit = {
        if (Util.isEmpty(datas)) {
            return
        }
        //获取客户端
        val jestClient: JestClient = getJestClient()
        val bulkBuilder: Bulk.Builder = new Bulk.Builder()
        for ((id, data) <- datas) {
            val index: Index = new Index.Builder(data)
              .index(indexName)
              .id(id)
              .`type`("_doc")
              .build()
            bulkBuilder.addAction(index)
        }
        //创建批量操作对象
        val bulk: Bulk = bulkBuilder.build()
        val bulkResult: BulkResult = jestClient.execute(bulk)
        println("向ES中插入" + bulkResult.getItems.size() + "条数据")
        jestClient.close()
    }
}



