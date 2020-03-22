package com.c503.hufeihu.dao

import com.c503.hufeihu.domain.CategarySearchClickCount
import com.c503.hufeihu.utils.HBaseUtils
import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.hadoop.hbase.client.{Connection, Get, Increment, Result, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer


/**
  * Created by zhang on 2017/11/22.
  */
object CategarySearchClickCountDAO {

     val tableName = "categary_search_cout"
     val cf = "info"
     val qualifer = "click_count"

    /**
      * 保存数据
      * @param list
      */
    def save(list:ListBuffer[CategarySearchClickCount]): Unit ={
        for(els <- list){
          val table: Table = HBaseUtils.getTable(tableName)
          val increment = new Increment(Bytes.toBytes(els.day_search_categary))
          increment.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifer), els.clickCout)
          table.increment(increment)
         // table.incrementColumnValue(Bytes.toBytes(els.day_search_categary),Bytes.toBytes(cf),Bytes.toBytes(qualifer),els.clickCout);
         // HBaseUtils.putData(tableName,els.day_search_categary,cf,qualifer,els.clickCout.toString)
        }

    }


    def count(day_categary:String) : Long={
      var resultCount:Long=0;
      val connection: Connection = HBaseUtils.getConnection
      //获取表名称
      val table: Table = connection.getTable(TableName.valueOf(tableName))

      //创建get对象
      val get = new Get(Bytes.toBytes(day_categary))
      //get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifer))

      //获取结果
      val result: Result = table.get(get)

      for (cell <- result.rawCells) {
        resultCount=Bytes.toLong(CellUtil.cloneValue(cell))
      }
      resultCount
    }

    def main(args: Array[String]): Unit = {
  //     val list = new ListBuffer[CategarySearchClickCount]
//        list.append(CategarySearchClickCount("20171122_1_1",300))
//        list.append(CategarySearchClickCount("20171122_2_1", 300))
//        list.append(CategarySearchClickCount("20171122_1_2", 1600))
//        save(list)
//
//      print(count("20171122_2_1"))
      println(count("20200215_cn.bing.com_0"))
      println(count("20200215_search.yahoo.com_0"))
      println(count("20200216_www.baidu.com_0"))
      println(count("20200215_www.sogou.com_0"))

    }
}
