package com.c503.hufeihu.dao


import com.c503.hufeihu.domain.CategaryClickCount
import com.c503.hufeihu.utils.HBaseUtils
import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.hadoop.hbase.client.{Connection, Get, Result, Table}
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes


/**
  * Created by zhang on 2017/11/22.
  */


object CategaryClickCountDAO {

     val tableName = "category_clickcount"
     val cf = "info"
     val qualifer = "click_count"

    /**
      * 保存数据
      * @param list
      */
    def save(list:ListBuffer[CategaryClickCount]): Unit ={
        for(els <- list){
          val table: Table = HBaseUtils.getTable(tableName)
          val increment = new Increment(Bytes.toBytes(els.categaryID))
          increment.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifer), els.clickCout)
          table.increment(increment)

          //table.incrementColumnValue(Bytes.toBytes(els.categaryID),Bytes.toBytes(cf),Bytes.toBytes(qualifer),els.clickCout)
          //HBaseUtils.putData(tableName,els.categaryID,cf,qualifer,els.clickCout.toString)
        }
    }

    def count(day_categary:String) : Long={
      var resultCount:Long=0;
      val connection: Connection = HBaseUtils.getConnection
      //获取表名称
      val table: Table = connection.getTable(TableName.valueOf(tableName))

      //创建get对象
      val get = new Get(Bytes.toBytes(day_categary))
      get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifer))

      //获取结果
      val result: Result = table.get(get)

      for (cell <- result.rawCells) {
        resultCount=Bytes.toLong(CellUtil.cloneValue(cell))
      }
      resultCount
    }

    def main(args: Array[String]): Unit = {
//       val list = new ListBuffer[CategaryClickCount]
//        list.append(CategaryClickCount("20171122_1",300))
//        list.append(CategaryClickCount("20171122_9", 60))
//        list.append(CategaryClickCount("20171122_10", 160))
//        save(list)

      print(count("202002150"))
    }

}
