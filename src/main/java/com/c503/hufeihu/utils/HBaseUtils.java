package com.c503.hufeihu.utils;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;


public class HBaseUtils {
    public static Connection connection=null;
    public static Admin admin=null;

    /**
     * 静态代码块，用于初始化Hbase连接
     */
    static{
        try {
            //获取配置信息
            Configuration  configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "123.206.202.158");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");

            //创建连接
            connection= ConnectionFactory.createConnection(configuration);

            //创建Admin
            admin=connection.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 获取HBase连接
     * @return
     */
    public static Connection getConnection(){
        return connection;
    }

    /**
     * 获取Admin实例
     * @return
     */
    public static Admin getAdmin(){
        return admin;
    }

    public static Table getTable(String tableName) throws IOException {
        Table table=connection.getTable(TableName.valueOf(tableName));
        return table;
    }


    /**
     * 关闭资源
     */
    public static void close(){
        if (admin!=null){
            try {
                admin.close();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }

        if (connection!=null){
            try {
                connection.close();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    /**
     * 判断表名称是否存在
     * @param tableName 表名称
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 创建表
     * @param tableName 表名称
     * @param cfs 可变 列簇
     * @throws IOException
     */
    public static void createTable(String tableName,String... cfs) throws IOException {
        //判断是否存在列簇信息
        if (cfs.length<=0){
            System.out.println("请设置列簇信息");
            return;
        }
        if (isTableExist(tableName)){
            System.out.println(tableName+"表已经存在！");
            return;
        }
        //创建表描述器
        HTableDescriptor hTableDescriptor=new HTableDescriptor(TableName.valueOf(tableName));
        //循环添加列簇
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor=new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);

    }

    /**
     * 删除表
     * @param tableName 表名称
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        //判断表是否存在
        if (!isTableExist(tableName)){
            System.out.println(tableName+"表不存在");
            return;
        }
        //使表下线
        admin.disableTable(TableName.valueOf(tableName));
        //删除表
        admin.deleteTable(TableName.valueOf(tableName));

    }

    /**
     * 创建命名空间
     * @param ns 命名空间
     */
    public static void createNameSpace(String ns){
        //创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor=NamespaceDescriptor.create(ns).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        }
        catch (NamespaceExistException e){
            System.out.println("命名空间"+ns+"已经存在");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 向表中添加数据
     * @param tableName 表名称
     * @param rowKey 行键
     * @param columnFamily 列簇
     * @param column 列名
     * @param value 值
     * @throws IOException
     */
    public static void putData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        //获取表对象
        Table table=connection.getTable(TableName.valueOf(tableName));

        //创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 查询数据
     * @param tableName 表名称
     * @param rowKey 行键
     * @param columnFamily 列簇名称
     * @param column 列名
     * @throws IOException
     */
    public static void getData(String tableName, String rowKey,String columnFamily, String column) throws IOException {
        //获取表名称
        Table table=connection.getTable(TableName.valueOf(tableName));
        //创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column));
        get.getMaxVersions();
        //获取结果
        Result result = table.get(get);

        for (Cell cell : result.rawCells()) {
            System.out.println("CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                    +",CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                    +",Value:"+Bytes.toString(CellUtil.cloneValue(cell)));

            System.out.println(" 行 键 :" + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println(" 列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(" 列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(" 值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

    }

    /**
     * 查询所有数据
     * @param tableName 表名
     * @throws IOException
     */
    public static void scanData(String tableName) throws IOException {
        //获取表名称
        Table table=connection.getTable(TableName.valueOf(tableName));
        Scan scan=new Scan(Bytes.toBytes("1001"));
        ResultScanner resultScanner=table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                        +",CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                        +",Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * 删除数据
     * @param tableName 表名称
     * @param rowKey  行键
     * @param columnFamily 列簇
     * @param column 列名
     * @throws IOException
     */
    public static void deleteData(String tableName,String rowKey,String columnFamily, String column) throws IOException {
        //获取表名称
        Table table=connection.getTable(TableName.valueOf(tableName));

        //构建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //设置删除的列
       // delete.addColumn();
       // delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(column));
        delete.addFamily(Bytes.toBytes(columnFamily));
        table.delete(delete);
        table.close();
    }

    //


    public static void main(String[] args) throws IOException {
        //测试表是否存在
//        boolean hufeihu = isTableExist("tabletest");
//        System.out.println(hufeihu);
        createTable("test","datat");
  //      dropTable("test");
//        createNameSpace("namespacetest");
//        close();

//        putData("stu1","1001","info1","name","hufeihu");
//        putData("stu1","1001","info2","age","123");
//
//        putData("stu1","1002","info1","name","hufeihu");
//        putData("stu1","1002","info2","age","123");
        scanData("stu1");
 //       getData("stu1","1001","info2","age");

       //deleteData("stu1","1002","info1","name");
    }
}
