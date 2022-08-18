package com.serendipity.engine.demos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Hbase_demo1 {
    HBaseAdmin hBaseAdmin = null;
    Connection connection = null;
    Configuration configuration = null;
    HTable hTable = null;

    Hbase_demo1() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "1.15.149.196");
        configuration.set("hbase.zookeeper.property.clientPort", "12181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            hBaseAdmin = (HBaseAdmin) connection.getAdmin();
            //HTable hTable=(HTable) connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Hbase_demo1 instance = null;

    public static synchronized Hbase_demo1 getInstance() {
        if (instance == null) {
            instance = new Hbase_demo1();
        }
        return instance;
    }

    public boolean isTableExist(String tableName) throws IOException {
        try {
            HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hBaseAdmin.tableExists(TableName.valueOf(tableName));
    }

    // 创建表
    public void createTable(String tableName, String... columnFamily) throws IOException {
        // 判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
            // System.exit(0);
        } else {
            // 创建表属性对象，表名需要转字节
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            // 创建多个列族
            for (String f : columnFamily) {
                tableDescriptor.addFamily(new HColumnDescriptor(f));
            }
            // 创建表
            hBaseAdmin.createTable(tableDescriptor);
            System.out.println("创建" + tableName + "表成功");
        }
    }

    // 删除表
    public void dropTable(String tableName) throws IOException {

        if (isTableExist(tableName)) {
            hBaseAdmin.disableTable(TableName.valueOf(tableName));
            hBaseAdmin.deleteTable(TableName.valueOf(tableName));
            System.out.println("表" + tableName + "删除成功");
        } else {
            System.out.println("表" + tableName + "不存在!");
        }
    }

    // 向表中插入数据
    public void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {

        try {
            HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            // 向表中插入数据
            // 像Put 对象中封装数据
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
            hTable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("插入数据成功");
    }

    // 删除多行数据
    public void deleteMultiRow(String tableName, String... rows) throws IOException {
        //HTable hTable = new HTable(configuration, tableName);

        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        try {
            HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            hTable.delete(deleteList);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 获取所有数据
    public void getAllRows(String tableName) throws IOException {
        //HTable hTable = new HTable(configuration, tableName);
        // 得到用于扫描region的对象
        Scan scan = new Scan();
        // 使用HTable 得到 resultcanner实现类的对象
        try {
            HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            ResultScanner resultScanner = hTable.getScanner(scan);

            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    // 得到rowkey
                    System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                    // 得到列族
                    System.out.println("列族:" + Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));

                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    // 获取某一行数据
    public void getRow(String tableName, String rowKey) throws IOException {
        //HTable table = new HTable(configuration, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        // get.setMaxVersions(); 显示所有版本
        // get.setTimeStamp(); 显示指定时间的版本


        try {
            HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            Result result = hTable.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("行键:" + Bytes.toString(result.getRow()));
                System.out.println("列族:" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("时间戳:" + cell.getTimestamp());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    // 获取某一行指定 "列族:列" 的数据
    public void getRowQualifier(String tableName, String rowKey, String family, String qualifier) throws IOException {
        //HTable table = new HTable(configuration, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.setMaxVersions();
        //get.setTimestamp();

        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

        try {
            HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            Result result = hTable.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("行键:" + Bytes.toString(result.getRow()));
                System.out.println("列族:" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
                //System.out.println("时间戳:" + cell.getTimestamp());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
