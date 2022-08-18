package com.serendipity.engine.demos;

import java.io.IOException;

public class Hbase_test1 {
    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop");
        Hbase_demo1 hd1 = new Hbase_demo1();

//        // 删除表
//        hd1.dropTable("java_test1");

        // 创建表
        hd1.createTable("java_test1", "info1", "info2");

//        // 像表中插入数据
//        hd1.addRowData("java_test1","1001","info1","name","詹姆斯");
//        hd1.addRowData("java_test1","1001","info1","sex","男");
//        hd1.addRowData("java_test1","1001","info1","age","33");
//        hd1.addRowData("java_test1","1001","info2","job","BasketBall");
//        hd1.addRowData("java_test1","1002","info1","name","杜兰特");
//        hd1.addRowData("java_test1","1002","info1","sex","男");
//        hd1.addRowData("java_test1","1002","info1","age","32");
//        hd1.addRowData("java_test1","1002","info2","job","BasketBall");
//
//        // 获取所有数据
//        hd1.getAllRows("java_test1");
//
//        // 获取某一行数据
//        hd1.getRow("java_test1","1001");
//
//        // 获取某一行指定 “列族:列” 的数据
//        hd1.getRowQualifier("java_test1","1001","info1","name");
//
//        // 删除多行数据
//        hd1.deleteMultiRow("java_test1","1001","1002");
//
//        // 删除表
//        hd1.dropTable("java_test1");
//
//        // 关闭连接
//        hd1.closeConn();
    }
}
