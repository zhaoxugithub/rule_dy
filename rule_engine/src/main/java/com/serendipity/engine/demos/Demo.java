package com.serendipity.engine.demos;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomUtils;

public class Demo {

    public static void main(String[] args) {
        for (int i = 0; i < 10000; i++) {
            String account = StringUtils.leftPad(RandomUtils.nextInt(0,100000) + "", 6, "0");
            int j = RandomUtils.nextInt();
            System.out.println(j);
            System.out.println("----" + account);
        }
    }
}
