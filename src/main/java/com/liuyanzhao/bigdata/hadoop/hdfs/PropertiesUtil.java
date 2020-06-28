package com.liuyanzhao.bigdata.hadoop.hdfs;

import java.io.IOException;
import java.util.Properties;

/**
 * @author 言曌
 * @date 2020/6/26 4:27 下午
 */

public class PropertiesUtil {

    private static Properties properties = new Properties();

    static {
        try {
            properties.load(PropertiesUtil.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties() {
        return properties;
    }

    public static void main(String[] args) {
        System.out.println(properties);
    }
}
