package com.liuyanzhao.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 优化了通过反射创建 WordCountMapper
 * 可插拔，即配置 wc.properties，通过读取配置文件中实现类，来实现不同的方法
 *
 * @author 言曌
 * @date 2020/6/26 3:04 下午
 */

public class HDFSWCApp03 {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        Properties properties = PropertiesUtil.getProperties();

        // 1）读取HDFS上的文件    ==> HDFS API
        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));

        // 获取HDFS
        FileSystem fs = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)), new Configuration(), "liuyanzhao");
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);


        // MyMapper mapper = new WordCountMapper();
        // 通过反射创建对象
        Class<?> clazz = Class.forName(properties.getProperty(Constants.MAPPER_CLASS));
        MyMapper mapper = (MyMapper) clazz.newInstance();

        MyContext context = new MyContext();

        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";

            while ((line = reader.readLine()) != null) {
                // 2）业务处理(词频统计)
                mapper.map(line, context);
            }

            reader.close();
            in.close();
        }

        // 3）将处理结果缓存起来
        Map<Object, Object> contextMap = context.getCacheMap();

        // 4）将结果输出到HDFS    ==> HDFS API
        Path outPath = new Path(properties.getProperty(Constants.OUTPUT_PATH));
        FSDataOutputStream out = fs.create(new Path(outPath, new Path(properties.getProperty(Constants.OUTPUT_FILE))));
        // 将第三步缓存的内容写到 out 中
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for (Map.Entry<Object, Object> entry : entries) {
            out.write((entry.getKey().toString() + "\t" + entry.getValue().toString() + "\n").getBytes());
        }
        out.close();

        fs.close();

        System.out.println("finished!");

    }
}
