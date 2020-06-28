package com.liuyanzhao.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;

/**
 * 使用 HDFS API 完成 wordcount 统计
 * <p>
 * 需求：统计HDFS上的文件的单词频率，输出结果到HDFS
 * <p>
 * 功能拆解：
 * 1）读取HDFS上的文件    ==> HDFS API
 * 2）业务处理(词频统计)：对文件的每一行进行处理，安装分隔符分隔   ==> Mapper
 * 3）将处理结果缓存起来   ==> Context
 * 4）将结果输出到HDFS    ==> HDFS API
 *
 * 最初版本
 * @author 言曌
 * @date 2020/6/26 3:04 下午
 */

public class HDFSWCApp01 {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        // 1）读取HDFS上的文件    ==> HDFS API
        Path input = new Path("/hdfsapi/a.txt");

        // 获取HDFS
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:8020"), new Configuration(), "liuyanzhao");
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);

        MyMapper mapper = new WordCountMapper();
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
        Path outPath = new Path("/hdfsapi/output/");
        FSDataOutputStream out = fs.create(new Path(outPath, new Path("wc.out")));
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
