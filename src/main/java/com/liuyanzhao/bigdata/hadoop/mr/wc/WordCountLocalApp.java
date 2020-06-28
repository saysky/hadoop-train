package com.liuyanzhao.bigdata.hadoop.mr.wc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * 使用MapReduce统计本地文件的词频
 * 统计结果输出到本地
 *
 * 本地模式
 * @author 言曌
 * @date 2020/6/27 3:42 下午
 */

public class WordCountLocalApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 创建一个Job
        Job job = Job.getInstance();

        // 设置Job对应的参数: 主类
        job.setJarByClass(WordCountLocalApp.class);

        // 设置Job对应的参数: 设置自定义的Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Job对应的参数: 设置Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Job对应的参数: 设置Reduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置Job对应的参数: 作业输入输出路径
        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        boolean result = job.waitForCompletion(true);
        System.out.println(result);
        System.exit(result ? 0 : -1);
    }
}
