package com.liuyanzhao.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN:   Map任务读取数据key类型，offset，是每行数据起始位置的偏移量，Long
 * VALUEIN: Map任务读取数据value类型，其实是一行行的字符串，String
 * <p>
 * KEYOUT:  map方法自定义实现输出的key的类型， String
 * VALUEOUT: map方法自定义实现输出的value的类型，Integer
 * <p>
 * <p>
 * Long、String、String、Integer是Java的数据类型
 * 词频统计：相同单词的次数  (word, 1)
 *
 * @author 言曌
 * @date 2020/6/26 11:35 下午
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 把value指定的行数据按照指定的分隔符拆开
        String[] words = value.toString().split(" ");
        for (String word : words) {
            // (hello,1)   (world,1)
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}
