package com.liuyanzhao.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author 言曌
 * @date 2020/6/27 3:24 下午
 */

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * map输出到reduce端，是按照相同的key分发到一个reduce上去执行
     *
     * reduce1: (hello,1) (hello,1) (hello,1) ==> (hello,[1,1,1])
     * reduce2: (world,1) (world,1) (world,1) ==> (world,[1,1,1])
     *
     *
     * Reducer和Mapper中实际使用了什么设计模式：模板方法
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;

        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntWritable value = iterator.next();
            count += value.get();
        }

        context.write(key, new IntWritable(count));

    }
}
