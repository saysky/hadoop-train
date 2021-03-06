package com.liuyanzhao.bigdata.hadoop.hdfs;

/**
 * 自定义wc实现类
 *
 * @author 言曌
 * @date 2020/6/26 3:56 下午
 */

public class WordCountMapper implements MyMapper {


    @Override
    public void map(String line, MyContext context) {
        String[] words = line.split(" ");
        for (String word : words) {
            Object value = context.get(word);
            if (value == null) {
                context.write(word, 1);
            } else {
                int v = Integer.parseInt(value.toString());
                context.write(word, v + 1);
            }
        }
    }
}
