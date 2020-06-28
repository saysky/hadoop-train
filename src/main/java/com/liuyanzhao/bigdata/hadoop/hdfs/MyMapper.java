package com.liuyanzhao.bigdata.hadoop.hdfs;

/**
 * 自定义Mapper
 *
 * @author 言曌
 * @date 2020/6/26 3:54 下午
 */

public interface MyMapper {

    /**
     * @param line    读取到每一行数据
     * @param context 上下文/缓存
     */
    void map(String line, MyContext context);
}
