package com.liuyanzhao.bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义上下文，其实就是缓存
 *
 * @author 言曌
 * @date 2020/6/26 3:47 下午
 */

public class MyContext {

    private Map<Object, Object> cacheMap = new HashMap<>();

    public Map<Object, Object> getCacheMap() {
        return cacheMap;
    }

    /**
     * 写数据到缓存
     *
     * @param key   单词
     * @param value 词频
     */
    public void write(Object key, Object value) {
        cacheMap.put(key, value);
    }


    /**
     * 从缓存读数据
     *
     * @param key 但粗
     * @return 词频
     */
    public Object get(Object key) {
        return cacheMap.get(key);
    }


}
