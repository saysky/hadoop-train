package com.liuyanzhao.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 使用Java API操作HDFS文件系统
 * <p>
 * 关键点：
 * 1）创建Configuration
 * 2）获取FileSystem
 * 3）操作
 *
 * @author 言曌
 * @date 2020/6/25 10:08 下午
 */

public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://localhost:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void before() throws URISyntaxException, IOException, InterruptedException {
        System.out.println("------before-------");
        configuration = new Configuration();
        // 设置创建文件时副本数为1
        configuration.set("dfs.replication", "1");

        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "liuyanzhao");
    }

    @After
    public void after() {
        System.out.println("------after-------");
        configuration = null;
        fileSystem = null;
    }


    /**
     * 创建 HDFS 文件夹
     *
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/hdfsapi/test2");
        boolean result = fileSystem.mkdirs(path);
        System.out.println(result);
    }

    /**
     * 查看 HDFS 文件内容
     *
     * @throws IOException
     */
    @Test
    public void text() throws IOException {
        Path path = new Path("/LICENSE.txt");
        FSDataInputStream in = fileSystem.open(path);
        IOUtils.copyBytes(in, System.out, 1024);
    }

    /**
     * 创建文件
     *
     * @throws IOException
     */
    @Test
    public void create() throws IOException {
        Path path = new Path("/hdfsapi/test/a.txt");
        FSDataOutputStream out = fileSystem.create(path);
        out.writeUTF("hello hdfs");
        out.flush();
        out.close();
    }

    /**
     * 重命名
     *
     * @throws IOException
     */
    @Test
    public void rename() throws IOException {
        Path srcPath = new Path("/hdfsapi/test/a.txt");
        Path destPath = new Path("/hdfsapi/test/b.txt");
        boolean result = fileSystem.rename(srcPath, destPath);
        System.out.println(result);
    }

    /**
     * 上传本地文件到 HDFS
     *
     * @throws IOException
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        Path srcPath = new Path("/Users/liuyanzhao/Desktop/category.sql");
        Path descPath = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(srcPath, descPath);
    }


    /**
     * 上传大文件到 HDFS，带进度条
     *
     * @throws IOException
     */
    @Test
    public void copyFromLocalBigFile() throws IOException {

        InputStream in = new BufferedInputStream(new FileInputStream(new File("/Users/liuyanzhao/Desktop/ForestShop.war")));

        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/ForestShop.war"), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096);

    }

    /**
     * 从 HDFS 下载文件到本地
     */
    @Test
    public void copyToLocalFile() throws IOException {
        Path srcPath = new Path("/hdfsapi/test/b.txt");
        Path descPath = new Path("/Users/liuyanzhao/Desktop");
        fileSystem.copyToLocalFile(srcPath, descPath);
    }

    /**
     * 列出文件夹下的文件和文件夹列表
     *
     * @throws IOException
     */
    @Test
    public void listFiles() throws IOException {
        FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for (FileStatus file : statuses) {
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + length + "\t" + path);
        }
    }

    /**
     * 递归列出文件夹下的文件列表
     *
     * @throws IOException
     */
    @Test
    public void listFilesRecursive() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfsapi"), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + length + "\t" + path);
        }
    }


    /**
     * 获取文件分块信息
     *
     * @throws IOException
     */
    @Test
    public void getFileBlockLocations() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/ForestShop.war"));
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation block : blockLocations) {
            String[] names = block.getNames();
            for (String name : names) {
                System.out.println(name + " : " + block.getOffset() + " : " + block.getLength());
            }
        }
    }

    /**
     * 删除HDFS文件
     */
    @Test
    public void delete() throws IOException {
        Path path = new Path("/hdfsapi/test/ForestShop.war");
        boolean result = fileSystem.delete(path, true);
        System.out.println(result);
    }

//    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
//
//        // localhost:8020
//        Configuration configuration = new Configuration();
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:8020"), configuration, "liuyanzhao");
//
//        Path path = new Path("/hdfsapi/test2");
//        boolean result = fileSystem.mkdirs(path);
//        System.out.println(result);
//    }


}
