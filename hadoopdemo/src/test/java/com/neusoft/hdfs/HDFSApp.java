package com.neusoft.hdfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

public class HDFSApp {
    Configuration configuration = null;
    FileSystem fileSystem = null;
    public static final String HDFS_PATH = "hdfs://192.168.10.128:8020";

    // Java 连接hdfs 需要先建立一个连接
    // 测试方法执行之前要执行的操作
    @Before
    public void setUp() throws Exception {
        System.out.println("开始建立与HDFS的连接");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    // 测试之后要执行的代码
    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("关闭与HDFS的连接");
    }

    // 创建文件夹
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/ruochen/test2"));
    }

    // 创建文件
    @Test
    public void create() throws Exception {
        Path path = new Path("/ruochenchen.txt");
        FSDataOutputStream outputStream = fileSystem.create(path);
        outputStream.write("hello world ha hah dd hello world ha hah dd hello world ha hah dd ".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    // rename文件
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/ruochen/test1/hello.txt");
        Path newPath = new Path("/ruochen/test1/xixi.txt");
        fileSystem.rename(oldPath, newPath);
    }

    // 查看文件
    @Test
    public void cat() throws Exception {
        Path path = new Path("/ruochen/test1/xixi.txt");
        FSDataInputStream inputStream = fileSystem.open(path);
        IOUtils.copyBytes(inputStream, System.out, 1024);
        inputStream.close();
    }

    // 上传文件
    @Test
    public void upload() throws Exception {
        Path localPath = new Path("cifar-10-python.tar.gz");
        Path hdfsPath = new Path("/");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    // 下载文件
    @Test
    public void download() throws Exception {
        Path hdfsPath = new Path("/hadoop-2.6.0-cdh5.7.0.tar.gz");
        Path localPath = new Path("./down/hadoop-2.6.0-cdh5.7.0.tar.gz");
        fileSystem.copyToLocalFile(false, hdfsPath, localPath, true);
    }
}
