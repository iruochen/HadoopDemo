package com.neusoft;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class HDFSApp {
    public static final String HDFS_PATH = "hdfs://192.168.10.128:8020";

    public static void main(String[] args) throws Exception {
        System.out.println("开始进行连接");
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
        fileSystem.mkdirs(new Path("/ruochen/test1"));
    }
}
