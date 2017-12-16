package com.wust.pony;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * Created by Administrator on 2017/12/16 0016.
 */
public class HDFSApp {
    public static final String HDFS_PATH="hdfs://119.29.20.230:9000";

    FileSystem fileSystem=null;
    Configuration configuration=null;

    @Test
    public void mkdir()throws Exception{

        fileSystem=FileSystem.get(new URI(HDFS_PATH),configuration,"localhost");
        System.out.println(fileSystem.toString());
        fileSystem.mkdirs(new Path("/hdfsapi"));
    }






    @Before
    public void setUp()throws Exception{
        configuration=new Configuration();

        System.out.println("setUp");
    }

    @After
    public void tearDown(){
        configuration=null;
        fileSystem=null;
        System.out.println("tearDown");
    }





}
