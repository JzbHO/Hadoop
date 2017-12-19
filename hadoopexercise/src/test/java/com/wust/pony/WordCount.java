package com.wust.pony;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2017/12/19 0019.
 */
public class WordCount {
    /*
    * Map:LongWritable 偏移量，四个参数对应输入和输出的key-value
    * */


    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        LongWritable one=new LongWritable(1);
        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            //接收每一行数据
            String line=value.toString();
            String[]words=line.split(" ");

//            for(String word:words){
//                context.write(new Text(word),one);
//            }
            //手机处理
            context.write(new Text(words[0]),new LongWritable(Long.parseLong(words[1])));

        }
    }
    /*
    * 归并操作，一个集合进行输入
    */
    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
            long sum=0;
            for(LongWritable value:values){
                sum+=value.get();
            }
            //统计结果输出
            context.write(key,new LongWritable(sum));
        }
    }

    public static class MyPartitioner extends Partitioner<Text,LongWritable>{

        public int getPartition(Text key, LongWritable longWritable, int i) {
            if(key.toString().equals("xiaomi")){
                return 0;
            }
            if(key.toString().equals("huawei")){
                return 1;
            }
            if(key.toString().equals("iphone7")){
                return 2;
            }
            return 3;
        }
    }
    public  static void main(String[]args) throws Exception{
        Configuration configuration=new Configuration();
        //创建Job

        //准备清理已存在的输出目录
        Path outputPath=new Path(args[1]);
        FileSystem fileSystem=FileSystem.get(configuration);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
            System.out.println("out put file has been deleted");
        }

        Job job=Job.getInstance(configuration,"wordcount");
        //设置job处理类
        job.setJarByClass(WordCount.class);
        //设置作业处理路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //设置map相关参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //通过job设置combiner处理类
        //job.setCombinerClass(MyReducer.class);

        //设置job的partition
        job.setPartitionerClass(MyPartitioner.class);
        //设置四个reducer
        job.setNumReduceTasks(4);

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);

    }


}
