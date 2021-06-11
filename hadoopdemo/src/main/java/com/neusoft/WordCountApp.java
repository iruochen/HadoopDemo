package com.neusoft;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 词频统计
 */
public class WordCountApp {
    /**
     * map 阶段
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 分
            String line = value.toString();
            // 拆分
            String[] s = line.split(" ");
            for (String word : s) {
                // 输出
                context.write(new Text(word), one);
            }
        }
    }

    /**
     * reduce 阶段
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            // 合并统计
            for (LongWritable value : values) {
                // 求和
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "wordcount");
        job.setJarByClass(WordCountApp.class);

        // 设置 map 相关参数
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置 reduce 相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(MyReducer.class);
        job.setOutputValueClass(LongWritable.class);

        Path outPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outPath)) {
            // 删除文件
            fileSystem.delete(outPath, true);
            System.out.println("输出路径已存在, 已被删除");
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 控制台输出详细信息
        // 输出：1  不输出：0
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
