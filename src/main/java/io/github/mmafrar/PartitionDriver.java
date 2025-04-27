package io.github.mmafrar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PartitionDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Create job and set its configurations
        Job job = Job.getInstance(conf, "Partition");
        job.setJarByClass(PartitionDriver.class);

        // Input and Output paths
        FileInputFormat.setInputPaths(job, new Path("/Users/mmafrar/Partition/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/mmafrar/Partition/output"));

        // Set Mapper, Reducer, Partitioner, and other configurations
        job.setMapperClass(PartitionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(PartitionPartitioner.class);
        job.setReducerClass(PartitionReducer.class);

        job.setNumReduceTasks(3);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set Output Key and Value Classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Delete output path if it exists (to avoid errors)
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path("/Users/mmafrar/Partition/output");
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
