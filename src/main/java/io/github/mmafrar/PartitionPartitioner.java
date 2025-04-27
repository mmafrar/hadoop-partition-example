package io.github.mmafrar;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        if (numReduceTasks == 0) {
            return 0;
        }

        String[] str = value.toString().split("\t");
        int age = Integer.parseInt(str[2]);

        if (age <= 20) {
            return 0;
        } else if (age <= 30) {
            return 1 % numReduceTasks;
        } else {
            return 2 % numReduceTasks;
        }
    }
}
