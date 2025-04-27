package io.github.mmafrar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class PartitionReducer extends Reducer<Text, Text, Text, IntWritable> {

    private static final Logger LOG = Logger.getLogger(PartitionReducer.class);

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int max = -1;

        for (Text val : values) {
            String[] str = val.toString().split("\t", -3);

            if (str.length >= 5) {  // Ensure there are enough fields
                try {
                    int value = Integer.parseInt(str[4]);
                    if (value > max) {
                        max = value;
                    }
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid number format in value: " + str[4]);
                }
            }
        }

        context.write(key, new IntWritable(max));
    }

}
