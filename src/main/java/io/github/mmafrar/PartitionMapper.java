package io.github.mmafrar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class PartitionMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger LOG = Logger.getLogger(PartitionMapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context) {
        try {
            String[] str = value.toString().split("\t", -3);

            if (str.length >= 4) {  // Ensure there are enough fields
                String gender = str[3];
                // key = gender, value = whole string
                context.write(new Text(gender), new Text(value));
            } else {
                LOG.warn("Skipping record due to insufficient fields: " + value);
            }
        } catch (Exception e) {
            LOG.error("Error in Map: " + e.getMessage(), e);
        }
    }

}
