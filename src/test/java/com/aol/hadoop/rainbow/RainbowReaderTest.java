package com.aol.hadoop.rainbow;

import java.math.BigInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

/**
 *
 */
public class RainbowReaderTest {

    /**
     * Test of initialize method, of class RainbowReader.
     */
    @Test
    public void testReader() throws Exception {
        final Configuration conf = new Configuration();

        final TaskAttemptContext tac = new TaskAttemptContextImpl(conf, new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 1), 1));
        final InputSplit is = new RainbowInputSplit(0, 2, BigInteger.valueOf(100), BigInteger.valueOf(400), "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
        final RainbowReader instance = new RainbowReader();
        instance.initialize(is, tac);
        while (instance.nextKeyValue()) {
            final BigIntegerWritable iKey = instance.getCurrentKey();
            final Text iVal = instance.getCurrentValue();
            System.out.printf("%.3f - %s => %s\n", instance.getProgress(), iKey, iVal);
        }
        instance.close();
    }
}
