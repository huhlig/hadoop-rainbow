package com.aol.hadoop.rainbow;

import static com.aol.hadoop.rainbow.RainbowConstants.*;
import java.math.BigInteger;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author hansuhlig9
 */
public class RainbowInputFormatTest {
    /**
     * Test of getSplits method, of class RainbowInputFormat.
     * @throws java.lang.Exception
     */
    @Test
    public void testGetSplits() throws Exception {
        System.out.println("getSplits");
        
        final JobContext jc = new JobContextImpl(new Configuration(), new JobID());
        final RainbowInputFormat instance = new RainbowInputFormat();
        final List<InputSplit> result = instance.getSplits(jc);
        for(final InputSplit split:result) {
            System.out.println(split);
        }
        System.out.println(RainbowInputFormat.permutations(defaultCharset, defaultMinLength, defaultMaxLength));     
    }
}
