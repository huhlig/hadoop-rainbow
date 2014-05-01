package com.aol.hadoop.rainbow;

import static com.aol.hadoop.rainbow.RainbowConstants.*;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates Password InputSplits for Rainbow Table Generation.
 */
public class RainbowInputFormat extends InputFormat<BigIntegerWritable, Text> {

    private static final Logger log = LoggerFactory.getLogger(RainbowInputFormat.class);
    
    @Override
    public List<InputSplit> getSplits(final JobContext jc) throws IOException, InterruptedException {
        final List<InputSplit> splitList = new ArrayList<InputSplit>();
        final Configuration conf = jc.getConfiguration();
        final String charset = conf.get("rainbow.charset", defaultCharset);
        final int minLength = conf.getInt("rainbow.minLength", defaultMinLength);
        final int maxLength = conf.getInt("rainbow.maxLength", defaultMaxLength);
        final int splitCount = conf.getInt("rainbow.mappers", defaultMappers);
        final BigInteger permutations = permutations(charset, minLength, maxLength);
        final BigInteger splitSize = permutations.divide(BigInteger.valueOf(splitCount));

        // Create Splits
        BigInteger s = BigInteger.ZERO;
        BigInteger e = splitSize;
        for (long i = 0; i < splitCount; i++) {
            final InputSplit is = new RainbowInputSplit(minLength, maxLength, s, e, charset);
            splitList.add(is);
            s = e;
            e = s.add(splitSize);
            if (e.compareTo(permutations) > 0) {
                e = permutations;
            }
            log.warn(is.toString());
        }
        return splitList;
    }

    /**
     * Create new Rainbow Record Reader.
     *
     * @param is
     * @param tac
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<BigIntegerWritable, Text> createRecordReader(final InputSplit is, final TaskAttemptContext tac) throws IOException, InterruptedException {
        return new RainbowReader();
    }

    public static BigInteger permutations(final String charset, final int minLength, final int maxLength) {
        final BigInteger l = BigInteger.valueOf(charset.length());
        BigInteger p = BigInteger.ZERO;
        for (int i = minLength; i <= maxLength; i++) {
            p = p.add(l.pow(i));
        }
        return p;
    }
}
