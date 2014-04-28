package com.aol.hadoop.rainbow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Generates Password InputSplits for Rainbow Table Generation.
 */
public class RainbowInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(final JobContext jc) throws IOException, InterruptedException {
        final List<InputSplit> splitList = new ArrayList<>();
        final Configuration conf = jc.getConfiguration();
        final char[] charset = conf.get("rainbow.charset", new String(defaultCharset())).toCharArray();
        final int minLength = conf.getInt("rainbow.minLength", 1);
        final int maxLength = conf.getInt("rainbow.minLength", 10);
        final int splitCount = jc.getConfiguration().getInt("rainbow.splits", 1000);
        final long permutations = permutations(charset, minLength, maxLength);
        final long splitSize = permutations / splitCount;
        for (long i = 0, s = 0, e = splitSize; i < splitCount; i += 1, s += splitSize, e += splitSize) {
            splitList.add(new RainbowInputSplit(minLength, maxLength, s, e, charset));
        }
        return splitList;
    }

    @Override
    public RecordReader createRecordReader(final InputSplit is, final TaskAttemptContext tac) throws IOException, InterruptedException {
        if (is == null || !RainbowInputSplit.class.isAssignableFrom(is.getClass())) {
            throw new IOException("InputSplit must be a RainbowInputSplit was " + is);
        }
        return new RainbowReader();
    }

    private static long permutations(final char[] charset, final int minLength, final int maxLength) {
        long p = 0;
        for (int i = minLength; i <= maxLength; i++) {
            p += (long) Math.pow(charset.length, i);
        }
        return p;
    }

    private static char[] defaultCharset() {
        final char[] validCharacters = new char[0x7E - 0x20];
        for (int i = 0, c = 0x20; i <= validCharacters.length; i++, c++) {
            validCharacters[i] = (char) c;
        }
        return validCharacters;
    }
}
