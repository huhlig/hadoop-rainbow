package com.aol.hadoop.rainbow;

import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * RainbowReader generates passwords and hashes based on the specified
 * algorithm.
 * <pre>
 * rainbow.charset = [valid characters]
 * rainbow.minLength = 1
 * rainbow.maxLength = 9
 * </pre>
 */
public class RainbowReader extends RecordReader<LongWritable, Text> {

    private final Charset UTF8 = Charset.forName("UTF-8");
    private final StringBuilder buffer;
    private final LongWritable key;
    private final Text val;
    private RainbowInputSplit split;
    private TaskAttemptContext context;
    private int minLength, maxLength;
    private char[] charset;
    private long start, end, curr;

    public RainbowReader() {
        this.buffer = new StringBuilder();
        this.key = new LongWritable();
        this.val = new Text();
    }

    @Override
    public void initialize(final InputSplit is, final TaskAttemptContext tac) throws IOException, InterruptedException {
        if (is == null || !RainbowInputSplit.class.isAssignableFrom(is.getClass())) {
            throw new IOException("InputSplit for RainbowReader MUST be a RainbowInputSplit");
        }
        split = (RainbowInputSplit) is;
        context = tac;
        charset = split.getCharset();
        minLength = split.getMinLength();
        maxLength = split.getMaxLength();
        start = curr = split.getStart();
        end = split.getEnd();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (++curr < end) {
            context.getCounter("RainbowReader", "Generated").increment(1);
            generatePassword();
            key.set(curr);
            val.set(buffer.toString());
            return true;
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return val;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (curr - start) / (end - start);
    }

    @Override
    public void close() throws IOException {
    }

    private CharSequence generatePassword() {
        buffer.setLength(0);
        
        // Determine Password Length based on sequence (1 len),(2 len),(3 len)
        long q = curr, p = 0, len = 1, pos = 0;
        for (len = minLength; len <= maxLength; len++) {
            final long t = (long) Math.pow(charset.length, len);
            p += t;
            if (q < p) {
                break;
            }
            len += 1;
            q -= t;
        }
        
        // Generate Value Sequence from Low to high
        while (true) {
            final int r = (int) (q % charset.length);
            buffer.append(charset[r]);
            q = (q - r) / charset.length;
            pos += 1;
            if (q == 0) {
                break;
            }
        }
        
        // Pad value to Length with Zero symbol
        for (; pos < len; pos++) {
            buffer.append(charset[0]);
        }
        
        // Reverse Buffer
        buffer.reverse();        
        return buffer;
    }
}
