package com.aol.hadoop.rainbow;

import java.io.IOException;
import java.math.BigInteger;
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
public class RainbowReader extends RecordReader<BigIntegerWritable, Text> {

    private final StringBuilder buffer;
    private final BigIntegerWritable key;
    private final Text val;
    private RainbowInputSplit split;
    private TaskAttemptContext context;
    private int minLength, maxLength;
    private String charset;
    private BigInteger charsetLength;
    private BigInteger start, end, curr;

    public RainbowReader() {
        this.buffer = new StringBuilder();
        this.key = new BigIntegerWritable();
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
        charsetLength = BigInteger.valueOf(charset.length());
        minLength = split.getMinLength();
        maxLength = split.getMaxLength();
        start = curr = split.getStart();
        end = split.getEnd();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (curr.compareTo(end) < 0) {
            context.getCounter("RainbowReader", "Generated").increment(1);
            generatePassword();
            key.set(curr);
            val.set(buffer.toString());
            curr = curr.add(BigInteger.ONE);
            return true;
        }
        return false;
    }

    @Override
    public BigIntegerWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return val;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        final BigInteger[] res = curr.subtract(start).divideAndRemainder(split.length);
        return res[0].floatValue() + (res[1].floatValue() / split.length.floatValue());
    }

    @Override
    public void close() throws IOException {
    }

    private CharSequence generatePassword() {
        buffer.setLength(0);

        // Determine Password Length & shifted index value
        // based on sequence (1 len),(2 len),(3 len)
        BigInteger q = curr; // Current Index Value
        BigInteger p = BigInteger.ZERO; // Current permutation Value
        BigInteger l = BigInteger.valueOf(minLength); // Password Length
        BigInteger e = BigInteger.valueOf(maxLength); // End Password Length
        while (l.compareTo(e) < 0) {
            BigInteger t = charsetLength.pow(l.intValue());
            p = p.add(t);
            if (q.compareTo(p) < 0) {
                break;
            }
            l = l.add(BigInteger.ONE);
            q = q.subtract(t);
        }

        // Generate Value Sequence from Low to high
        p = BigInteger.ZERO; // Position
        while (true) {
            final BigInteger r = q.mod(charsetLength);
            buffer.append(charset.charAt(r.intValue()));
            q = q.subtract(r).divide(charsetLength);
            p = p.add(BigInteger.ONE);
            if (q.compareTo(BigInteger.ZERO) == 0) {
                break;
            }
        }

        // Pad value to Length with Zero symbol
        while (p.compareTo(l) < 0) {
            buffer.append(charset.charAt(0));
            p = p.add(BigInteger.ONE);
        }

        // Reverse Buffer
        buffer.reverse();
        return buffer;
    }
}
