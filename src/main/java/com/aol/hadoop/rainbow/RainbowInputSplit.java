package com.aol.hadoop.rainbow;

import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 *
 */
public class RainbowInputSplit extends InputSplit {

    final int minLength, maxLength;
    final long start, end;
    final char[] charset;

    public RainbowInputSplit(
            final int minLength, final int maxLength,
            final long start, final long end,
            final char[] charset) {
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.charset = charset;
        this.start = start;
        this.end = end;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return end - start;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public int getMinLength() {
        return minLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public char[] getCharset() {
        return charset;
    }
}
