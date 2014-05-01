package com.aol.hadoop.rainbow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 *
 */
public class RainbowInputSplit extends InputSplit implements Writable {

    int minLength, maxLength;
    BigInteger start, end, length;
    String charset;

    public RainbowInputSplit() {
    }

    public RainbowInputSplit(
            final int minLength, final int maxLength,
            final BigInteger start, final BigInteger end,
            final String charset) {
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.length = end.subtract(start);
        this.charset = charset;
        this.start = start;
        this.end = end;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return length.longValue();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    public BigInteger getStart() {
        return start;
    }

    public BigInteger getEnd() {
        return end;
    }

    public int getMinLength() {
        return minLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public String getCharset() {
        return charset;
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        final byte[] startBuf = new byte[di.readInt()];
        di.readFully(startBuf);
        start = new BigInteger(startBuf);

        final byte[] endBuf = new byte[di.readInt()];
        di.readFully(endBuf);
        end = new BigInteger(endBuf);

        length = end.subtract(start);

        minLength = di.readInt();
        maxLength = di.readInt();
        charset = di.readUTF();
    }

    @Override
    public void write(DataOutput d) throws IOException {
        final byte[] startBuf = start.toByteArray();
        final byte[] endBuf = end.toByteArray();
        d.writeInt(startBuf.length);
        d.write(startBuf);
        d.writeInt(endBuf.length);
        d.write(endBuf);
        d.writeInt(minLength);
        d.writeInt(maxLength);
        d.writeUTF(charset);
    }

    @Override
    public String toString() {
        return String.format("{ \"range\":[%s, %s], \"length\": %s }",
                start, end, length);
    }

}
