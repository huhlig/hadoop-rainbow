package com.aol.hadoop.rainbow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 */
public class BigIntegerWritable implements WritableComparable<BigIntegerWritable> {

    private BigInteger val;

    public BigIntegerWritable() {
    }

    public BigIntegerWritable(final long val) {
        this.val = BigInteger.valueOf(val);
    }

    public BigIntegerWritable(final BigInteger val) {
        this.val = val;
    }

    public BigIntegerWritable(final byte[] val) {
        this.val = new BigInteger(val);
    }

    public BigIntegerWritable(final byte[] val, final int off, final int len) {
        final byte[] buf = new byte[len];
        System.arraycopy(val, off, val, 0, len);
        this.val = new BigInteger(buf);
    }

    public BigInteger get() {
        return val;
    }

    public void set(final long val) {
        this.val = BigInteger.valueOf(val);
    }

    public void set(final byte[] buf) {
        this.val = new BigInteger(buf);
    }

    public void set(final BigInteger val) {
        this.val = val;
    }

    @Override
    public void write(final DataOutput d) throws IOException {
        final byte[] buf = val.toByteArray();
        d.writeInt(buf.length);
        d.write(buf);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        final byte[] buf = new byte[di.readInt()];
        di.readFully(buf);
        val = new BigInteger(buf);
    }

    @Override
    public int compareTo(final BigIntegerWritable o) {
        return val.compareTo(o.val);
    }

    @Override
    public int hashCode() {
        return val.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BigIntegerWritable)) {
            return false;
        }
        return compareTo((BigIntegerWritable) o) == 0;
    }

    /**
     * TODO: Write better comparator
     */
    public static class Comparator extends WritableComparator {

        private BigIntegerWritable thisValue;
        private BigIntegerWritable thatValue;

        public Comparator() {
            super(BigIntegerWritable.class);
        }

        @Override
        public int compare(
                byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            final byte[] l = new byte[l1 - s1 - 4];
            final byte[] r = new byte[l2 - s2 - 4];
            System.arraycopy(b1, s1 + 4, l, 0, l1 - 4);
            System.arraycopy(b2, s2 + 4, r, 0, l2 - 4);
            thisValue = new BigIntegerWritable(l);
            thatValue = new BigIntegerWritable(r);
            return thisValue.compareTo(thatValue);
        }
    }

    @Override
    public String toString() {
        return val.toString();
    }

    // register this comparator
    static {
        WritableComparator.define(BigIntegerWritable.class, new Comparator());
    }
}
