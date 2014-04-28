package com.aol.hadoop.rainbow;

import java.io.IOException;
import java.security.MessageDigest;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 */
public class RainbowMapper extends Mapper<LongWritable, Text, Text, Text> {

    final Text oKey = new Text();
    MessageDigest digest;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        final String algorithm = context.getConfiguration().get("rainbow.algorithm", "MD5");
        digest = DigestUtils.getDigest(algorithm);
    }

    @Override
    protected void map(final LongWritable iKey, final Text iVal, final Context context) throws IOException, InterruptedException {
        oKey.set(digest.digest(iVal.getBytes()));
        context.write(oKey, iVal);
    }
}
