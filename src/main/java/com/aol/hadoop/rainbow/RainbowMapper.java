package com.aol.hadoop.rainbow;

import static com.aol.hadoop.rainbow.RainbowConstants.defaultAlgorithm;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Base64.Encoder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 */
public class RainbowMapper extends Mapper<BigIntegerWritable, Text, Text, Text> {

    final Encoder b64 = Base64.getEncoder();
    final Text oKey = new Text();
    MessageDigest digest;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        final String algorithm = context.getConfiguration().get("rainbow.algorithm", defaultAlgorithm);
        try {
            digest = MessageDigest.getInstance(algorithm);
        } catch (final NoSuchAlgorithmException nsae) {
            throw new IOException(nsae);
        }
    }

    @Override
    protected void map(final BigIntegerWritable iKey, final Text iVal, final Context context) throws IOException, InterruptedException {
        oKey.set(b64.encode(digest.digest(iVal.getBytes())));
        context.write(oKey, iVal);
    }
}
