package com.aol.hadoop.rainbow;

import static com.aol.hadoop.rainbow.RainbowConstants.defaultAlgorithm;
import static com.aol.hadoop.rainbow.RainbowConstants.defaultHashDepth;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 */
public class RainbowMapper extends Mapper<BigIntegerWritable, Text, Text, Text> {

    final Base64 b64 = new Base64();
    final Text oKey = new Text();
    MessageDigest digest;
    int hashDepth;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        final String algorithm = context.getConfiguration().get("rainbow.algorithm", defaultAlgorithm);
        hashDepth = context.getConfiguration().getInt("rainbow.hashDepth", defaultHashDepth);
        try {
            digest = MessageDigest.getInstance(algorithm);
        } catch (final NoSuchAlgorithmException nsae) {
            throw new IOException(nsae);
        }
    }

    @Override
    protected void map(final BigIntegerWritable iKey, final Text iVal, final Context context) throws IOException, InterruptedException {
        byte[] buffer = iVal.getBytes();
        for(int c =0; c<hashDepth;c++) {
            buffer = digest.digest(buffer);
        }
        oKey.set(b64.encode(buffer));
        context.write(oKey, iVal);
    }
}
