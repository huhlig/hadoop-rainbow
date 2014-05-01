package com.aol.hadoop.rainbow;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 */
public class RainbowReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(final Text iKey, final Iterable<Text> iValues, Context context) throws IOException, InterruptedException {
        for (final Text text : iValues) {
            context.write(iKey, iKey);
        }
    }

}
