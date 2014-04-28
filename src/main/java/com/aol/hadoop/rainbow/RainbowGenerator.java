package com.aol.hadoop.rainbow;

import java.io.IOException;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * RainbowGenerator
 */
public class RainbowGenerator extends Configured implements Tool {
    
    public static Job createJob(final Configuration conf, final Path outputPath) throws IOException {
        final Job job = Job.getInstance(conf, "Rainbow Table Generator");
        job.setJarByClass(RainbowGenerator.class);

        // Configure Input
        job.setInputFormatClass(RainbowInputFormat.class);

        // Configure Mapper
        job.setMapperClass(RainbowMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.getConfiguration().set("rainbow.algorithm", "MD5");
        job.getConfiguration().setInt("rainbow.minLength", 1);
        job.getConfiguration().setInt("rainbow.maxLength", 10);

        // Configure Reducer
        job.setReducerClass(RainbowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        // Configure Output
        SequenceFileOutputFormat.setOutputPath(job, outputPath);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        
        return job;
    }
    
    @Override
    public int run(final String[] args) throws Exception {
        // Parse Options
        final Options options = new Options();
        options.addOption("a", "rainbow.algorithm", true, "Digest Algorithm");
        options.addOption("b", "rainbow.minLength", true, "Minimum password Length");
        options.addOption("e", "rainbow.maxLength", true, "Maximum password Length");
        final GenericOptionsParser gop = new GenericOptionsParser(getConf(), options, args);
        final Job job = createJob(getConf(), new Path(gop.getRemainingArgs()[0]));
        job.submit();
        System.out.println("Tracking URL: "+job.getTrackingURL());
        return 0;
    }
    
    public static void main(final String[] args) throws Exception {
        System.exit(ToolRunner.run(new RainbowGenerator(), args));
    }
}
