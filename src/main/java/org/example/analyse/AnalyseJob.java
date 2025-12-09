package org.example.analyse;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class AnalyseJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        String inputDir = args[0];
        String outputDir = args[1];

        int reducerCount = Integer.parseInt(args[2]);
        var analysisJob = initAnalyseJob(reducerCount);

        FileInputFormat.addInputPath(analysisJob, new Path(inputDir));
        FileOutputFormat.setOutputPath(analysisJob, new Path(outputDir));

        var success = analysisJob.waitForCompletion(true);

        if (!success) {
            return 1;
        }

        return 0;
    }

    private Job initAnalyseJob(Integer reducerCount) throws IOException {
        var configuration = getConf();

        var job = Job.getInstance(configuration, "analyse");
        job.setNumReduceTasks(reducerCount);
        job.setJarByClass(AnalyseJob.class);
        job.setMapperClass(AnalyseMapper.class);
        job.setReducerClass(AnalyseReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }
}
