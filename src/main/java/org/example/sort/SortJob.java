package org.example.sort;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class SortJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String inputDir = args[0];
        String outputDir = args[1];
        int reducerCount = Integer.parseInt(args[2]);

        Job sortingJob = initSortJob(reducerCount);

        FileInputFormat.addInputPath(sortingJob, new Path(inputDir));
        FileOutputFormat.setOutputPath(sortingJob, new Path(outputDir));

        boolean success = sortingJob.waitForCompletion(true);

        if (!success) {
            return 1;
        }

        return 0;
    }

    private Job initSortJob(Integer reducerCount) throws IOException {
        var configuration = getConf();

        var job = Job.getInstance(configuration, "sort");
        job.setNumReduceTasks(reducerCount);
        job.setJarByClass(SortJob.class);
        job.setSortComparatorClass(SortComparator.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(CompositeData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }
}
