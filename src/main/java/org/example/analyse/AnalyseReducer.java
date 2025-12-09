package org.example.analyse;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AnalyseReducer extends Reducer<Text, SalesWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<SalesWritable> values, Context context) throws IOException, InterruptedException {
        var totalRevenue = 0.0;
        var totalQuantity = 0;

        for (SalesWritable val : values) {
            totalRevenue += val.getRevenue();
            totalQuantity += val.getQuantity();
        }

        context.write(key, new Text(String.format("%.2f\t%d", totalRevenue, totalQuantity)));
    }

}

