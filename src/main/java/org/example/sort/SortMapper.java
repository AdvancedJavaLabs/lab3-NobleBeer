package org.example.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, DoubleWritable, CompositeData> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        var fields = value.toString().split("\t");

        if (fields.length == 3) {
            var category = fields[0];
            var revenue = Double.parseDouble(fields[1]);
            var quantity = Integer.parseInt(fields[2]);

            context.write(new DoubleWritable(revenue), new CompositeData(category, quantity));
        }
    }

}
