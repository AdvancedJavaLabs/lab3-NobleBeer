package org.example.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class SortReducer extends Reducer<DoubleWritable, CompositeData, Text, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<CompositeData> values, Context context) {
        var stream = StreamSupport.stream(values.spliterator(), false);

        stream.forEach(value -> {
            try {
                var category = new Text(value.getCategory());
                var output = new Text(String.format("%.2f\t%d", key.get(), value.getQuantity()));
                context.write(category, output);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
