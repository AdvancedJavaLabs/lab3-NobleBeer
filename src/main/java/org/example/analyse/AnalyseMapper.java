package org.example.analyse;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnalyseMapper extends Mapper<Object, Text, Text, SalesWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        var fields = value.toString().split(",");

        if (fields.length == 5 && !fields[0].equals("transaction_id")) {
            var category = fields[2];
            var price = Double.parseDouble(fields[3]);
            var quantity = Integer.parseInt(fields[4]);

            context.write(new Text(category), new SalesWritable(price * quantity, quantity));
        }
    }

}

