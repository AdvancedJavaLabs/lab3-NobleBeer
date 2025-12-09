package org.example.analyse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.IOException;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class SalesWritable implements Writable {
    private double revenue;
    private int quantity;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(revenue);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(java.io.DataInput in) throws IOException {
        revenue = in.readDouble();
        quantity = in.readInt();
    }
}
