// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        final int[] maxValue = {Integer.MIN_VALUE};

        values.forEach(value -> maxValue[0] = Math.max(maxValue[0], value.get()));

        context.write(key, new IntWritable(maxValue[0]));
    }
}
// ^^ MaxTemperatureReducer
