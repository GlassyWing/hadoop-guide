// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class MaxTemperatureReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        Optional<IntWritable> maxValue = StreamSupport.stream(values.spliterator(), false)
                .map(IntWritable::get)
                .max(Integer::compareTo)
                .map(IntWritable::new);

        if (maxValue.isPresent()) {
            context.write(key, maxValue.get());
        }
    }
}
// ^^ MaxTemperatureReducer
