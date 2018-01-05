package v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class MaxTemperatureReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        Stream<IntWritable> seq = StreamSupport.stream(values.spliterator(), false);

        Optional<IntWritable> maxValue = seq
                .map(IntWritable::get)
                .max(Integer::compare)
                .map(IntWritable::new);

        if (maxValue.isPresent()) {
            context.write(key, maxValue.get());
        }
    }
}
