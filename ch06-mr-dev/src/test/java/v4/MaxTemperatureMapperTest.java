package v4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import java.io.IOException;

public class MaxTemperatureMapperTest {

    @Test
    public void parseMalformedTemperature() throws IOException {

        Text value = new Text("0335999999433181957042302005+37950+139117SAO  +0004" +
                // Year ^^^^
                "RJSN V02011359003150070356999999433201957010100005+353");
        // Temperature ^^^^^
        Counters counters = new Counters();
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .withCounters(counters)
                .runTest();
        Counter c = counters.findCounter(MaxTemperatureMapper.Temperature.MALFORMED);
        assertThat(c.getValue(), is(1L));
    }

}
