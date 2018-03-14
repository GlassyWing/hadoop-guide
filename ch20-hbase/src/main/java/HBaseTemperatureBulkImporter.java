import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HBaseTemperatureBulkImporter extends Configured implements Tool {

	static class HBaseTemperatureMapper extends Mapper<LongWritable, Text,
			ImmutableBytesWritable, Put> {
		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			parser.parse(value);
			if (parser.isValidTemperature()) {
				byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),
						parser.getObservationDate().getTime());
				Put p = new Put(rowKey);
				p.addColumn(HBaseTemperatureQuery.DATA_COLUMNFAMILY,
						HBaseTemperatureQuery.AIRTEMP_QUALIFIER,
						Bytes.toBytes(parser.getAirTemperature()));
				context.write(new ImmutableBytesWritable(rowKey), p);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: HBaseTemperatureBulkImporter <input>");
			return -1;
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, getClass().getSimpleName());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path tmpPath = new Path("/tmp/bulk");
		FileOutputFormat.setOutputPath(job, tmpPath);
		job.setMapperClass(HBaseTemperatureMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		try (Connection connection = ConnectionFactory.createConnection(conf)) {
			TableName tableName = TableName.valueOf("observations");
			try (Table table = connection.getTable(tableName)) {
				HFileOutputFormat2.configureIncrementalLoad(job, table
						, connection.getRegionLocator(tableName));
				if (!job.waitForCompletion(true)) {
					return 1;
				}

				LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
				loader.doBulkLoad(tmpPath, connection.getAdmin()
						, table
						, connection.getRegionLocator(tableName));
				FileSystem.get(conf).delete(tmpPath, true);
				return 0;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(),
				new HBaseTemperatureBulkImporter(), args);
		System.exit(exitCode);
	}
}
