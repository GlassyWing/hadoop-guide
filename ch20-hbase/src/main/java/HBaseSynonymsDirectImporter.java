import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public class HBaseSynonymsDirectImporter extends Configured implements Tool {

	static final String TABLE_NAME = "thesaurus";
	static final byte[] SYNONYMS_COLUMNFAMILYS = Bytes.toBytes("synonyms");

	static class HBaseSynonymsMapper extends Mapper<LongWritable, Text, Void, Void> {
		private HTable table;
		private SynonymsRecordParser parser = new SynonymsRecordParser();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			table = new HTable(HBaseConfiguration.create(context.getConfiguration()),
					TABLE_NAME);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			parser.parse(value.toString());
			if (parser.isRecordValid()) {
				List<Put> puts = generateSynonymsPuts(parser.getSynonyms(), 1);
				table.put(puts);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			table.close();
		}

		private List<Put> generateSynonymsPuts(Queue<String> words, float initRelation) {
			List<Put> puts = new LinkedList<>();
			int len = words.size();
			for (int i = 0; i < len; i++) {
				String word = words.poll();
				puts.addAll(words.stream().map(s -> {
					Put put = new Put(Bytes.toBytes(word));
					put.addColumn(SYNONYMS_COLUMNFAMILYS,
							Bytes.toBytes(s),
							Bytes.toBytes(initRelation));
					return put;
				}).collect(Collectors.toList()));
			}
			return puts;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: HBaseSynonymsDirectImporter <input>");
			return -1;
		}
		Job job = Job.getInstance(getConf(), getClass().getSimpleName());
		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(HBaseSynonymsMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HBaseSynonymsDirectImporter(), args);
		System.exit(exitCode);
	}
}
