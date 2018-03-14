import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;

public class SolrIndexingBuilder extends Configured implements Tool {

	static class SolrIndexerMapper extends TableMapper<Void, Void> {

		public static enum Counters {ROWS}

		private String solrUrl = "http://localhost:8983/solr";
		private String collectionName;
		private SolrClient solrClient;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			collectionName = conf.get("solr.collection");
			solrClient = new HttpSolrClient.Builder(solrUrl)
					.withConnectionTimeout(10000)
					.withSocketTimeout(60000)
					.build();
		}

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			SolrInputDocument solrDoc = new SolrInputDocument();
			String id = Bytes.toString(value.getRow());

			solrDoc.addField("id", id);
			for (Cell cell : value.listCells()) {
				String filedName = Bytes.toString(cell.getQualifierArray());
				String filedValue = Bytes.toString(cell.getValueArray());
				solrDoc.addField(filedName, filedValue);
			}
			try {
				solrClient.add(collectionName, solrDoc);
			} catch (SolrServerException e) {
				throw new IOException(e);
			}
			context.getCounter(Counters.ROWS).increment(1);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			solrClient.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: SolrIndexingBuilder <tablename> <collectionname>");
			return -1;
		}
		Configuration config = HBaseConfiguration.create(getConf());
		config.set("solr.collection", args[1]);
		Job job = Job.getInstance(HBaseConfiguration.create(getConf()),
				getClass().getSimpleName());
		job.setJarByClass(getClass());
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob(args[0], scan, SolrIndexerMapper.class,
				null, null, job);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SolrIndexingBuilder(), args);
		System.exit(exitCode);
	}
}
