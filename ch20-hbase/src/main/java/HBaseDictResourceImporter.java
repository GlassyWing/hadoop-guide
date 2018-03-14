import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.List;


public class HBaseDictResourceImporter extends Configured implements Tool {
	static final byte[] INFO_COLUMNFAMILY = Bytes.toBytes("info");
	static final byte[] WEIGHT_QUALIFIER = Bytes.toBytes("weight");
	static final byte[] TAG_QUALIFIER = Bytes.toBytes("wordclass");

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: HBaseDictResourceImporter <config> <input>");
			return -1;
		}

		Configuration config = HBaseConfiguration.create(getConf());
		try (Connection connection = ConnectionFactory.createConnection(config)) {
			TableName tableName = TableName.valueOf("jieba_dict");
			try (Table table = connection.getTable(tableName)) {
				DictResourceMetadata metadata = new DictResourceMetadata();
				metadata.initialize(new File(args[0]));
				List<DictResourceMetadata.DictRecord> records = metadata.getResources();
				for (DictResourceMetadata.DictRecord record : records) {
					Put put = new Put(Bytes.toBytes(record.word));
					if (record.freq != null)
						put.addColumn(INFO_COLUMNFAMILY, WEIGHT_QUALIFIER,
								Bytes.toBytes(record.freq));
					if (record.tag != null)
						put.addColumn(INFO_COLUMNFAMILY, TAG_QUALIFIER,
								Bytes.toBytes(record.tag));
					table.put(put);
				}
			}
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HBaseDictResourceImporter(), args);
		System.exit(exitCode);
	}
}
