import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class HBaseCompResourceImporter extends Configured implements Tool {
	static final String TABLE_NAME = "components";
	static final byte[] INFO_COLUMNFAMILY = Bytes.toBytes("info");
	static final byte[] DESC_QUALIFIER = Bytes.toBytes("desc");

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: HBaseCompResourceImporter <config> <input>");
			return -1;
		}
		Configuration configuration = HBaseConfiguration.create(getConf());
		try (Connection connection = ConnectionFactory.createConnection(configuration)) {
			try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
				CompMetadata metadata = new CompMetadata();
				metadata.initialize(Paths.get(args[0]).toAbsolutePath().normalize());
				List<Put> puts = metadata.getRecords().parallelStream()
						.map(record -> {
							Put put = new Put(Bytes.toBytes(record.name));
							put.addColumn(INFO_COLUMNFAMILY, DESC_QUALIFIER,
									Bytes.toBytes(record.desc));
							return put;
						}).collect(Collectors.toList());
				table.put(puts);
			}

		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HBaseCompResourceImporter(), args);
		System.exit(exitCode);
	}
}
