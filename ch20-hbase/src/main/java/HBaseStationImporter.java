import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class HBaseStationImporter extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: HBaseStationImporter <input>");
			return -1;
		}
		Configuration conf = HBaseConfiguration.create(getConf());
		try (Connection conn = ConnectionFactory.createConnection(conf)) {
			try (Table table = conn.getTable(TableName.valueOf("stations"))) {
				NcdcStationMetadata metadata = new NcdcStationMetadata();
				metadata.initialize(Paths.get(args[0]).toFile());
				Map<String, String> stationIdToNameMap = metadata.getStationIdToName();
				for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
					Put put = new Put(Bytes.toBytes(entry.getKey()));
					put.addColumn(HBaseStationQuery.INFO_COLUMNFAMILY,
							HBaseStationQuery.NAME_QUALIFIER,
							Bytes.toBytes(entry.getValue()));
					put.addColumn(HBaseStationQuery.INFO_COLUMNFAMILY,
							HBaseStationQuery.DESCRIPTION_QUALIFIER,
							Bytes.toBytes("(unknown)"));
					put.addColumn(HBaseStationQuery.INFO_COLUMNFAMILY,
							HBaseStationQuery.LOCATION_QUALIFIER,
							Bytes.toBytes("(unknown)"));
					table.put(put);
				}

			}
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseStationImporter(), args);
		System.exit(exitCode);
	}
}
