import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

public class HBaseTemperatureQuery extends Configured implements Tool {
	static final byte[] DATA_COLUMNFAMILY = Bytes.toBytes("data");
	static final byte[] AIRTEMP_QUALIFIER = Bytes.toBytes("airtemp");

	public NavigableMap<Long, Integer>
	getStationObservations(Table table, String stationId, long maxStamp, int maxCount)
			throws IOException {
		byte[] startRow = RowKeyConverter.makeObservationRowKey(stationId, maxStamp);
		NavigableMap<Long, Integer> resultMap = new TreeMap<>();
		Scan scan = new Scan(startRow);
		scan.addColumn(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);
		try (ResultScanner scanner = table.getScanner(scan)) {
			Result res;
			int count = 0;
			while ((res = scanner.next()) != null && count++ < maxCount) {
				byte[] row = res.getRow();
				byte[] value = res.getValue(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);
				Long stamp = Long.MAX_VALUE -
						Bytes.toLong(row, row.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
				Integer temp = Bytes.toInt(value);
				resultMap.put(stamp, temp);
			}
		}

		return resultMap;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: HBaseTemperatureQuery <station_id>");
			return -1;
		}

		Configuration conf = HBaseConfiguration.create(getConf());
		try (Connection conn = ConnectionFactory.createConnection(conf)) {
			try (Table table = conn.getTable(TableName.valueOf("observations"))) {
				NavigableMap<Long, Integer> observations =
						getStationObservations(table, args[0], Long.MAX_VALUE, 10)
								.descendingMap();
				observations.forEach((k, v) -> System.out.printf("%1$tF %1$tR\t%2$s\n", k, v));
				return 0;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HBaseTemperatureQuery(), args);
		System.exit(exitCode);
	}
}
