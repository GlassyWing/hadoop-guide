import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class DictResourceMetadata {

	private List<DictRecord> resources = new LinkedList<>();

	public static class DictRecord {
		public final String word;
		public final String freq;
		public final String tag;

		public DictRecord(String word, String freq, String tag) {
			this.word = word;
			this.freq = freq;
			this.tag = tag;
		}
	}

	public void initialize(File file) throws IOException {
		try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
			DictRecordParser parser = new DictRecordParser();
			String line;
			while ((line = in.readLine()) != null) {
				if (parser.parse(line)) {
					resources.add(new DictRecord(
							parser.getWord(), parser.getFreq(), parser.getTag()));
				}
			}
		}
	}

	public List<DictRecord> getResources() {
		return resources;
	}
}
