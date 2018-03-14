import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompMetadata {

	private CompDocumentParser parser = new CompDocumentParser();
	private List<CompDocumentParser.Record> records;

	public void initialize(Path fileDir) throws IOException {
		records = Files.list(fileDir)
				.filter((path) -> !path.getFileName().toString().startsWith("_"))
				.parallel()
				.flatMap((Function<Path, Stream<CompDocumentParser.Record>>) path -> {
					Document doc;
					try {
						doc = Jsoup.parse(path.toFile(), "UTF-8");
						parser.parse(doc);
						return parser.getRecords().stream();
					} catch (IOException e) {
						e.printStackTrace();
						return null;
					}
				}).collect(Collectors.toList());
	}

	public List<CompDocumentParser.Record> getRecords() {
		return records;
	}
}
