import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(JUnit4.class)
public class CompDocumentParserTest {

	CompDocumentParser parser = new CompDocumentParser();

	@Test
	public void testParseSingleDoc() throws IOException {
		File input = Paths.get("../input/symbols/SYSTEM.html").toFile();
		Document doc = Jsoup.parse(input, "UTF-8");
		parser.parse(doc);
		Assert.assertEquals(2, parser.getRecords().size());
		Assert.assertEquals("SYSTEM.refresh()", parser.getRecords().get(0).name);
	}

	@Test
	public void testParseMulDocs() throws IOException {
		List<CompDocumentParser.Record> records = Files.list(Paths.get("../input/symbols"))
				.filter(path -> !path.getFileName().toString().startsWith("_"))
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
		System.out.println(records);
	}
}
