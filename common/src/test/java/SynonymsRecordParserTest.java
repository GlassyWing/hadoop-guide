import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class SynonymsRecordParserTest {

	private SynonymsRecordParser parser;

	@Before
	public void setUp() {
		parser = new SynonymsRecordParser();
	}

	@Test
	public void testParse() throws IOException {
		String jsonStr = "{\"word\": \"荣誉\", \"synonyms\": [\"荣耀\"], \"antonym\": [\"屈辱\", \"耻辱\"]}";
		parser.parse(jsonStr);
		assertTrue(parser.isRecordValid());
		assertEquals(2, parser.getAntonym().size());
	}
}