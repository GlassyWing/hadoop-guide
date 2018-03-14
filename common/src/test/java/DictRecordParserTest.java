import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class DictRecordParserTest {

	private DictRecordParser parser = new DictRecordParser();

	@Test
	public void testParser() {
		parser.parse("中华 4 n");
		parser.parse("中国");
		parser.parse("中 国 n n");
	}

	@Test
	public void testParse() {
		parser.parse("中 国 n n");
		System.out.println(parser.getWord());
		System.out.println(parser.getFreq());
		System.out.println(parser.getTag());
	}
}
