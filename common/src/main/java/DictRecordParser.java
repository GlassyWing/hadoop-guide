import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DictRecordParser {

	private static Pattern RE_USER_DICT = Pattern.compile("^(.+?)\\s*([0-9]+)?\\s*([a-z]+)?$");

	private String word;
	private String freq;
	private String tag;

	public boolean parse(String line) {
		Matcher m = RE_USER_DICT.matcher(line);
		if (m.matches()) {
			word = m.group(1);
			if (m.group(2) != null)
				freq = m.group(2);
			if (m.group(3) != null)
				tag = m.group(3);
			return true;
		}
		return false;
	}


	public String getWord() {
		return word;
	}

	public String getFreq() {
		return freq;
	}

	public String getTag() {
		return tag;
	}


}
