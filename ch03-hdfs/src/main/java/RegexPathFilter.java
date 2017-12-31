import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexPathFilter implements PathFilter {

    private final String regex;
    private final boolean include;

    public RegexPathFilter(String regex, boolean include) {
        this.regex = regex;
        this.include = include;
    }

    RegexPathFilter(String regex) {
        this(regex, true);
    }

    @Override
    public boolean accept(Path path) {
        return (path.toString().matches(regex) == include);
    }
}
