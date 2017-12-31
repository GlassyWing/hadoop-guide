import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

public class FileSystemGlobTest {

    private static final String BASE_PATH="/tmp" +
            FileSystemGlobTest.class.getSimpleName();

    private FileSystem fs;

    @Before
    public void setUp() throws Exception {
        fs = FileSystem.get(new Configuration());
        fs.mkdirs(new Path(BASE_PATH, "2007/12/30"));
        fs.mkdirs(new Path(BASE_PATH, "2007/12/31"));
        fs.mkdirs(new Path(BASE_PATH, "2008/01/01"));
        fs.mkdirs(new Path(BASE_PATH, "2008/01/02"));
    }

    @After
    public void tearDown() throws Exception {
        fs.delete(new Path(BASE_PATH), true);
    }

    @Test
    public void glob() throws Exception {
        assertThat(glob("/*"), is(paths("/2007", "/2008")));
        assertThat(glob("/*/*"), is(paths("/2007/12", "2008/01")));
        assertThat(glob("/*/12/*"), is(paths("/2007/12/30", "/2007/12/31")));
        assertThat(glob("/200?"), CoreMatchers.is(paths("/2007", "/2008")));
        assertThat(glob("/200[78]"), CoreMatchers.is(paths("/2007", "/2008")));
        assertThat(glob("/200[7-8]"), CoreMatchers.is(paths("/2007", "/2008")));
        assertThat(glob("/200[^01234569]"), CoreMatchers.is(paths("/2007", "/2008")));

        assertThat(glob("/*/*/{31,01}"), CoreMatchers.is(paths("/2007/12/31", "/2008/01/01")));
        assertThat(glob("/*/*/3{0,1}"), CoreMatchers.is(paths("/2007/12/30", "/2007/12/31")));

        assertThat(glob("/*/{12/31,01/01}"), CoreMatchers.is(paths("/2007/12/31", "/2008/01/01")));
    }

    @Test
    public void regexIncludes() throws Exception {
        assertThat(glob("/*", new RegexPathFilter("^.*/2007$")), CoreMatchers.is(paths("/2007")));
        assertThat(glob("/*/*/*", new RegexPathFilter("^.*/2007/12/31$")), CoreMatchers.is(paths("/2007/12/31")));
        assertThat(glob("/*/*/*", new RegexPathFilter("^.*/2007(/12(/31)?)?$")), CoreMatchers.is(paths("/2007/12/31")));
    }

    @Test
    public void regexExcludes() throws Exception {
        assertThat(glob("/*", new RegexPathFilter("^.*/2007$", false)), CoreMatchers.is(paths("/2008")));
        assertThat(glob("/2007/*/*", new RegexPathFilter("^.*/2007/12/31$", false)), CoreMatchers.is(paths("/2007/12/30")));
    }

    @Test
    public void testDateRange() throws Exception {
        DateRangePathFilter filter = new DateRangePathFilter(date("2007/12/31"),
                date("2008/01/01"));
        assertThat(glob("/*/*/*", filter), CoreMatchers.is(paths("/2007/12/31", "/2008/01/01")));
    }


    private Set<Path> glob(String pattern) throws IOException {
        return new HashSet<>(Arrays.asList(FileUtil.stat2Paths(fs.globStatus(new Path(BASE_PATH + pattern)))));
    }

    private Set<Path> glob(String pattern, PathFilter pathFilter) throws IOException {
        return new HashSet<>(Arrays.asList(FileUtil.stat2Paths(
                fs.globStatus(new Path(pattern), pathFilter)
        )));
    }

    private Set<Path> paths(String... pathStrings) {
        return new TreeSet<>(Arrays.stream(pathStrings).map(Path::new).collect(Collectors.toList()));
    }

    private Date date(String date) throws ParseException {
        return new SimpleDateFormat("yyyy/MM/dd").parse(date);
    }
}
