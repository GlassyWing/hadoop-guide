import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

public class FileCopyWithProgress {

    public static void main(String[] args) throws IOException {
        String localStr = args[0];
        String dst = args[1];
        InputStream in = new BufferedInputStream(new FileInputStream(localStr));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);

        OutputStream out = fs.create(new Path(dst), () -> System.err.print("."));

        IOUtils.copyBytes(in, out, 4096, true);
    }
}
