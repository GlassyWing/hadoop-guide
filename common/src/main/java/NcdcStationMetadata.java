import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NcdcStationMetadata {

    private Map<String, String> stationIdToName = new HashMap<>();

    public void initialize(File file) throws IOException {
        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            NcdcStationMetadataParser parser = new NcdcStationMetadataParser();
            String line;
            while ((line = in.readLine()) != null) {
                if (parser.parse(line)) {
                    stationIdToName.put(parser.getStationId(), parser.getStationName());
                }
            }
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public String getStationName(String stationId) {
        String stationName = stationIdToName.get(stationId);
        if (stationName == null || stationName.trim().length() == 0) {
            return stationId;
        }
        return stationName;
    }

    public Map<String, String> getStationIdToName() {
        return Collections.unmodifiableMap(stationIdToName);
    }
}
