package v2;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {

    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private String quality;

    public void parse(String record) {
        year = record.substring(15, 19);
        int offset = record.charAt(87) == '+' ? 1 : 0;
        airTemperature = Integer.parseInt(record.substring(87 + offset, 92));
        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {
        return airTemperature != MISSING_TEMPERATURE
                && quality.matches("[01459]");
    }

    public int getAirTemperature() {
        return airTemperature;
    }

    public String getYear() {
        return year;
    }
}
