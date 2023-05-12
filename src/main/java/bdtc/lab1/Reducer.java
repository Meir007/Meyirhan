package bdtc.lab1;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        String temperature = getTemperature(sum);
        context.write(new Text(key.toString() + sum + " (" + temperature + ")"), new IntWritable(sum));
    }
    private String getTemperature(int clicks) {
        Map<String, String> tempMap = new HashMap<>();

        tempMap.put("0-9", "Маленькая");
        tempMap.put("10-99", "Средняя");
        tempMap.put("100+", "Большая");

        if (clicks < 10) {
            return tempMap.get("0-9");
        } else if (clicks < 100) {
            return tempMap.get("10-99");
        } else {
            return tempMap.get("100+");
        }
    }
}

