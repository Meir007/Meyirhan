package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    private Map<Rectangle, String> areas;
    private Map<IntPair, String> temperatures;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        areas = new HashMap<>();
        areas.put(new Rectangle(0, 0, 500, 500), "Верхний угол");
        areas.put(new Rectangle(500, 0, 1000, 500), "Правый угол");
        areas.put(new Rectangle(0, 500, 500, 1000), "Левый угол");
        areas.put(new Rectangle(500, 500, 1000, 1000), "Нижний правый угол");

    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        // Получаем координаты нажатия
        int x = Integer.parseInt(fields[0]);
        int y = Integer.parseInt(fields[1]);

        // Определяем область экрана, в которой произошло нажатие
        String areaName = "Неопределнная область";
        for (Map.Entry<Rectangle, String> entry : areas.entrySet()) {
            Rectangle rect = entry.getKey();
            if (rect.contains(x, y)) {
                areaName = entry.getValue();
                break;
            }
        }



        // Отправляем результат в reducer
        context.write(new Text(areaName + " "), new IntWritable(1));
    }

    private static class Rectangle {
        private final int x1;
        private final int y1;
        private final int x2;
        private final int y2;

        public Rectangle(int x1, int y1, int x2, int y2) {
            this.x1 = x1;
            this.y1 = y1;
            this.x2 = x2;
            this.y2 = y2;
        }

        public boolean contains(int x, int y) {
            return x >= x1 && x < x2 && y >= y1 && y < y2;
        }
    }

    private static class IntPair {
        private final int left;
        private final int right;

        public IntPair(int left, int right) {
            this.left = left;
            this.right = right;
        }

        public int getLeft() {
            return left;
        }

        public int getRight() {
            return right;
        }
    }
}



