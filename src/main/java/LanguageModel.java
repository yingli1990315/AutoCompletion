import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by YingLi on 5/22/17.
 */

public class LanguageModel {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        int threashold;

        @Override
        public void setup(Context context) {
            Configuration config = new Configuration();
            threashold = config.getInt("threashold", 20);
        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if((value == null) || (value.toString().trim()).length() == 0) {
                return;
            }
            //this is cool\t20
            String line = value.toString().trim();
            String[] wordsPlusCount = line.split("\t");
            if(wordsPlusCount.length < 2) {
                return;
            }

            String[] words = wordsPlusCount[0].split("\\s+");
            int count = Integer.valueOf(wordsPlusCount[1]);

            if(count<threashold) return;

            StringBuilder sb = new StringBuilder();
            for(int i=0; i<words.length-1; i++){
                sb.append(words[i]).append(" ");
            }

            String outputKey = sb.toString().trim();
            String outputValue = words[words.length-1]+"="+count;

            if(!((outputKey == null) || (outputKey.length()<1))){
                context.write(new Text(outputKey), new Text(outputValue));
            }

        }
    }

    public static class Pair implements Comparable<Pair>{
        String word;
        int count;
        public Pair(String _word, int _count){
            word = _word;
            count = _count;
        }

        public int compareTo(Pair o) {
            return (this.count == o.count)? this.word.compareTo(o.word): this.count-o.count;
        }
    }

    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

        int n;
        // get the n parameter from the configuration
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            n = conf.getInt("n", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //can you use priorityQueue to rank topN n-gram, then write out to hdfs?
            PriorityQueue<Pair> heap = new PriorityQueue<Pair>();
            for(Text value: values){
                String curValue = value.toString().trim();
                String word = curValue.split("=")[0].trim();
                int count = Integer.parseInt(curValue.split("=")[1].trim());
                Pair pair = new Pair(word, count);
                if(heap.size() == n){
                    heap.poll();
                }
                heap.add(pair);
            }

            List<Pair> list = new ArrayList<Pair>();
            while(heap.size()>0) list.add(heap.poll());
            for(int i=list.size()-1; i>=0; i--){
                context.write(new DBOutputWritable(key.toString(), list.get(i).word, list.get(i).count), NullWritable.get());
            }
        }
    }
}
