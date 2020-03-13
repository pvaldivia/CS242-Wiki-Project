import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.MapWritable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String title;
            String url;
            String description;
            JSONParser jsonParser = new JSONParser();
            try { // Extract title and description from wiki page as strings
                JSONObject page = (JSONObject) jsonParser.parse(value.toString());
                // Remove all non alpha characters and update string
                title = page.get("title").toString().replaceAll("[^a-zA-Z ]", "").toLowerCase();
                url = page.get("url").toString();
                description = page.get("description").toString().replaceAll("[^a-zA-Z ]", "").toLowerCase();
                StringTokenizer itr = new StringTokenizer(description);
                // We don't have an ID, so consider the url as the ID
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    if(word.toString() != " " && !word.toString().isEmpty()){
                        context.write(word, new Text(url));
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<String, Integer> docIdx = new HashMap();
            String id;
            for (Text url : values) {
                id = url.toString();
                // First occurrence. Initialize count to 1
                if(docIdx.containsKey(id)) {
                    docIdx.put(id, docIdx.get(id) + 1);
                } else { // Increment count by 1
//                    System.out.println(id);
                    docIdx.put(id, 1);
                }
            }

//            System.out.println(docIdx.toString());

//            StringBuilder docValueList = new StringBuilder();
//            for(String docID : docIdx.keySet()){
////                docValueList.append(docID);
////                if(docID != " " && docID != "") {
//                docValueList.append(docID + ":" + docIdx.get(docID) + ", ");
////                }
//            }

//            System.out.println(docValueList.toString());
//            String result;
//            for(String doc : docIdx.keySet()) {
//                result = result + doc + " : " + docIdx.get(doc);
////                System.out.println(word);
////                System.out.println(docIdx.get(word));
//            }
            context.write(key, new Text(docIdx.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job buildIndexJob = Job.getInstance(conf, "Inverted Index");
        buildIndexJob.setJarByClass(InvertedIndex.class);
        buildIndexJob.setMapperClass(TokenizerMapper.class);
        buildIndexJob.setCombinerClass(IntSumReducer.class);
        buildIndexJob.setReducerClass(IntSumReducer.class);

        buildIndexJob.setOutputKeyClass(Text.class);
        buildIndexJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(buildIndexJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(buildIndexJob, new Path(args[1]));
        System.exit(buildIndexJob.waitForCompletion(true) ? 0 : 1);
    }
}