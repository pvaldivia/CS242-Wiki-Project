import java.io.IOException;
import java.util.*;

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

        private final static Text title = new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String sw[] = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "you're", "you've", "you'll", "you'd", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "she's", "her", "hers", "herself", "it", "it's", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "that'll", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "don't", "should", "should've", "now", "d", "ll", "m", "o", "re", "ve", "y", "ain", "aren", "aren't", "couldn", "couldn't", "didn", "didn't", "doesn", "doesn't", "hadn", "hadn't", "hasn", "hasn't", "haven", "haven't", "isn", "isn't", "ma", "mightn", "mightn't", "mustn", "mustn't", "needn", "needn't", "shan", "shan't", "shouldn", "shouldn't", "wasn", "wasn't", "weren", "weren't", "won", "won't", "wouldn", "wouldn't"};
            HashSet<String> stopwords = new HashSet<String>(Arrays.asList(sw));
            String description;
            JSONParser jsonParser = new JSONParser();
            try { // Extract title and description from wiki page as strings
                JSONObject page = (JSONObject) jsonParser.parse(value.toString());
                // Remove all non alpha characters and update string
                title.set(page.get("title").toString().replaceAll("[^a-zA-Z ]", "").toLowerCase());
//                url.set(page.get("url").toString());
                description = page.get("description").toString().replaceAll("[^a-zA-Z ]", "").toLowerCase();
                StringTokenizer itr = new StringTokenizer(description);
                // We don't have an ID, so consider the url as the ID
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    if(word.toString() != " " && !word.toString().isEmpty() && !stopwords.contains(word.toString())){
                        context.write(word, title);
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
                if(docIdx.containsKey(id)) { // Increment count by 1
                    docIdx.put(id, docIdx.get(id) + 1);
                } else { // First occurrence. Initialize count to 1
                    docIdx.put(id, 1);
                }
            }
            context.write(key, new Text(docIdx.toString()));

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
//            context.write(key, new Text(docIdx.toString()));
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