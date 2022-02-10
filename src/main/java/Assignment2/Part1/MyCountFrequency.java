package Assignment2.Part1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;

public class MyCountFrequency {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://localhost:9000");
        FileSystem hdfs = FileSystem.get(uri, conf);


        Job job = Job.getInstance(conf, "WC");
        job.setJarByClass(MyCountFrequency.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000//Users/sagarsangam/Downloads/directory1"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000//Users/sagarsangam/Downloads/output_txt1"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
