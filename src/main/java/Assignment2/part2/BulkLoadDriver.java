package Assignment2.part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BulkLoadDriver extends Configured implements Tool {
    private static final String DATA_SEPARATOR = ",";
    private static final String TABLE_NAME = "Peoples";
    private static final String COLUMN_FAMILY1 = "peoples";
    private static final String COLUMN_FAMILY2 = "ContactDetails";

    public static void main(String[] args) {
        try {
            int response = ToolRunner.run(HBaseConfiguration.create(),new BulkLoadDriver(), args);
            if(response == 0) {
                System.out.println("Job is completed Successfully");
            } else {
                System.out.println("Job Failed");
            }
        } catch(Exception ex){
            ex.printStackTrace();
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        int result = 0;
        String outputPath = args[1];
        Configuration configuration = getConf();
        configuration.set("data.separator", DATA_SEPARATOR);
        configuration.set("hbase.table.name",TABLE_NAME);
        configuration.set("COLUMN_FAMILY_1",COLUMN_FAMILY1);
        configuration.set("COLUMN_FAMILY_2",COLUMN_FAMILY2);
        Job job = new Job(configuration);
        job.setJarByClass(BulkLoadDriver.class);
        job.setJobName("Bulk Loading HBase Table::"+TABLE_NAME);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapperClass(BulkLoadMapper.class);
        FileInputFormat.addInputPaths(job, args[0]);
        FileSystem.getLocal(getConf()).delete(new Path(outputPath), true);
        FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(outputPath));
        job.setMapOutputValueClass(Put.class);
        Connection connection = ConnectionFactory.createConnection(configuration);

        Table table = connection.getTable(TableName.valueOf("table1"));
        HFileOutputFormat2.configureIncrementalLoad(job,table, connection.getRegionLocator(TableName.valueOf("people2")));

        job.waitForCompletion(true);
        if (job.isSuccessful()) {
            BulkLoad.doBulkLoad(outputPath, TABLE_NAME);
        } else {
            result = -1;
        }
        return result;
    }
}
