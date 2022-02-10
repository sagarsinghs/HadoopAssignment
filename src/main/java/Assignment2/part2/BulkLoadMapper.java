package Assignment2.part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BulkLoadMapper extends Mapper<LongWritable,Text, ImmutableBytesWritable, Put> {
    private String hbaseTable;
    private String dataSeparator;
    private String columnFamily1;
    private String columnFamily2;
    private ImmutableBytesWritable hbaseTableName;

    public void setup(Mapper.Context context) {
        Configuration configuration = context.getConfiguration();
        hbaseTable = configuration.get("hbase.table.name");
        dataSeparator = configuration.get("data.seperator");
        columnFamily1 = configuration.get("COLUMN_FAMILY_1");
        columnFamily2 = configuration.get("COLUMN_FAMILY_2");
        hbaseTableName = new ImmutableBytesWritable(Bytes.toBytes(hbaseTable));
    }

    public void map(LongWritable key, Text value, Mapper.Context context) {
        try {
            String[] values = value.toString().split(dataSeparator);
            String rowKey = values[0];
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("first_name"), Bytes.toBytes(values[1]) );
            put.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("last_name"), Bytes.toBytes(values[2]));
            put.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("email"), Bytes.toBytes(values[3]));
            put.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("city"), Bytes.toBytes(values[4]));
            context.write(hbaseTableName, put);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
