package Assignment4.Assignment_B;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.io.IntWritable;


import java.io.IOException;

public class MapperClass extends TableMapper<IntWritable,Result> {

    public MapperClass(){
        System.out.println("in the mapper Class");
    }


    @Override
    public void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        TableSplit tableSplit = (TableSplit) context.getInputSplit();
        String ss= String.valueOf(result.value());
        System.out.println("the value of result is " + ss);

       // byte[] tablename = tableSplit.getTableName();
//        if(Arrays.equals(tablename, Bytes.toBytes("employee"))){
//            Employee employee = Employee.parseFrom(result.value());
//            System.out.println("Employee : " + employee);
//            int building_code = employee.getBuildingCode();
//            context.write(new IntWritable(building_code),result);
//        }
//        else{
//            Building building = Building.parseFrom(result.value());
//            System.out.println("Building : " + building);
//            int building_code= building.getBuildingCode();
//            context.write(new IntWritable(building_code),result);
//        }
    }
}
