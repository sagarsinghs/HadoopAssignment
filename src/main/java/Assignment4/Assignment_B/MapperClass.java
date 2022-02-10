package Assignment4.Assignment_B;

import example.building.Buildings;
import example.employees.EmployeeCafeteria;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;


import java.io.IOException;
import java.util.Arrays;

public class MapperClass extends TableMapper<IntWritable,Result> {

    public MapperClass(){
        System.out.println("in the mapper Class");
    }


    @Override
    public void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        TableSplit tableSplit = (TableSplit) context.getInputSplit();
        String ss= String.valueOf(result.value());
        System.out.println("the value of result is " + ss);

        byte[] tablename = tableSplit.getTableName();
        if(Arrays.equals(tablename, Bytes.toBytes("employee"))){
            EmployeeCafeteria.EmployeeDetailsCafeteria employee = EmployeeCafeteria.EmployeeDetailsCafeteria.parseFrom(result.value());
            System.out.println("Employee : " + employee);
            int building_code = Integer.parseInt(employee.getBuildingCodeCafeteria());
            context.write(new IntWritable(building_code),result);
        }
        else{
            Buildings.BuildingsData building = Buildings.BuildingsData.parseFrom(result.value());
            System.out.println("Building : " + building);
            int building_code= Integer.parseInt(building.getBuildingCode());
            context.write(new IntWritable(building_code),result);
        }
    }
}
