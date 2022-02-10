package Assignment4.Assignment_B;

import com.google.protobuf.InvalidProtocolBufferException;
import example.building.Buildings;
import example.employees.Employee;
import example.employees.EmployeeCafeteria;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReducerClass extends TableReducer<IntWritable, Result, ImmutableBytesWritable> {
    public ReducerClass() {
        System.out.println("in the reducer class");
    }

    @Override
    public void reduce(IntWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        int cafeteria_code = 0;
        List<EmployeeCafeteria.EmployeeDetailsCafeteria> employeeList = new ArrayList<>();

        for (Result result : values) {

            System.out.println("Inside the reducer class" + result.toString());
            if (result.containsColumn(Bytes.toBytes("EMPLOYEE"), Bytes.toBytes("employeeDetails"))) {
                EmployeeCafeteria.EmployeeDetailsCafeteria employee = EmployeeCafeteria.EmployeeDetailsCafeteria.parseFrom(result.value());

                Buildings.BuildingsData.Builder builder =  Buildings.BuildingsData.newBuilder();

                System.out.println(builder.getCafeteriaCode());

                employeeList.add(employee);
            }
            if (result.containsColumn(Bytes.toBytes("BUILDING"), Bytes.toBytes("buildingDetails"))) {
                Buildings.BuildingsData building = Buildings.BuildingsData.parseFrom(result.value());

                cafeteria_code = Integer.parseInt(building.getCafeteriaCode());
                int building_id = Integer.parseInt(building.getBuildingCode());
            }


               for (EmployeeCafeteria.EmployeeDetailsCafeteria employee : employeeList) {
            Put put = enrichCafeteriaCode(employee, cafeteria_code);
            int employee_id = Integer.parseInt(employee.getDepartmentCafeteria());
            context.write(new ImmutableBytesWritable(Bytes.toBytes("EMPLOYEE_CAFETERIA_CODE")), put);

             }
        }
    }
    public Put enrichCafeteriaCode(EmployeeCafeteria.EmployeeDetailsCafeteria employee, int cafe_value) {
        employee = employee.toBuilder().setCafeteriaCode(String.valueOf(cafe_value)).build();
        String name = employee.getNameCafeteria();
       // String cafeteria
        int building_code = Integer.parseInt(employee.getBuildingCodeCafeteria());
        int salary = employee.getSalaryCafeteria();
        String department = employee.getDepartmentCafeteria();
        int floor = employee.getNumOfFloorsCafeteria().getNumber();
        int cafeteria_code = Integer.parseInt(employee.getBuildingCodeCafeteria());


        System.out.println("employee with Cafeteria code : " + employee);

        Put put = new Put(Bytes.toBytes(department));
        put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("building_code"), Bytes.toBytes(building_code + ""));
      //  put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("employee_id"), Bytes.toBytes(employee_id + ""));
        put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("salary"), Bytes.toBytes(salary + ""));
        put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("department"), Bytes.toBytes(department + ""));
        put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("floor"), Bytes.toBytes(floor + ""));
        put.addColumn(Bytes.toBytes("employeeCafeteriaCodeDetails"), Bytes.toBytes("cafeteria_code"), Bytes.toBytes(cafeteria_code + ""));
        return put;

    }
}