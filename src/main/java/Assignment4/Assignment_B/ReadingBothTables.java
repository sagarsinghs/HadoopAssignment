package Assignment4.Assignment_B;

import example.building.Buildings;
import example.employees.Employee;
import example.employees.EmployeeCafeteria;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class ReadingBothTables {
    static  Employee.EmployeeDetails.Builder builder = Employee.EmployeeDetails.newBuilder();
   // static  EmployeeCafeteria.EmployeeDetailsCafeteria.Builder builderCafetria = EmployeeCafeteria.EmployeeDetailsCafeteria.newBuilder();
    static int rowNum = 0;

    //this variable is to cross check the number of lines are there in the file.the number if lines present in each file is equal to each file number in HDFS.
    static int count = 0;


    public static  void floorsDetail(char floor_number)
    {

        switch (floor_number-'0')
        {
            case 1:builder.setNumOfFloors(Employee.NumberOfFloors.ONE_FLOOR);break;
            case 2:builder.setNumOfFloors(Employee.NumberOfFloors.TWO_FLOORS);break;
            case 3:builder.setNumOfFloors(Employee.NumberOfFloors.THREE_FLOORS);break;
            case 4:builder.setNumOfFloors(Employee.NumberOfFloors.FOUR_FLOORS);break;
            case 5:builder.setNumOfFloors(Employee.NumberOfFloors.FIVE_FLOORS);break;
            case 6:builder.setNumOfFloors(Employee.NumberOfFloors.SIX_FLOORS);break;
            case 7:builder.setNumOfFloors(Employee.NumberOfFloors.SEVEN_FLOORS);break;
            case 8:builder.setNumOfFloors(Employee.NumberOfFloors.EIGHT_FLOORS);break;
            case 9:builder.setNumOfFloors(Employee.NumberOfFloors.NINE_FLOORS);break;
            case 10:builder.setNumOfFloors(Employee.NumberOfFloors.TEN_FLOORS);break;
            default:
                System.out.println("wrong floor");

        }

    }

    public  static void updatingHbaseEmployeeCafeteriaTable() throws  Exception {
        try {

            //The configuration object contains all Hadoop settings necessary to launch your app.
            // It's in key value format and is read from the xml files from /etc/hadoop
            Configuration conf = HBaseConfiguration.create();

            //Firstly, we need to create a connection to the database and get admin object, which we will use for manipulating a database structure
            Connection connection = ConnectionFactory.createConnection(conf);


            // Verifying the existance of the table
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            Table table;
            if (admin.tableExists(TableName.valueOf("employeeCafeteria"))) {
                //Table Exist so just create connection
                System.out.println("Table Already Exists No Need To Create");
                table = connection.getTable(TableName.valueOf("employeeCafeteria"));
            } else {

                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("employeeCafeteria"));

                // Adding column families to table descriptor
                //Name, age, company, building_code, phone_number, address
                tableDescriptor.addFamily(new HColumnDescriptor("EmployeesCafe"));
                tableDescriptor.addFamily(new HColumnDescriptor("name"));
                tableDescriptor.addFamily(new HColumnDescriptor("address"));
                tableDescriptor.addFamily(new HColumnDescriptor("department"));
                tableDescriptor.addFamily(new HColumnDescriptor("salary"));
                tableDescriptor.addFamily(new HColumnDescriptor("floor_number"));
                tableDescriptor.addFamily(new HColumnDescriptor("cafeteria_code"));
                System.out.println("creating table... ");

                admin.createTable(tableDescriptor);
                System.out.println("Done! ");
                table = connection.getTable(TableName.valueOf("employeeCafeteria"));
            }


            System.out.println(builder.getName()+ " " + builder.getBuildingCode()+ " "+ builder.getDepartment()+" "+
                    builder.getSalary()+ " " + builder.getNumOfFloorsValue() + " "+ builder.getCafeteriaCode());
                Put p = new Put(Bytes.toBytes("row" + rowNum));

                String salary = String.valueOf(builder.getSalary());
                String floor=String.valueOf(builder.getNumOfFloorsValue());

                p.addColumn(Bytes.toBytes("EmployeesCafe"), Bytes.toBytes("name"), Bytes.toBytes(builder.getName()));
                p.addColumn(Bytes.toBytes("EmployeesCafe"), Bytes.toBytes("address"), Bytes.toBytes(builder.getBuildingCode()));
                p.addColumn(Bytes.toBytes("EmployeesCafe"), Bytes.toBytes("department"), Bytes.toBytes(builder.getDepartment()));
                p.addColumn(Bytes.toBytes("EmployeesCafe"), Bytes.toBytes("salary"), Bytes.toBytes(salary));
                p.addColumn(Bytes.toBytes("EmployeesCafe"), Bytes.toBytes("floor_number"), Bytes.toBytes(floor));
                p.addColumn(Bytes.toBytes("EmployeesCafe"), Bytes.toBytes("cafeteria_code"), Bytes.toBytes(builder.getCafeteriaCode()));

                table.put(p);
                Thread.sleep(1000);
                rowNum++;

            table.close();
    } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
    public  static  void employeeDetails(String builderCode,String cafeteriaCode) throws Exception {

        Thread.sleep(1000);
        String localpath = "/Users/sagarsangam/Documents/inputDirs/Employees/employeeDetailsCafeteria.csv";
        FileReader fr = new FileReader(localpath);
        BufferedReader br = new BufferedReader(fr);  //creates a buffering character input stream
        StringBuffer sb = new StringBuffer();    //constructs a string buffer with no characters
        String line;
        //System.out.println(builderCode);

        builderCode= builderCode.substring(1);
      //  System.out.println("entered employeeDetails() method");

        while ((line = br.readLine()) != null)
        {
            String[] fields = line.split(",");
            //builder.setNumberOfFloors(Employee.NumberOfFloors.TEN_FLOORS);

            if(!(fields[3].charAt(0)>=48 && fields[3].charAt(0)<=57))
                continue;

            builder.setName(fields[0]);

            builder.setBuildingCode(fields[1]);


            builder.setDepartment(fields[2]);

            builder.setSalary(Integer.parseInt(fields[3]));

            floorsDetail(fields[4].charAt(0));

           // System.out.println("Building code is" + builder.getBuildingCode());
            if(builderCode.equals( fields[1]/*builder.getBuildingCode()*/))
            {
                System.out.println(builder.toString());

                builder.setCafeteriaCode(cafeteriaCode);
                updatingHbaseEmployeeCafeteriaTable();
            }

        }
        fr.close();
 }
    public  static  void findingEmployeeDetailsUsingBuilderCode(String buildercode,String cafeteriaCode) throws Exception {

        employeeDetails(buildercode,cafeteriaCode);
    }


    public  static  void buildingDetails() throws Exception {
        String localpath = "/Users/sagarsangam/Documents/inputDirs/Buildings/buildingDetails1.csv";
        FileReader fr = new FileReader(localpath);
        BufferedReader br = new BufferedReader(fr);  //creates a buffering character input stream
        StringBuffer sb = new StringBuffer();    //constructs a string buffer with no characters
        String line;
        while ((line = br.readLine()) != null)
        {
            String[] fields = line.split(",");

            Buildings.BuildingsData.Builder builder =  Buildings.BuildingsData.newBuilder();
            builder.setBuildingCode(fields[0]);
            builder.setTotalFloors(Integer.parseInt(fields[1]));
            builder.setCompaniesInBuilding(fields[2]);
            builder.setCafeteriaCode(fields[3]);

            builder.setCafeteriaCode(fields[3].substring(0,fields[3].length()-1));
           // System.out.println(builder.toString());

            findingEmployeeDetailsUsingBuilderCode(builder.getBuildingCode(),builder.getCafeteriaCode());
        }
        fr.close();    //closes the strea

    }
    public static void main(String[] args) throws Exception {

        buildingDetails();

    }
}
