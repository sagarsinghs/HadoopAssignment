package Assignment1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kerby.config.Conf;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class LoadHbase {

    public  static  FileSystem fs;
    public  static  String uri="hdfs://localhost:9000/";
    public  static  String dir="hdfs://localhost:9000/Users/sagarsangam/Downloads/Assignments/";
    public  static Path[] filePaths;

    public  static  void readPath() throws IOException {
        Configuration configuration = new Configuration();
        fs= FileSystem.get(URI.create(uri),configuration);

        FileStatus[]  fileStatus =  fs.listStatus(new Path(dir));
        Path[] paths = FileUtil.stat2Paths(fileStatus);

        filePaths=paths;
    }

    public static void insertingDetails() throws IOException {

        try{

            //The configuration object contains all Hadoop settings necessary to launch your app.
            // It's in key value format and is read from the xml files from /etc/hadoop
            Configuration conf = HBaseConfiguration.create();

            //Firstly, we need to create a connection to the database and get admin object, which we will use for manipulating a database structure
            Connection connection = ConnectionFactory.createConnection(conf);


            // Verifying the existance of the table
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            Table table ;
            if( admin.tableExists(TableName.valueOf("people")) ){
                //Table Exist so just create connection
                System.out.println("Table Already Exists No Need To Create");
                table = connection.getTable(TableName.valueOf("people"));
            }else{

                //Create Table
                // Instantiating table descriptor class
                //Then, we can create a table by passing an instance of the HTableDescriptor class to a createTable() method on the admin object
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("people"));

                // Adding column families to table descriptor
                //Name, age, company, building_code, phone_number, address
                tableDescriptor.addFamily(new HColumnDescriptor("peoples"));

                tableDescriptor.addFamily(new HColumnDescriptor("name"));
                tableDescriptor.addFamily(new HColumnDescriptor("age"));
                tableDescriptor.addFamily(new HColumnDescriptor("company"));
                tableDescriptor.addFamily(new HColumnDescriptor("building_code"));
                tableDescriptor.addFamily(new HColumnDescriptor("phone_number"));
                tableDescriptor.addFamily(new HColumnDescriptor("address"));

                System.out.println("creating table... ");

                admin.createTable(tableDescriptor);
                System.out.println("Done! ");
                table = connection.getTable(TableName.valueOf("people"));}
                int rowNum = 0;

                for(Path path : filePaths){
                 //   System.out.println(path.toString() +" " +  path.depth());
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
                    String line;
                    line=br.readLine();
                    if(path.toString().contains(".csv") == false) {
                        continue;
                    }

                    //this variable is to cross check the number of lines are there in the file.the number if lines present in each file is equal to each file number in HDFS.
                    int count=0;

                    while(line !=null){

                            String[] fields = line.split(",");
                            Put p = new Put(Bytes.toBytes("row" + rowNum));
                            count++;
                            p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("name"), Bytes.toBytes(fields[0]));
                            p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("age"), Bytes.toBytes(fields[1]));
                            p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("company"), Bytes.toBytes(fields[2]));
                            p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("building_code"), Bytes.toBytes(fields[3]));
                            p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("phone_number"), Bytes.toBytes(fields[4]));
                            p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("address"), Bytes.toBytes(fields[5]));
                            table.put(p);

                            rowNum++;
                            line=br.readLine();
                    }
                    System.out.println("Count is " + count);
                    count=0;
                }
                table.close();
                connection.close();
                fs.close();


            }catch (Exception ex){
                ex.printStackTrace();
            }

    }
    public static void main(String[] args) throws IOException {
        //Creating the Config file

        readPath();
        insertingDetails();


    }
}

