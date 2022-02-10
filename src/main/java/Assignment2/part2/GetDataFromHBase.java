package Assignment2.part2;

import com.jcraft.jsch.IO;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

public class GetDataFromHBase {
    public final static String outputFilePath = "/Users/Users/sagarsangam/Downloads/directory1/outputinTxt.txt";

    public static void getDataFromHBase(Configuration configuration,HBaseAdmin admin,Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf("people"));
        Scan scan = new Scan();
        ResultScanner resultScanner = null;

        scan.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("age"));
        scan.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("company"));
        scan.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("building_code"));
        scan.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("phone_number"));
        scan.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("address"));

        resultScanner = table.getScanner(scan);
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath));

        for(Result result= resultScanner.next(); result != null; result = resultScanner.next()) {
            byte[] name = result.getValue(Bytes.toBytes("peoples"), Bytes.toBytes("name"));
            byte[] ag = result.getValue(Bytes.toBytes("peopkles"),Bytes.toBytes("age"));
            byte[] company = result.getValue(Bytes.toBytes("peoples"),Bytes.toBytes("company"));
            byte[] building_code = result.getValue(Bytes.toBytes("peoples"),Bytes.toBytes("building_code"));
            byte[] phone = result.getValue(Bytes.toBytes("peoples"),Bytes.toBytes("phone_number"));
            byte[] address = result.getValue(Bytes.toBytes("peoples"), Bytes.toBytes("address"));

            String Fname = Bytes.toString(name);
            String age = Bytes.toString(ag);
            String Company = Bytes.toString(company);
            String code = Bytes.toString(building_code);
            String no = Bytes.toString(phone);
            String Adddrss = Bytes.toString(address);

            String lines = Fname+","+age+","+Company+","+code+","+no+","+Adddrss;
            writer.write(lines);

            System.out.println(Fname +" " +age +" "+Company+" "+code+" "+no+" "+Adddrss);
        }
        writer.close();
    }

    public static void main(String args[]) throws IOException,InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);

        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        Table table;
        if(admin.tableExists(TableName.valueOf("people"))) {
            getDataFromHBase(configuration,admin,connection);
        }
        else {
            System.out.println("people data table does not exist");
        }
        connection.close();
    }
}