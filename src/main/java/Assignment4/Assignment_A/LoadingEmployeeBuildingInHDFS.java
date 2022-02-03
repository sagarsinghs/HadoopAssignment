package Assignment4.Assignment_A;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class LoadingEmployeeBuildingInHDFS {

    String uri="hdfs://localhost:9000/";

    public static void loadingEmployeeInHDFS() throws IOException {
        String uri="hdfs://localhost:9000/";


            String localpath="/Users/sagarsangam/Documents/inputDirs/Employees/employeeDetails.csv";

            String hdfsDir="hdfs://localhost:9000/Users/sagarsangam/Downloads/Employees";
            Configuration conf =  new Configuration();
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            fs.copyFromLocalFile(new Path(localpath),new Path(hdfsDir));


    }
    public static void loadingBuildingInHDFS() throws IOException {

            String uri="hdfs://localhost:9000/";
            String localpath="/Users/sagarsangam/Documents/inputDirs/Buildings/buildingDetails1.csv";

            String hdfsDir="hdfs://localhost:9000/Users/sagarsangam/Downloads/Buildings";
            Configuration conf =  new Configuration();
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            fs.copyFromLocalFile(new Path(localpath),new Path(hdfsDir));
    }
    public static void main(String[] args) throws IOException {


        loadingEmployeeInHDFS();

        loadingBuildingInHDFS();
    }
}
