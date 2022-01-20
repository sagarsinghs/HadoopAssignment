package Assignment1;

import com.opencsv.CSVWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class WritingInHDFS {
    public static void main(String[] args) throws IOException {

        String uri="hdfs://localhost:9000/";

        for(int i=1;i<=100;i++) {

            String localpath="/Users/sagarsangam/Documents/inputDirs/File/file"+Integer.toString(i)+".csv";
            System.out.println(localpath);

            String hdfsDir="hdfs://localhost:9000/Users/sagarsangam/Downloads/Assignments/";
            Configuration conf =  new Configuration();
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            fs.copyFromLocalFile(new Path(localpath),new Path(hdfsDir));


        }

    }
}
