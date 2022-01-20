package Assignment1;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class CreatingDirectory {

    private static final String uri = "hdfs://localhost:9000/Users/sagarsangam/Downloads/";
    private static final String directory = uri + "Assignments";
     static final Configuration config = new Configuration();

    public static void createDirectory() throws IOException {

        /* Get FileSystem object for given uri */
        FileSystem fs = FileSystem.get(URI.create(uri), config);
        boolean isCreated = fs.mkdirs(new Path(directory));

        if (isCreated) {
            System.out.println("Directory created");
        } else {
            System.out.println("Directory creation failed");
        }
    }

    public static void main(String args[]) throws IOException {
        createDirectory();
    }
}
