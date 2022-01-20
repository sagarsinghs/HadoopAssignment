package Assignment1;

import com.github.javafaker.Faker;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private String name,company, building,address;

    Integer age ;

    public Main(String name, String company, String building, String address, Integer age) {
        this.name = name;
        this.company = company;
        this.building = building;
        this.address = address;
        this.age = age;
    }

    public static String[] creatingData()
    {
        Faker faker = new Faker();

        People people = new People();

        people.setName(faker.name().name());
        people.setAge(faker.number().numberBetween(19,69));
        people.setCompany(faker.company().name());
        people.setBuilding(faker.address().buildingNumber());
        people.setAddress(faker.address().fullAddress());


        String[] data = {people.getName()+","+people.getAge().toString()+","+people.getCompany()+","+people.getBuilding()+","+people.getAddress()};

        return  data;
    }
    public  static void insertingData() throws IOException {


        //I am creating 100 files with each file containing the data as much number as the count of the filename.
        for(int i=1;i<=100;i++) {



            String filepath="/Users/sagarsangam/Documents/inputDirs/Checking";
            //filepath=filepath.concat("/file"+Integer.toString(i)+".csv");
            File file = new File(filepath,"file"+Integer.toString(i)+".csv");
            FileWriter outputfile = new FileWriter(file);
            CSVWriter csvWriter = new CSVWriter(outputfile);
            List<String[]> data = new ArrayList<>();

            for (int j = 0; j <i; j++) {
                csvWriter.writeNext(creatingData());
            }


            csvWriter.close();
        }

    }
    public static void main(String[] args) throws IOException {
        insertingData();

    }


}
