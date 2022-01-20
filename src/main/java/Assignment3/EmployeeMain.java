package Assignment3;

import java.io.*;

import com.github.javafaker.Faker;
import com.opencsv.CSVWriter;
import example.employees.*;
public class EmployeeMain {

    public static EmployeeModel storingEmployeeDetails()
    {

        Faker faker = new Faker();

        EmployeeModel employeeModel = new EmployeeModel();
        employeeModel.setName(faker.name().fullName());
        employeeModel.setBuilding_code(faker.address().zipCode());
        employeeModel.setDepartment(faker.company().industry());
        employeeModel.setSalary(faker.number().numberBetween(10000,25000));
        employeeModel.setFloor_number(faker.number().numberBetween(1,10));

        return employeeModel;

    }

    public static void insertingDetails() throws IOException {

        String filepath="/Users/sagarsangam/Documents/inputDirs/Employees";
        File file = new File(filepath,"employeeDetails"+".csv");
        FileWriter outputfile = new FileWriter(file);
        CSVWriter csvWriter = new CSVWriter(outputfile);

        //I am creating 100 files with each file containing the data as much number as the count of the filename.
        for(int i=1;i<=100;i++)
        {
            EmployeeModel build= storingEmployeeDetails();
            String[] data={build.getName()+","+build.getBuilding_code()+","+build.getDepartment() +","+build.getSalary().toString()+
            ","+build.getFloor_number().toString()};


            csvWriter.writeNext(data);

        }
        csvWriter.close();

    }

    public static void main(String[] args) throws IOException {

        insertingDetails();

    }

}





