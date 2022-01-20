package Assignment3;

import com.github.javafaker.Faker;
import example.building.Buildings;
import example.employees.Employee;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

//import employee.Driver;


public class Driver {

    static  Employee.EmployeeDetails.Builder builder = Employee.EmployeeDetails.newBuilder();

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
    public  static  void employeeDetails() throws IOException {

            String localpath = "/Users/sagarsangam/Documents/inputDirs/Employees/employeeDetails.csv";
            FileReader fr = new FileReader(localpath);
            BufferedReader br = new BufferedReader(fr);  //creates a buffering character input stream
            StringBuffer sb = new StringBuffer();    //constructs a string buffer with no characters
            String line;
            while ((line = br.readLine()) != null)
            {
                String[] fields = line.split(",");


                //builder.setNumberOfFloors(Employee.NumberOfFloors.TEN_FLOORS);
                builder.setName(fields[0]);
                builder.setBuildingCode(fields[1]);
                builder.setDepartment(fields[2]);
                if(!(fields[3].charAt(0)>=48 && fields[3].charAt(0)<=57))
                    continue;

                builder.setSalary(Integer.parseInt(fields[3]));

                floorsDetail(fields[4].charAt(0));

                System.out.println(builder.toString());
            }
            fr.close();    //closes the strea



    }
    public  static  void buildingDetails() throws IOException {
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

            System.out.println(builder.toString());
        }
        fr.close();    //closes the strea

    }
    public static void main(String[] args) throws IOException {

        System.out.println("Enter your Choice\n 1. To see the Employee Details \n 2.To see the Building Details");
        Scanner in = new Scanner(System.in);
        System.out.println("Please enter your value: ");
        String msg = in.nextLine();
        int option = Integer.valueOf(msg);

        switch (option)
        {
            case 1: buildingDetails();
                     break;

            case 2:

                employeeDetails();
                break;

            default :
                System.out.println("Wrong Choice");

        }
    }
}
