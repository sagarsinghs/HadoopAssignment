package Assignment3;

import com.github.javafaker.Faker;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BuildingMain {

    private String building_code;
    private int total_floor;
    private String companiesInTheBuilding;
    private String cafeteria_code;

    public static  BuildingModel storingBuildingDetails() {
        Faker faker = new Faker();

        BuildingModel buildingModel = new BuildingModel();
        buildingModel.setBuildingCode(faker.address().zipCode());
        buildingModel.setTotalFloor(faker.number().numberBetween(2, 10));
        buildingModel.setCompaniesInBuilding(faker.number().numberBetween(2, 10));
        buildingModel.setCafeteriaCode(faker.address().cityPrefix());

        return buildingModel;


    }
    public  static  void insertingDetails() throws IOException {


        String filepath="/Users/sagarsangam/Documents/inputDirs/Buildings";
        File file = new File(filepath,"buildingDetails"+Integer.toString(1)+".csv");
        FileWriter outputfile = new FileWriter(file);
        CSVWriter csvWriter = new CSVWriter(outputfile);

        //I am creating 100 files with each file containing the data as much number as the count of the filename.
        for(int i=1;i<=10;i++)
        {
            BuildingModel build= storingBuildingDetails();
            String[] data={build.getBuildingCode()+","+build.getTotalFloor().toString()+","+build.getCompaniesInBuilding().toString() +","+build.getBuildingCode()};


            csvWriter.writeNext(data);

        }
        csvWriter.close();
    }
    public static void main(String[] args) throws IOException {

        insertingDetails();

    }



}
