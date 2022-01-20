package Assignment3;

public class BuildingModel {
   // String buildingcode= faker.address().zipCode();
//    Integer totalfloor=faker.number().numberBetween(2,10);
//    Integer companiesinbuilding=faker.number().numberBetween(2,10);
//    String cafeteriacode=faker.address().cityPrefix();

    private  String buildingCode;
    private Integer totalFloor;
    private  Integer companiesInBuilding;
    private String cafeteriaCode;

    public BuildingModel()
    {

    }

    public String getBuildingCode() {
        return buildingCode;
    }

    public void setBuildingCode(String buildingCode) {
        this.buildingCode = buildingCode;
    }

    public Integer getTotalFloor() {
        return totalFloor;
    }

    public void setTotalFloor(Integer totalFloor) {
        this.totalFloor = totalFloor;
    }

    public Integer getCompaniesInBuilding() {
        return companiesInBuilding;
    }

    public void setCompaniesInBuilding(Integer companiesInBuilding) {
        this.companiesInBuilding = companiesInBuilding;
    }

    public String getCafeteriaCode() {
        return cafeteriaCode;
    }

    public void setCafeteriaCode(String cafeteriaCode) {
        this.cafeteriaCode = cafeteriaCode;
    }
}
