package Assignment3;

public class EmployeeModel {
    // employee -  name, employee_id,building_code,floor_number(enum),salary,department

//    String employee_id=emp.idNumber().toString();
//    String building_code=emp.address().zipCode();
//    Integer floor_number=emp.number().numberBetween(2,10);
//    Integer salary=emp.number().numberBetween(20000,30000);
//    String department=emp.company().industry();

    private String name;
    private String building_code;
    private Integer floor_number;
    private Integer salary;
    private String department;

    public EmployeeModel()
    {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBuilding_code() {
        return building_code;
    }

    public void setBuilding_code(String building_code) {
        this.building_code = building_code;
    }

    public Integer getFloor_number() {
        return floor_number;
    }

    public void setFloor_number(Integer floor_number) {
        this.floor_number = floor_number;
    }

    public Integer getSalary() {
        return salary;
    }

    public void setSalary(Integer salary) {
        this.salary = salary;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }
}
