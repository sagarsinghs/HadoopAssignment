package Assignment1;

public class People {
        private String name,company, building,address;
        private   Integer age ;

        public People()
        {

        }

        public void setName(String name) {
                this.name = name;
        }

        public void setCompany(String company) {
                this.company = company;
        }

        public void setBuilding(String building) {
                this.building = building;
        }

        public void setAddress(String address) {
                this.address = address;
        }

        public void setAge(Integer age) {
                this.age = age;
        }

        public String getName() {
                return name;
        }

        public String getCompany() {
                return company;
        }

        public String getBuilding() {
                return building;
        }

        public String getAddress() {
                return address;
        }

        public Integer getAge() {
                return age;
        }
}
