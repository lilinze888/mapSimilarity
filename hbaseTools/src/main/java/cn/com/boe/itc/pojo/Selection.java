package cn.com.boe.itc.pojo;

public class Selection {

    private String model ;

    private String productiontype ;

    private String operation_id_epm ;

    private String param_name_epm ;

    private String operation_id_crt ;

    private String param_name_crt ;

    private String querytime;

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getProductiontype() {
        return productiontype;
    }

    public void setProductiontype(String productiontype) {
        this.productiontype = productiontype;
    }

    public String getOperation_id_epm() {
        return operation_id_epm;
    }

    public void setOperation_id_epm(String operation_id_epm) {
        this.operation_id_epm = operation_id_epm;
    }

    public String getParam_name_epm() {
        return param_name_epm;
    }

    public void setParam_name_epm(String param_name_epm) {
        this.param_name_epm = param_name_epm;
    }

    public String getOperation_id_crt() {
        return operation_id_crt;
    }

    public void setOperation_id_crt(String operation_id_crt) {
        this.operation_id_crt = operation_id_crt;
    }

    public String getParam_name_crt() {
        return param_name_crt;
    }

    public void setParam_name_crt(String param_name_crt) {
        this.param_name_crt = param_name_crt;
    }

    public String getQuerytime() {
        return querytime;
    }

    public void setQuerytime(String querytime) {
        this.querytime = querytime;
    }

    @Override
    public String toString() {
        return "Selection{" +
                "model='" + model + '\'' +
                ", productiontype='" + productiontype + '\'' +
                ", operation_id_epm='" + operation_id_epm + '\'' +
                ", param_name_epm='" + param_name_epm + '\'' +
                ", operation_id_crt='" + operation_id_crt + '\'' +
                ", param_name_crt='" + param_name_crt + '\'' +
                ", querytime='" + querytime + '\'' +
                '}';
    }
}
