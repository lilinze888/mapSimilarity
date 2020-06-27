package cn.com.boe.itc.mapSimilarity.service;


import cn.com.boe.itc.pojo.BpmapCorrelation;
import org.springframework.stereotype.Service;
import cn.com.boe.itc.filter.SelectionFilter;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class FilterService {

    private String tableName = BpmapCorrelation.TABLE_NAME;


    public Map<String, Set<String>> querySelectionList() {
        Map<String, Set<String>> selectionList = null ;
        try {
            selectionList = SelectionFilter.getSelectionList(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return selectionList;
    }

    public List<String> queryModelList() {

        List<String> modelList = null ;
        try {
            modelList = SelectionFilter.getSelectionList(tableName, BpmapCorrelation.MODEL);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return modelList;
    }

    public List<String> queryProductionTypeList() {

        return null;
    }

    public List<String> queryEpmOperationtList() {

        return null;
    }

    public List<String> queryEpmParamtList() {

        return null;
    }

    public List<String> queryCrtOperationlList() {

        return null;
    }

    public List<String> queryCrtParamlListList() {

        return null;
    }

}
