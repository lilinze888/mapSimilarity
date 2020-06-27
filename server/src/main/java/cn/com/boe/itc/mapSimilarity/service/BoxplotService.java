package cn.com.boe.itc.mapSimilarity.service;

import cn.com.boe.itc.filter.BoxplotFilter;
import cn.com.boe.itc.pojo.Selection;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class BoxplotService {

    public ArrayList<LinkedHashMap<String, String>> queryBoxplotData(Selection selection, String dateFlag) {

        ArrayList<LinkedHashMap<String, String>> BoxplotData = null;
        try {
            BoxplotData = BoxplotFilter.queryBoxplotData(selection, dateFlag);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return BoxplotData;
    }
}
