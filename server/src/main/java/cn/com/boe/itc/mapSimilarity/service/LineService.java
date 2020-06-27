package cn.com.boe.itc.mapSimilarity.service;

import cn.com.boe.itc.filter.LineFilter;
import cn.com.boe.itc.pojo.Selection;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;

@Service
public class LineService {


    public ArrayList<LinkedHashMap<String, String>> queryLineData(Selection selection) {
        ArrayList<LinkedHashMap<String, String>> lineData = null;
        try {
            lineData = LineFilter.queryLineData(selection);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return lineData;
    }

/*    public ArrayList<LinkedHashMap<String,String>> queryLine2Data(Selection selection) {
        ArrayList<LinkedHashMap<String, String>> lineData = null;
        try {
            lineData = LineFilter.queryLine2Data(selection);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return lineData;
    }*/
}
