package cn.com.boe.itc.mapSimilarity.service;

import cn.com.boe.itc.filter.ContourFilter;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;

@Service
public class ContourService {
    public LinkedHashMap<String, List> queryData(String lotId, String operationId, String paramName) {

        LinkedHashMap<String, List> dataList = null;
        try {
            dataList = ContourFilter.queryData(lotId, operationId, paramName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataList;

    }
}
