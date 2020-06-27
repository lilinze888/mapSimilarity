package cn.com.boe.itc.mapSimilarity.controller;

import cn.com.boe.itc.mapSimilarity.service.LineService;
import cn.com.boe.itc.pojo.Selection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("line")
public class LineController {

    @Autowired
    LineService lineService;

    @PostMapping()
//    @ApiOperation(value = "相关系数折线接口", notes = "按照“查询时间”和指定的Model、ProductionType、EPM站点、 EPM参数、CD/RS/THK站点、 CD/RS/THK参数 查询ByLot的相关系数值，鼠标悬停时显示lot_id、相关系数值")
    public ResponseEntity<ArrayList<LinkedHashMap<String, String>>> queryLine1Data(@RequestBody Selection selection) {
        ArrayList<LinkedHashMap<String, String>> lineData = this.lineService.queryLineData( selection );
        if (lineData == null || lineData.size() <= 0) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
        return ResponseEntity.ok(lineData);
    }


/*    @PostMapping("2")
//    @ApiOperation(value = "CD/RS/THK的测量值折线接口", notes = "按照“查询时间”和指定的Model、ProductionType、CD/RS/THK站点、CD/RS/THK参数 查询ByGlass的均值，再计算出ByLot的均值，鼠标悬停时显示lot_id、ByLot均值")
    public ResponseEntity<ArrayList<LinkedHashMap<String, String>>> queryLine2Data(@RequestBody Selection selection) {
        ArrayList<LinkedHashMap<String, String>> lineData = this.lineService.queryLine2Data( selection );
        if (lineData == null || lineData.size() <= 0) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
        return ResponseEntity.ok(lineData);
    }*/



}
