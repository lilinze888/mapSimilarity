package cn.com.boe.itc.mapSimilarity.controller;

import cn.com.boe.itc.mapSimilarity.service.BoxplotService;
import cn.com.boe.itc.pojo.Selection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("boxplot")
public class BoxplotController {

    @Autowired
    private BoxplotService boxplotService;

    /**
     * 获取盒须图需要的数据
     *
     * @return boxplotList
     */
    @PostMapping("{dateFlag}")
//    @ApiOperation(value = "箱线趋势图接口", notes = "按照“查询时间”和指定的Model、ProductionType、EPM站点、参数、CD/RS/THK站点、CD/RS/THK参数 查询得ByGlass的均值，按天、周、月的间隔绘制箱线图")
    public ResponseEntity<ArrayList<LinkedHashMap<String, String>>> queryBoxplotData(
            @PathVariable("dateFlag") String dateFlag,
            @RequestBody Selection selection) {
        ArrayList<LinkedHashMap<String, String>> boxplotList = this.boxplotService.queryBoxplotData( selection , dateFlag);
        if (boxplotList == null || boxplotList.size() <= 0) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
        return ResponseEntity.ok(boxplotList);
    }

}
