package cn.com.boe.itc.mapSimilarity.controller;


import cn.com.boe.itc.mapSimilarity.service.ContourService;
import cn.com.boe.itc.pojo.Selection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("contour")
public class ContourController {

    @Autowired
    private ContourService contourService;

    @GetMapping("epm/{lotId}")
//    @ApiOperation(value = "epm站点的等高线图接口", notes = "按照“查询时间”和指定的EPM站点、EPM参数 查询得到")
    public ResponseEntity<LinkedHashMap<String, List>> queryEpmData(
            @PathVariable("lotId") String lotId,
            @RequestParam("operation_id_epm") String operationId,
            @RequestParam("param_name_epm") String paramName
    ) {
        LinkedHashMap<String, List> epmData = this.contourService.queryData(lotId, operationId, paramName  );
        if (epmData == null || epmData.size() <= 0) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
        return ResponseEntity.ok(epmData);
    }

    @GetMapping("crt/{lotId}")
//    @ApiOperation(value = "CD/RS/THK站点的等高线图接口", notes = "按照“查询时间”和指定的CD/RS/THK站点、CD/RS/THK参数 查询得到")
    public ResponseEntity<LinkedHashMap<String, List>> queryCrtData(
            @PathVariable("lotId") String lotId,
            @RequestParam("operation_id_crt") String operationId,
            @RequestParam("param_name_crt") String paramName
    ) {
        LinkedHashMap<String, List> crtData = this.contourService.queryData(lotId, operationId, paramName  );
        if (crtData == null || crtData.size() <= 0) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
        return ResponseEntity.ok(crtData);
    }

}
