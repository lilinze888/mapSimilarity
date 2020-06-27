package cn.com.boe.itc.mapSimilarity.controller;


import cn.com.boe.itc.mapSimilarity.service.FilterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("filter")
public class FilterController {

    @Autowired
    private FilterService filterService;

    /**
     * 获取所有selection列表
     * @return selectionList
     */
    @GetMapping("selection")
//    @ApiOperation(value = "筛选器接口", notes = "按照“查询时间”筛选出的Model、ProductionType、EPM站点、 EPM参数、CD/RS/THK站点、 CD/RS/THK参数，做成下拉式筛选器")
    public ResponseEntity<Map<String, Set<String>>> querySelectionList() {

        Map<String, Set<String>> selectionList = this.filterService.querySelectionList();
        if (selectionList == null || selectionList.size() <= 0 ) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
        return ResponseEntity.ok(selectionList);
    }


//    /**
//     * 获取Model列表
//     * @return modelList
//     */
//    @GetMapping("model")
//    public ResponseEntity<List<String>> queryModelList() {
//        List<String> modelList = this.filterService.queryModelList();
//        if (modelList == null || modelList.size() <= 0 ) {
//            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
//        }
//        return ResponseEntity.ok(modelList);
//    }
//
//    /**
//     * 获取ProductionType列表
//     * @return productionTypeList
//     */
//    @GetMapping("productionType")
//    public ResponseEntity<List<String>> queryProductionTypeList() {
//        List<String> productionTypeList = this.filterService.queryProductionTypeList();
//        if (productionTypeList == null || productionTypeList.size() <= 0 ) {
//            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
//        }
//        return ResponseEntity.ok(productionTypeList);
//    }
//
//    /**
//     * 获取EPM站点列表
//     * @return epmOperationList
//     */
//    @GetMapping("epmOperation")
//    public ResponseEntity<List<String>> queryEpmOperationtList() {
//        List<String> epmOperationList = this.filterService.queryEpmOperationtList();
//        if (epmOperationList == null || epmOperationList.size() <= 0 ) {
//            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
//        }
//        return ResponseEntity.ok(epmOperationList);
//    }
//
//    /**
//     * 获取EPM参数列表
//     * @return epmParamList
//     */
//    @GetMapping("epmParam")
//    public ResponseEntity<List<String>> queryEpmParamtList() {
//        List<String> epmParamList = this.filterService.queryEpmParamtList();
//        if (epmParamList == null || epmParamList.size() <= 0 ) {
//            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
//        }
//        return ResponseEntity.ok(epmParamList);
//    }
//
//    /**
//     * 获取CD/RS/THK站点列表
//     * @return crtOperationlList
//     */
//    @GetMapping("crtOperation")
//    public ResponseEntity<List<String>> queryCrtOperationlList() {
//        List<String> crtOperationlList = this.filterService.queryCrtOperationlList();
//        if (crtOperationlList == null || crtOperationlList.size() <= 0 ) {
//            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
//        }
//        return ResponseEntity.ok(crtOperationlList);
//    }
//
//    /**
//     * 获取CD/RS/THK参数列表
//     * @return crtParamlList
//     */
//    @GetMapping("crtParam")
//    public ResponseEntity<List<String>> queryCartList() {
//        List<String> crtParamlList = this.filterService.queryCrtParamlListList();
//        if (crtParamlList == null || crtParamlList.size() <= 0 ) {
//            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
//        }
//        return ResponseEntity.ok(crtParamlList);
//    }

}
