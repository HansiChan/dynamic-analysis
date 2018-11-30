package com.dachen.dynamicanalysis.service;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import com.dachen.dynamicanalysis.dataprovider.ExcelExportProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletResponse;
import java.util.List;

@Service
public class ExcelExportService {

    @Autowired
    ExcelExportProvider excelProvider = new ExcelExportProvider();

    public boolean export(String dimension, String charType, List<String> feilds, String sheetName, List<String> firstCell, List<String> secondCell, HttpServletResponse response) {
        try {
            HSSFWorkbook wb = excelProvider.generateExcel();
            wb = excelProvider.generateSheet(dimension, charType, wb, sheetName, feilds, firstCell, secondCell);

            excelProvider.export(wb, response);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
