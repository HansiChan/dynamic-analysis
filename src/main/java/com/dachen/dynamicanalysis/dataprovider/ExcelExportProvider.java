package com.dachen.dynamicanalysis.dataprovider;

import com.dachen.dynamicanalysis.dto.Dimension;
import com.dachen.dynamicanalysis.dto.Index;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.springframework.stereotype.Repository;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.List;

@Repository
public class ExcelExportProvider {

    // 第一步，创建一个webbook，对应一个Excel文件
    public HSSFWorkbook generateExcel() {
        return new HSSFWorkbook();
    }

    public HSSFWorkbook generateSheet(String dimension, String charType, HSSFWorkbook wb, String sheetName,
                                      List<String> fields, List<String> firstCell, List<String> secondCell) {

        sheetName = Index.getName(sheetName);
        dimension = Dimension.getName(dimension);

        // 第二步，在webbook中添加一个sheet,对应Excel文件中的sheet
        HSSFSheet sheet = wb.createSheet(sheetName);
        // 第三步，在sheet中添加表头第0行,注意老版本poi对Excel的行数列数有限制short
        HSSFRow row = sheet.createRow(0);
        // 第四步，创建单元格，并设置值表头 设置表头居中
        HSSFCellStyle style = wb.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER); // 创建一个居中格式
        //设置表头字段名
        HSSFCell cell;
        if ("lines".equals(charType)) {
            for (int i = 0; i < fields.size(); i++) {
                cell = row.createCell(i);
                cell.setCellValue(fields.get(i));
                cell.setCellStyle(style);
            }
        } else {
            cell = row.createCell(0);
            cell.setCellValue(dimension);
            cell.setCellStyle(style);
            cell = row.createCell(1);
            cell.setCellValue("num");
            cell.setCellStyle(style);
        }


        for (int i = 0; i < firstCell.size(); i++) {
            row = sheet.createRow(i + 1);
            // 第五步，创建单元格，并设置值
            if ("lines".equals(charType)) {
                row.createCell(0).setCellValue(firstCell.get(i));
                for (int j = 0; j < secondCell.size(); j++) {
                    row.createCell(1 + j)
                            .setCellValue(Integer.parseInt(secondCell.get(j).replace("\"", "")
                                    .replace("[", "").replace("]", "").split(",")[i]));
                }

            } else {
                row.createCell(0).setCellValue(firstCell.get(i));
                row.createCell(1).setCellValue(Integer.parseInt(secondCell.get(i)));
            }
        }

        return wb;
    }

    public void export(HSSFWorkbook wb, HttpServletResponse response) {
        // 第六步，实现文件下载保存
        try {
            //response.setHeader("content-disposition", "attachment;filename="+ URLEncoder.encode("res", "UTF-8") + ".xls");
            String fileName = new String("人员数据.xls".getBytes(), "ISO-8859-1");
            response.setHeader("Content-Disposition", "attachment;filename=" + fileName);
            OutputStream out = response.getOutputStream();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            wb.write(baos);
            byte[] xlsBytes = baos.toByteArray();
            out.write(xlsBytes);
            baos.close();
            out.flush();
            out.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}