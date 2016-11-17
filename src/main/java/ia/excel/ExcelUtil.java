package ia.excel;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.WorkbookUtil;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ExcelUtil {

    public static void create(List<Map<String, Object>> table, List<String> columnNames) throws IOException {
        Workbook wb = new HSSFWorkbook();
        Sheet sheet1 = wb.createSheet(WorkbookUtil.createSafeSheetName("Sheet1"));
        int rowNum = 0;
        for (Map<String, Object> r : table) {
            Row row = sheet1.createRow(rowNum);
            int colNum = 0;
            for (String col : columnNames) {
                Cell cell = row.createCell(colNum);
                cell.setCellValue(r.getOrDefault(col, "").toString());
                colNum++;
            }
            rowNum++;
        }
        FileOutputStream fileOut = new FileOutputStream("/Users/CYJIA/Desktop/workbook.xlsx");
        wb.write(fileOut);
        fileOut.close();
    }
}
