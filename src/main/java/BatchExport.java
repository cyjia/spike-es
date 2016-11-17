import ia.es.EsClient;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.WorkbookUtil;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BatchExport {

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        EsClient esClient = new EsClient();
        Workbook wb = new HSSFWorkbook();
        Sheet sheet1 = wb.createSheet(WorkbookUtil.createSafeSheetName("Sheet1"));
        final List<String> columns = new ArrayList<>();
        esClient.searchScroll("prddoview", 65000, (rowNum, hit) -> {
            Map<String, Object> source = hit.getSource();
            if (columns.size() == 0) {
                columns.addAll(source.keySet());
                Row headerRow = sheet1.createRow(0);
                for (int i=0; i<columns.size(); i++) {
                    Cell cell = headerRow.createCell(i);
                    cell.setCellValue(columns.get(i));
                }
            }

            Row row = sheet1.createRow(rowNum + 1);
            for (int i = 0; i < columns.size(); i++) {
                Cell cell = row.createCell(i);
                Object cellValue = source.getOrDefault(columns.get(i), "");
                cell.setCellValue(cellValue == null ? "" : cellValue.toString());
            }
        });
        FileOutputStream fileOut = new FileOutputStream("/tmp/workbook.xls");
        wb.write(fileOut);
        fileOut.close();
        System.out.println("Time took:" + (System.currentTimeMillis() - start));
    }

}
