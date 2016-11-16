import ia.es.ESMetadata;
import ia.es.EsClient;
import ia.jdbc.util.DB;
import ia.jdbc.util.DBMetadata;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImportToEs {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new RuntimeException("No tableName provided");
        }
        final String tableName = "doview";
        final String sql = "select\n" +
                "    hu.huId as id,\n" +
                "    hu.huId as huId,\n" +
                "    hu.sscc as sscc,\n" +
                "    hu.dlvryNum as deliveryNum,\n" +
                "    hu.dlvryItemNum as deliveryItemNum,\n" +
                "    hu.netWeight as netWeight,\n" +
                "    hu.weightUnit as weightUnit,\n" +
                "    hu.totalWeight as totalWeight,\n" +
                "    hu.weightUnitTare as weightUnitTare,\n" +
                "    hu.length as length,\n" +
                "    hu.width as width,\n" +
                "    hu.height as height,\n" +
                "    hu.unitOfDimension as unitOfDimension,\n" +
                "    hu.totalVolume as totalVolume,\n" +
                "    hu.volumeUnit as volumeUnit,\n" +
                "    hu.tareVolume as tareVolume,\n" +
                "    hu.volumeUnitTare as volumeUnitTare,\n" +
                "    hu.packedQty as packedQuantity,\n" +
                "    hu.materialNumber as materialNumber,\n" +
                "    hu.idocNum as huIdocNum,\n" +
                "    hu.idocCreatedDate as huIdocCreatedDate,\n" +
                "    hu.lastUpdateDate as huLastUpdateDate,\n" +
                "    hu.calcFlag as huCalcFlag,\n" +
                "    hu.operFlag as huOperFlag,\n" +
                "    hu.prdType as productType,\n" +
                "    doh.dlvryItemQty as deliveryItemQuantity,\n" +
                "doh.sosDlvryNum as soDeliveryNum,\n" +
                "doh.sosOrderNum as soOrderNum,\n" +
                "doh.prtlShip as prtlShip,\n" +
                "doh.carrCode as carrierCode,\n" +
                "doh.carrName as carrierName,\n" +
                "doh.actlGoodsIssueDate as actualGoodsIssueDate,\n" +
                "doh.carrPhnDesc1 as carrierPhoneDesc1,\n" +
                "doh.carrPhnDesc2 as carrierPhoneDesc2,\n" +
                "doh.carrPhnNum1 as carrierPhoneNum1,\n" +
                "doh.carrPhnNum2 as carrierPhoneNum2,\n" +
                "doh.carrPickupDate as carrierPickupDate,\n" +
                "doh.slsOrderNum as soOrderNum,\n" +
                "doh.modeOfTrspn as modeOfTransport,\n" +
                "doh.rte as rte,\n" +
                "doh.shipToCtryCd as shipToCountryCode,\n" +
                "doh.shipToCtryNm as shipToCountryName,\n" +
                "doh.ctOrderEntToShipDateBsns as ctOrderEntToShipDateBsns,\n" +
                "doh.ctOrderEntToShipDateCal as ctOrderEntToShipDateCal,\n" +
                "doh.ctOrderRcptToShipDateBsns as ctOrderRcptToShipDateBsns,\n" +
                "doh.ctOrderRcptToShipDateCal as ctOrderRcptToShipDateCal,\n" +
                "doh.transtTm as transtTm,\n" +
                "doh.shpngCode as shippingCode,\n" +
                "doh.shpngSrc as shippingSource,\n" +
                "doh.createdDate as dohCreatedDate,\n" +
                "doh.modifiedDate as dohModifiedDate,\n" +
                "doh.msgType as dohMessageType,\n" +
                "doh.estArrDate as estimatedArriveDate,\n" +
                "doh.idocNum as dohIdocNum,\n" +
                "doh.idocCreatedDate as dohIdocCreatedDate,\n" +
                "doh.lastUpdateDate as dohLastUpdateDate,\n" +
                "doh.deptDate as departureDate,\n" +
                "doh.podEntryDate as podEntryDate,\n" +
                "doh.cdd as dohCdd,\n" +
                "doh.podDate as podDate,\n" +
                "doh.orderMilestone as orderMilestone,\n" +
                "doh.orderMilestoneName as orderMilestoneName,\n" +
                "doh.subMilestone as subMilestone,\n" +
                "doh.subMilestoneName as subMilestoneName,\n" +
                "doh.dk01Color as dk01Color,\n" +
                "doh.dk02Color as dk02Color,\n" +
                "doh.dk03Color as dk03Color,\n" +
                "doh.dk04Color as dk04Color,\n" +
                "doh.dk05Color as dk05Color,\n" +
                "doh.allStatusDttm as allStatusTime,\n" +
                "doh.allStatusReason as allStatusReason,\n" +
                "doh.handOverDate as handOverDate,\n" +
                "doh.packDate as packDate,\n" +
                "doh.pickDate as pickDate,\n" +
                "doh.bol as billOfLading,\n" +
                "doh.carrPickUpTm as carrierPickUpTime,\n" +
                "doh.netWeight as dohNetWeight,\n" +
                "doh.totalWeight as dohTotalWeight,\n" +
                "doh.volumeWeight as dohVolumeWeight,\n" +
                "doi.prodId as productId,\n" +
                "doi.dlvrdQty as deliveredQuantity,\n" +
                "doi.slsOrderLineNum as salesOrderLineNum,\n" +
                "doi.createdDate as doiCreatedDate,\n" +
                "doi.modifiedDate as doiModifiedDate,\n" +
                "doi.idocNum as doiIdocNum,\n" +
                "doi.idocCreatedDate as doiIdocCreatedDate,\n" +
                "doi.lastUpdateDate as doiLastUpdateDate,\n" +
                "doi.convertFlag as doiConvertFlag\n" +
                "from handlingunitcsl hu\n" +
                "    LEFT JOIN deliveryOdrHeaderBiz doh on (hu.dlvryNum = doh.dlvryNum)\n" +
                "LEFT JOIN deliveryOdrItemBiz doi on (hu.dlvryNum = doi.dlvryNum and hu.dlvryItemNum = doi.dlvryItemNum)";
        final DB db = new DB();
        long pageNum = 0;
        long pageSize = 50000;
        long successCount = importFromView(db, paginateSql(sql, pageNum, pageSize), tableName);
        while (successCount > 0) {
            pageNum++;
            successCount = importFromView(db, paginateSql(sql, pageNum, pageSize), tableName);
        }
    }

    private static String paginateSql(String sql, long pageNum, long pageSize) {
        return String.format("%s WHERE hu.id >= %d and hu.id < %d", sql, pageNum * pageSize, (pageNum + 1) * pageSize);
    }

    private static Long importFromView(DB db, String sql, String typeName) throws Exception {
        return db.query(sql, rs -> {
            try {
                ResultSetMetaData metaData = rs.getMetaData();
                DBMetadata.Table table = DBMetadata.buildTable(metaData, typeName);
                String mapping = ESMetadata.getMapping(table);
                EsClient esClient = new EsClient();
                String index = "prd" + table.name.toLowerCase();
                if (!esClient.isIndexExist(index)) {
                    CreateIndexResponse response = esClient.createIndex(index, table.name, mapping);
                }
                return resultSetToESIndex(table, esClient, index, rs);
            } catch (SQLException e) {
                throw new RuntimeException("Error get metadata from result");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static long resultSetToESIndex(DBMetadata.Table t, EsClient esClient, String index, ResultSet resultSet) {
        try {
            final List<Map<String, Object>> bulkList = new ArrayList<>();
            long count = 0;
            while (resultSet.next()) {
                Map<String, Object> fields = new HashMap<>();
                for (DBMetadata.Column c : t.columns) {
                    fields.put(c.name, resultSet.getObject(c.name));
                }
                bulkList.add(fields);
                count++;
                if (bulkList.size() == 10000) {
                    System.out.println("processing 10000 docs");
                    esClient.bulkIndexDocuments(index, t.name, bulkList);
                    System.out.println("finished 10000 docs");
                    bulkList.clear();
                }
            }
            if (bulkList.size() > 0) {
                esClient.bulkIndexDocuments(index, t.name, bulkList);
                bulkList.clear();
            }
            return count;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
