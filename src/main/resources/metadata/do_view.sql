
select
    hu.huId as id,
    hu.huId as huId,
    hu.sscc as sscc,
    hu.dlvryNum as deliveryNum,
    hu.dlvryItemNum as deliveryItemNum,
    hu.netWeight as netWeight,
    hu.weightUnit as weightUnit,
    hu.totalWeight as totalWeight,
    hu.weightUnitTare as weightUnitTare,
    hu.length as length,
    hu.width as width,
    hu.height as height,
    hu.unitOfDimension as unitOfDimension,
    hu.totalVolume as totalVolume,
    hu.volumeUnit as volumeUnit,
    hu.tareVolume as tareVolume,
    hu.volumeUnitTare as volumeUnitTare,
    hu.packedQty as packedQuantity,
    hu.materialNumber as materialNumber,
    hu.idocNum as huIdocNum,
    hu.idocCreatedDate as huIdocCreatedDate,
    hu.lastUpdateDate as huLastUpdateDate,
    hu.calcFlag as huCalcFlag,
    hu.operFlag as huOperFlag,
    hu.prdType as productType,
    doh.dlvryItemQty as deliveryItemQuantity,
doh.sosDlvryNum as soDeliveryNum,
doh.sosOrderNum as soOrderNum,
doh.prtlShip as prtlShip,
doh.carrCode as carrierCode,
doh.carrName as carrierName,
doh.actlGoodsIssueDate as actualGoodsIssueDate,
doh.carrPhnDesc1 as carrierPhoneDesc1,
doh.carrPhnDesc2 as carrierPhoneDesc2,
doh.carrPhnNum1 as carrierPhoneNum1,
doh.carrPhnNum2 as carrierPhoneNum2,
doh.carrPickupDate as carrierPickupDate,
doh.slsOrderNum as soOrderNum,
doh.modeOfTrspn as modeOfTransport,
doh.rte as rte,
doh.shipToCtryCd as shipToCountryCode,
doh.shipToCtryNm as shipToCountryName,
doh.ctOrderEntToShipDateBsns as ctOrderEntToShipDateBsns,
doh.ctOrderEntToShipDateCal as ctOrderEntToShipDateCal,
doh.ctOrderRcptToShipDateBsns as ctOrderRcptToShipDateBsns,
doh.ctOrderRcptToShipDateCal as ctOrderRcptToShipDateCal,
doh.transtTm as transtTm,
doh.shpngCode as shippingCode,
doh.shpngSrc as shippingSource,
doh.createdDate as dohCreatedDate,
doh.modifiedDate as dohModifiedDate,
doh.msgType as dohMessageType,
doh.estArrDate as estimatedArriveDate,
doh.idocNum as dohIdocNum,
doh.idocCreatedDate as dohIdocCreatedDate,
doh.lastUpdateDate as dohLastUpdateDate,
doh.deptDate as departureDate,
doh.podEntryDate as podEntryDate,
doh.cdd as dohCdd,
doh.podDate as podDate,
doh.orderMilestone as orderMilestone,
doh.orderMilestoneName as orderMilestoneName,
doh.subMilestone as subMilestone,
doh.subMilestoneName as subMilestoneName,
doh.dk01Color as dk01Color,
doh.dk02Color as dk02Color,
doh.dk03Color as dk03Color,
doh.dk04Color as dk04Color,
doh.dk05Color as dk05Color,
doh.allStatusDttm as allStatusTime,
doh.allStatusReason as allStatusReason,
doh.handOverDate as handOverDate,
doh.packDate as packDate,
doh.pickDate as pickDate,
doh.bol as billOfLading,
doh.carrPickUpTm as carrierPickUpTime,
doh.netWeight as dohNetWeight,
doh.totalWeight as dohTotalWeight,
doh.volumeWeight as dohVolumeWeight,
doi.prodId as productId,
doi.dlvrdQty as deliveredQuantity,
doi.slsOrderLineNum as salesOrderLineNum,
doi.createdDate as doiCreatedDate,
doi.modifiedDate as doiModifiedDate,
doi.idocNum as doiIdocNum,
doi.idocCreatedDate as doiIdocCreatedDate,
doi.lastUpdateDate as doiLastUpdateDate,
doi.convertFlag as doiConvertFlag
from handlingunitcsl hu
    LEFT JOIN deliveryOdrHeaderBiz doh on (hu.dlvryNum = doh.dlvryNum)
LEFT JOIN deliveryOdrItemBiz doi on (hu.dlvryNum = doi.dlvryNum and hu.dlvryItemNum = doi.dlvryItemNum);
