from shared.Common.Util import *
from shared.Common.Db import *

def readSAPMaterialMaster(plant, partNum):		
	parameters = {
		'plant': plant, 
		'part_num': partNum
	}
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("ReadSAPMaterialMaster", connection)
	results = getSPResults(storedProc, parameters, {}, {})
	
	return results

	
def readSAPBOM(plant, partNum):		
	parameters = {
		'plant': plant, 
		'part_num': partNum
	}
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("ReadSAPBillOfMaterial", connection)
	results = getSPResults(storedProc, parameters, {}, {})
	
	return results


def readSAPBOMItems(bomNum, bomAlternative):		
	parameters = {
		'bom_num': bomNum,
		'bom_alternative': bomAlternative
	}
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("ReadSAPBOMItems", connection)
	results = getSPResults(storedProc, parameters, {}, {})
	
	return results
	
		
def readSAPBOMTreeItems(bomNum, bomAlternative, dateChangeWindow = 0):		
	parameters = {
		'bom_num': bomNum,
		'bom_alternative': bomAlternative,
		'date_change_window': dateChangeWindow
	}
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("ReadSAPBOMTreeItems", connection)
	results = getSPResults(storedProc, parameters, {}, {})
	
	return results

		
def readSAPBOMTreeItemsByPN(plant, partNum, dateChangeWindow = 0):		
	parameters = {
		'plant': plant,
		'part_num': partNum,
		'date_change_window': dateChangeWindow
	}
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("ReadSAPBOMTreeItemsByPN", connection)
	results = getSPResults(storedProc, parameters, {}, {})
	
	return results
	
def readDeleteBOMItemAndBOM():
	"Method checks the SAPBillOfMaterial and SAPBOMItem for duplicates, deletes the duplicates, and sends an email alert"
	
	query = """
				SELECT BOM.[PART_NUM],BOM.[BOM_NUM] 
				FROM [dbo].[SAPBillOfMaterial] BOM
				JOIN [dbo].[SAPBOMItem] BOM1 ON BOM1.[PART_NUM] = BOM.[PART_NUM] AND BOM1.[BOM_NUM] = BOM.[BOM_NUM]
			"""
	database = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	result = system.db.runQuery(query,database)
	if result.getRowCount() > 0:
		for row in range(result.getRowCount()):
			part_num = result.getValueAt(row,'PART_NUM')
			bom_num = result.getValueAt(row,'BOM_NUM')
			
			query = """
						DELETE [dbo].[SAPBillOfMaterial]
						WHERE [PART_NUM] = ?
						AND BOM_NUM = ?
					"""
			system.db.runPrepQuery(query,[part_num,bom_num],database)
			
			query = """
						DELETE [dbo].[SAPBOMItem]
						WHERE [PART_NUM] = ?
						AND BOM_NUM = ?
					"""
			system.db.runPrepQuery(query,[part_num,bom_num],database)
			return part_num, bom_num
			
	else:
		return None, None
	
		
def updateSAPMaterialMaster(case, plant, partNum, revision = None, materialGroup = None, description = None, upc = None, unit = None, 
		brand = None, brandDescription = None, platform = None, platformDescription = None,
		buyCode = None, longDescription = None, storageLocation = None, idoc = None, user = None,
		standardCost = None, costUnit = None, width = None, height = None, length = None, unitDimension = None, partStatus = None,
		procurement = None, MRP = None, grossWeight = None, netWeight = None, family = None, weightUnit = None, familyDescription = None, deleted = None):
	user = getUser(user) 
		
	parameters = {
		'case': case,
		'plant': plant, 
		'part_num': partNum, 
		'revision': revision,
		'material_group': materialGroup,
		'description': description,
		'upc': upc,
		'unit': unit,
		'brand': brand,
		'brand_description' : brandDescription,
		'platform': platform,
		'platform_description': platformDescription,
		'buy_code': buyCode,
		'long_description': longDescription,
		'storage_loc': storageLocation,
		'idoc': idoc,
		'user': user,
		'standard_cost': standardCost,
		'cost_unit': costUnit,
		'width': width,
		'height': height,
		'length': length,
		'unit_dimension': unitDimension,
		'part_status': partStatus,
		'procurement': procurement,
		'mrp': MRP,
		'gross_weight': grossWeight,
		'net_weight': netWeight,
		'family' : family,
		'weight_unit' : weightUnit,
		'family_description': familyDescription
	}

	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("UpdateSAPMaterialMaster", connection)
	rc = getSPScalar(storedProc, parameters, {}, {})
	return rc > 0
	
	
def updateSAPBOM(bomNum, plant, partNum, bomAlternative, qty = None, unit = None, 
		startDate = None, endDate = None, idoc = None, user = None):
	user = getUser(user)
				
	parameters = {
		'bom_num': bomNum,
		'plant': plant, 
		'part_num': partNum, 
		'bom_alternative': bomAlternative,
		'qty': qty,
		'unit': unit,
		'start_date': startDate,
		'end_date' : endDate,
		'idoc': idoc,
		'user': user
	}
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("UpdateSAPBillOfMaterial", connection)
	rc = getSPScalar(storedProc, parameters, {}, {})
	
	return rc > 0
	

def updateSAPBOMItem(bom_id, bomNum, bomAlternative, plant, partNum, itemNum = None, qty = None, unit = None, 
		startDate = None, endDate = None, user = None, itemCategory = None, costRelevancy = None, bomNxtLvl = None, item_node = None, counter = None):
	user = getUser(user)

	parameters = {
		'bom_id': bom_id,
		'bom_num': bomNum,
		'bom_alternative': bomAlternative,
		'plant': plant, 
		'part_num': partNum, 
		'item_num': itemNum,
		'qty': qty,
		'unit': unit,
		'start_date': startDate,
		'end_date' : endDate,
		'user': user,
		'item_category': itemCategory,
		'cost_relevancy': costRelevancy,
		'bom_nxt_lvl': bomNxtLvl,
		'item_node': item_node,
		'counter': counter
	}
	types = {
		'bom_id': system.db.BIGINT
		# MS-SQL does a better job of date conversion than Jython and/or Ignition
		# ,
		# 'start_date': system.db.DATE,
		# 'end_date' : system.db.DATE
	}	
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("UpdateSAPBOMItem", connection)
	rc = getSPScalar(storedProc, parameters, {}, types)
	
	return rc > 0
	
	
def insertSAPMaterialMaster(plant, partNum, revision = None, materialGroup = None, description = None, upc = None, unit = None, 
		brand = None, brandDescription = None, platform = None, platformDescription = None,
		buyCode = None, longDescription = None, storageLocation = None, idoc = None, user = None,
		standardCost = None, costUnit = None, width = None, height = None, length = None, unitDimension = None, partStatus = None,
		procurement = None, MRP = None, grossWeight = None, netWeight = None, family = None, weightUnit = None, familyDescription = None):
	user = getUser(user) 

	parameters = {
		'plant': plant, 
		'part_num': partNum, 
		'revision': revision,
		'material_group': materialGroup,
		'description': description,
		'upc': upc,
		'unit': unit,
		'brand': brand,
		'brand_description' : brandDescription,
		'platform': platform,
		'platform_description': platformDescription,
		'buy_code': buyCode,
		'long_description': longDescription,
		'storage_loc': storageLocation,
		'idoc': idoc,
		'user': user,
		'standard_cost': standardCost,
		'cost_unit': costUnit,
		'width': width,
		'height': height,
		'length': length,
		'unit_dimension': unitDimension,
		'part_status': partStatus,
		'procurement': procurement,
		'mrp': MRP,
		'gross_weight': grossWeight,
		'net_weight': netWeight,
		'family' : family,
		'weight_unit' : weightUnit,
		'family_description': familyDescription
	}	
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("InsertSAPMaterialMaster", connection)
	rc = getSPScalar(storedProc, parameters, {}, {})
	
	return rc > 0
	
	
def insertSAPBOM(bomNum, plant, partNum, bomAlternative, qty = None, unit = None, 
		startDate = None, endDate = None, idoc = None, user = None):
	user = getUser(user)
		 		
	parameters = {
		'bom_num': bomNum,
		'bom_alternative': bomAlternative,
		'plant': plant, 
		'part_num': partNum, 
		'qty': qty,
		'unit': unit,
		'start_date': startDate,
		'end_date' : endDate,
		'idoc': idoc,
		'user': user
	}
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("InsertSAPBillOfMaterial", connection)
	rc = getSPScalar(storedProc, parameters, {}, {})
	
	return rc > 0


def insertSAPBOMItem(bomNum, bomAlternative , plant, partNum, itemNum, qty = None, unit = None, 
		startDate = None, endDate = None, user = None, itemCategory = None, costRelevancy = None, bomNxtLvl = None, item_node = None, counter = None):
	user = getUser(user)
		
	parameters = {
		'bom_num': bomNum,
		'bom_alternative': bomAlternative,
		'plant': plant, 
		'part_num': partNum, 
		'item_num': itemNum,
		'qty': qty,
		'unit': unit,
		'start_date': startDate,
		'end_date' : endDate,
		'user': user,
		'item_category': itemCategory,
		'cost_relevancy': costRelevancy,
		'bom_nxt_lvl': bomNxtLvl,
		'item_node': item_node,
		'counter': counter
	}
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("InsertSAPBOMItem", connection)
	rc = getSPScalar(storedProc, parameters, {}, {})
	
	return rc > 0
	
	
def deleteSAPMaterialMaster(plant, partNum, user=None, markRecordsDeletedOnly = True):
	user = getUser(user)
		
	parameters = {
		'plant': plant, 
		'part_num': partNum, 
		'user': user,
		'mark_deleted' : markRecordsDeletedOnly
	}	
	types = {
		'mark_deleted' : system.db.BIT
	}
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("DeleteSAPMaterialMaster", connection)
	rc = getSPScalar(storedProc, parameters, {}, types)
	
	return rc > 0
	
	
def deleteSAPBOM(bomNum, bomAlternative, user=None, markRecordsDeletedOnly = True):
	user = getUser(user)
		
	parameters = {
		'bom_num': bomNum,
		'bom_alternative': bomAlternative,
		'user': user,
		'mark_deleted' : markRecordsDeletedOnly
	}
	types = {
		'mark_deleted' : system.db.BIT
	}
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("DeleteSAPBillOfMaterial", connection)
	rc = getSPScalar(storedProc, parameters, {}, types)
	
	return rc > 0


def deleteSAPBOMItem(bom_id, user=None, markRecordsDeletedOnly = True):
	user = getUser(user)
			
	parameters = {
		'bom_id': bom_id,
		'user': user ,
		'mark_deleted' : 1 if markRecordsDeletedOnly else 0 
	}
	types = {
		'bom_id' : system.db.BIGINT,
		'mark_deleted' : system.db.BIT
	}	
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("DeleteSAPBOMItem", connection)
	rc = getSPScalar(storedProc, parameters, {}, types)
	return rc > 0

	
def deleteSAPBOMAndItems(bomNum, bomAlternative, user=None, markRecordsDeletedOnly = True):
	user = getUser(user)
			
	parameters = {
		'bom_num': bomNum,
		'bom_alternative': bomAlternative,
		'user': user,
		'mark_deleted' : 1 if markRecordsDeletedOnly else 0 
	}
	types = {
		'mark_deleted' : system.db.BIT
	}
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	storedProc = system.db.createSProcCall("DeleteSAPBOMAndItems", connection)
	rc = getSPScalar(storedProc, parameters, {}, types)
	
	return rc > 0
	
		
def deleteSAPBOMAndItemsByPN(plant, partNum, user=None, markRecordsDeletedOnly = True):
	user = getUser(user)
			
	parameters = {
		'plant': plant,
		'part_num': partNum,
		'user': user,
		'mark_deleted' : 1 if markRecordsDeletedOnly else 0  
	}
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/IgnitionMES_Extension")[0].value
	types = {
		'mark_deleted' : system.db.BIT
	}
	storedProc = system.db.createSProcCall("DeleteSAPBOMAndItemsByPN", connection)
	rc = getSPScalar(storedProc, parameters, {}, types)
	
	return rc > 0
