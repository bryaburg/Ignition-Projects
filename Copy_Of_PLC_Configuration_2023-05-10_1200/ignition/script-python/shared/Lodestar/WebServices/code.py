import java
from java.lang import Exception
import xml.etree.ElementTree as ET
from shared.Common.Util import *
from shared.Lodestar.DbPartsBoms import *
from datetime import datetime
from java.sql import Timestamp


def toSAPDate(sqlDatetime2Str):
	"""
		toSAPDate converts the date portion of a SQL datetime2 string to the SAP date format
		parameter:
			sqlDatetime2Str - string in SQL datetime2 format: YYYY-MM-DD hh:mm:ss[.nnnnnnn]
		
		response:
			SAP date format: YYYYMMDD
	"""
	if isinstance(sqlDatetime2Str, basestring):
		if len(sqlDatetimeStr) > 8:
			return sqlDatetime2Str[0:4] + sqlDatetime2Str[5:7] + sqlDatetime2Str[8:10]
		else:
			return sqlDatetimeStr  # Format may already be correct

	if isinstance(sqlDatetime2Str, java.sql.Timestamp):
		date_str = sqlDatetime2Str.toString()
		return date_str[0:4] + date_str[5:7] + date_str[8:10]
	else:
		return 'Expected:<string>'
	

def sendSAPErrorNotification(errorMessage, interfacePath):
#	recipients = system.tag.readBlocking("[default]Configuration/SAPIdocErrorNotificationEmailList")[0].value
	appName = system.tag.readBlocking("[default]Site/Configuration/App Name")[0].value
	fromTo = system.tag.readBlocking("[default]Configuration/Email")[0].value
	body = "<HTML><BODY><H1>Lodestar SAP Idoc Import Error</H1>"
	body += "There was an error processing an Idoc through the " + interfacePath + " interface at " + str(datetime.now()) + ' on the server ' + appName +".  The message returned is  <font color='blue'>" + errorMessage + "</font></BODY></HTML>"
	
	try:
		print 'sending email'
		shared.Lodestar.MailLists.sendEmail("SAP Idoc", None, body, "Lodestar SAP Idoc Import Error", fromTo)
	except Exception, e:
		print "shared.Lodestar.MailLists.sendEmail error: ", getExceptionCauseString(e)


def finishWebServicePOST(message, interfacePath):
	"""
	finishWebServicePOST sends the email notice for any failures and creates the response from the RESTful POST handler
	"""
	if message != 'Success': 
		print 'handleWebServicePOST response:' + message
		sendSAPErrorNotification(message, interfacePath)
	resContent = { 'error_message' : message }
	return {'code': 200, 'headers': {}, 'content': resContent}


def handleWebServicePOST(path, query, header, body):
	"""
	Responds to an incoming HTTP request with the response in dict form
	Puts a HTTP status code in integer format at the key 'code' in the response dict (required).
	Puts Http response headers in dict format at the key 'headers' in the response dict (optional).
	Puts a Http response content in dict format at the key 'content' in the response dict (required).
	
	Arguments:
	    path: string - 			HTTP path portion of URL
	    query: dictionary -		HTTP query parameters
	    headers: dictionary -	HTTP header parameters
	    body: dictionary -		HTTP request body with data in XML format
	"""	
	# the class loader mechanism gets confused in the etree definition, so force the classloader to reset to the parent for each call 
	from org.python.core import imp
	from java.lang import Thread
	Thread.currentThread().setContextClassLoader(imp.getParentClassLoader())
	print "Startign validation"
	# Exit early if the idoc is missing, malformed(not a well-formed idoc in xml format, or otherwise invalid (fails validation for action)
	if path is None:
		return finishWebServicePOST('HTTP POST has no path specified', 'No-Path')
	if body is None:
		print 'handleWebServicePOST headers:', header
		print 'handleWebServicePOST query params:', query
		return finishWebServicePOST('No dictionary received in body of POST', path)
		
#	sapMessage = body.get('ns0:ProcessMessageASync_v2', None)
	sapMessage = body.get('ProcessMessageASync_v2', None) # ns0 not needed for platform 3 Sepsasoft. Should not need to be added back in
	if sapMessage is None:
		return finishWebServicePOST('No process entry found in body of POST: ' + str(body), path)
		
#	idoc = sapMessage.get('ns0:xmlMessage', None)
	idoc = sapMessage.get('xmlMessage', None) # ns0 not needed for platform 3 Sepsasoft. Should not need to be added back in
	
	xmlObj = shared.lodestar_core.tools.xml.XML(idoc)
	idoc = xmlObj.escapeAmpString()
	
	if idoc is None:
		return finishWebServicePOST('No xml entry found in body of POST: ' + str(sapMessage), path)
	
	try:
		root = ET.fromstring(idoc)
	except Exception, e:
		return finishWebServicePOST('Idoc parse error: ' + getExceptionCauseString(e), path)
				
	path = path.lower()
	
	print 'validating'
	if path == 'sap/sendmaterialmaster':
		rc, msg = validateMaterialMaster(root)
		if not rc:
			return finishWebServicePOST('sendMaterialMaster Idoc validation failed:' + msg, path)
	elif path == 'sap/sendbillofmaterial':
		rc, msg = validateBillOfMaterial(root)
		if not rc:
			return finishWebServicePOST('sendBillOfMaterial Idoc validation failed:' + msg, path)
	else:
		return finishWebServicePOST('HTTP POST has invalid path specified: ' + path, path)
				
	# Now process the idoc
	responseMessage = 'Success'
	try:
		if path == 'sap/sendmaterialmaster':
			rc, msg = processMaterialMaster(root)
			if len(msg) > 0:
				responseMessage = 'Error processing idoc : ' + msg
		elif path == 'sap/sendbillofmaterial':
			rc, msg = processBillOfMaterial(root)
			if len(msg) > 0:
				responseMessage = 'Error processing idoc : ' + msg
	except Exception, err:
		responseMessage = 'Error processing idoc : ' + getExceptionCauseString(err)
		
	return finishWebServicePOST(responseMessage, path)


def validateMaterialMaster(root):
	"""
	validateMaterialMaster will check any idoc fields that could indicate invalid data has been sent
	"""
	rc = True
	message = ''
	site = system.tag.readBlocking('[default]Site/Site')[0].value
	enablePlantFiltering = system.tag.readBlocking('[default]Site/Configuration/EnablePlantFiltering')[0].value
	docnum = 'unknown'
	
	docnumElement = root.find('./IDOC/EDI_DC40/DOCNUM')
	if not docnumElement is None:
		docnum = docnumElement.text
	
	bomPlantElement = root.find('./IDOC/E1MARAM/E1MARCM/WERKS')
	
	if bomPlantElement.text == site and enablePlantFiltering == True:
		bomUsageElement = root.find('./IDOC/E1MARAM/E1MARCM/STLAN')
		if not bomUsageElement is None:
			debugPrint('STLAN:',  bomUsageElement.text, len(bomUsageElement.text)) 
			rc = bomUsageElement.text == '1'
			if not rc: message = 'Idoc(' + docnum + '), BOM Usage not set to Production (1).'
		return rc, message
		
	elif enablePlantFiltering == False:
	
		bomUsageElement = root.find('./IDOC/E1MARAM/E1MARCM/STLAN')
		if not bomUsageElement is None:
			debugPrint('STLAN:',  bomUsageElement.text, len(bomUsageElement.text)) 
			rc = bomUsageElement.text == '1'
			if not rc: message = 'Idoc(' + docnum + '), BOM Usage not set to Production (1).'
		return rc, message
		
	else:
		rc = False
		message = 'Idoc(' + docnum + '), Plant is not set to ' + site + '.'
		return rc, message


def validateBillOfMaterial(root):
	"""
	validateBillOfMaterial will check any idoc fields that could indicate invalid data has been sent
	"""
	rc = True
	message = ''
	site = system.tag.readBlocking('[default]Site/Site')[0].value
	enablePlantFiltering = system.tag.readBlocking('[default]Site/Configuration/EnablePlantFiltering')[0].value
	docNum = 'unknown'
	bomNum = 'unknown'
	
	docNumElement = root.find('./IDOC/EDI_DC40/DOCNUM')
	if not docNumElement is None:
		docNum = docNumElement.text
	
	bomNumElement = root.find('./IDOC/E1STZUM/STLNR')
	if not bomNumElement is None:
		bomNum = bomNumElement.text
	
	bomPlantElement = root.find('./IDOC/E1STZUM/E1MASTM/WERKS')
	if bomPlantElement.text == site and enablePlantFiltering == True:
		bomStatusElement = root.find('./IDOC/E1STZUM/E1STKOM/STLST')
		if not bomStatusElement is None:
			bomStatus = bomStatusElement.text.strip(' ')
			debugPrint('STLST:',  bomStatus, len(bomStatus))  
			rc = (bomStatus == '30') or (bomStatus == '50')
			if not rc: 
				message = 'BOM(' + bomNum + '), Idoc(' + docNum + '), BOM Status not set to 30 or 50.'
		
		bomUsageElement = root.find('./IDOC/E1STZUM/STLAN')
		if not bomUsageElement is None and rc == True:
			debugPrint('STLAN:',  bomUsageElement.text, len(bomUsageElement.text))
			rc = bomUsageElement.text == '1'
			if not rc: 
				message = 'BOM(' + bomNum + '), Idoc(' + docNum + '), BOM Usage not set to Production (1).'
		
		bomRecursiveCheck = root.find('./IDOC/E1STZUM/E1STPOM/REKRS')
		print "Recursive looup"
		if not bomRecursiveCheck is None and rc == True:
			debugPrint('REKRS:', 'Possible recursive BOM') 
			rc = bomRecursiveCheck.text != 'X'
			if not rc: 
				message = 'BOM(' + bomNum + '), Idoc(' + docNum + '), SAP indicates possible recursive BOM.'	
		return rc, message
		
	elif enablePlantFiltering == False:
		bomStatusElement = root.find('./IDOC/E1STZUM/E1STKOM/STLST')
		if not bomStatusElement is None:
			bomStatus = bomStatusElement.text.strip(' ')
			debugPrint('STLST:',  bomStatus, len(bomStatus))  
			rc = (bomStatus == '30') or (bomStatus == '50')
			if not rc: 
				message = 'BOM(' + bomNum + '), Idoc(' + docNum + '), BOM Status not set to 30 or 50.'
		
		bomUsageElement = root.find('./IDOC/E1STZUM/STLAN')
		if not bomUsageElement is None and rc == True:
			debugPrint('STLAN:',  bomUsageElement.text, len(bomUsageElement.text))
			rc = bomUsageElement.text == '1'
			if not rc: 
				message = 'BOM(' + bomNum + '), Idoc(' + docNum + '), BOM Usage not set to Production (1).'

		bomRecursiveCheck = root.findall('./IDOC/E1STZUM/E1STPOM')
		for _ in bomRecursiveCheck:
			isRecursive = _.find('./REKRS')
			if not isRecursive is None and rc == True:
				debugPrint('REKRS:', 'Possible recursive BOM') 
				rc = isRecursive.text != 'X'
				if not rc: 
					message = 'BOM(' + bomNum + '), Idoc(' + docNum + '), SAP indicates possible recursive BOM.'
					break	
		return rc, message
		
	else:
		rc = False
		message = 'BOM(' + bomNum + '), Idoc(' + docNum + '), Plant is not set to ' + site + '.'
		return rc, message


def processMaterialMaster(root):
	"""
	processMaterialMaster will read the needed Idoc fields and either update an existing Material Master
			or create a new one
	"""
	rc = True
	message = ''
	
	# Default values
	valuesMap = {
		'PLANT' : 				None,
		'PART_NUM' : 			None,
		'REVISION' : 			None,
		'MATERIAL_GROUP' : 		None,
		'DESCRIPTION' : 		None,
		'PRODUCT_UPC' : 		None,
		'UNIT_MEASURE' : 		None,
		'BRAND_NAME' : 			None,
		'BRAND_DESCRIPTION' : 	None,
		'PLATFORM' : 			None,
		'PLATFORM_DESCRIPTION' : None,
		'BUY_CODE' : 			None,
		'LONG_DESCRIPTION' : 	None,
		'STORAGE_LOC' : 		None,
		'LANGUAGE' : 			None,
		'DOCNUM' : 				None,
		'PART_STATUS' :			None,
		'PROCUREMENT' :			None,
		'MRP' :					None,
		'GROSS_WEIGHT' :		None,
		'NET_WEIGHT' :			None,
		'WIDTH' :				None,
		'HEIGHT' :				None,
		'LENGTH' :				None,
		'UNIT_DIMENSION' :		None,
		'STANDARD_COST' :		None,
		'COST_UNIT' :			None,
		'FAMILY' :				None,
		'WEIGHT_UNIT' :			None,
		'FAMILY_DESCRIPTION':	None
	}
	# Paths for each field, 
	fieldMap = {
		'PLANT' : 				'./IDOC/E1MARAM/E1MARCM/WERKS',
		'PART_NUM' : 			'./IDOC/E1MARAM/MATNR',
		'REVISION' : 			'./IDOC/E1MARAM/Z1MARA1/REVLV',
		'MATERIAL_GROUP' : 		'./IDOC/E1MARAM/MTART',
		'DESCRIPTION' : 		'./IDOC/E1MARAM/E1MAKTM/MAKTX',
		'PRODUCT_UPC' : 		'./IDOC/E1MARAM/EAN11',
		'UNIT_MEASURE' : 		'./IDOC/E1MARAM/MEINS',
		'BRAND_NAME' : 			'./IDOC/E1MARAM/E1MVKEM/MVGR1',
		'BRAND_DESCRIPTION' : 	'./IDOC/E1MARAM/ZTEXTS/BEZEI',
		'PLATFORM' : 			'./IDOC/E1MARAM/PRDHA',
		'PLATFORM_DESCRIPTION' : './IDOC/E1MARAM/ZTEXTS/VTEXT',
		'BUY_CODE' : 			'./IDOC/E1MARAM/E1MARCM/SOBSL',
		'LONG_DESCRIPTION' : 	'./IDOC/E1MARAM/E1MTXHM/E1MTXLM/TDLINE',
		'STORAGE_LOC' : 		'./IDOC/E1MARAM/E1MARCM/E1MARDM/LGORT',
		'LANGUAGE' : 			'./IDOC/E1MARAM/E1MAKTM/SPRAS',  # Several E1MAKTM records may exist, use the english one 'E'
		'DOCNUM' : 				'./IDOC/EDI_DC40/DOCNUM',
		'PART_STATUS' :			'./IDOC/E1MARAM/E1MARCM/MMSTA',
		'PROCUREMENT' :			'./IDOC/E1MARAM/E1MARCM/BESKZ',
		'MRP' :					'./IDOC/E1MARAM/E1MARCM/DISPO',
		'GROSS_WEIGHT' :		'./IDOC/E1MARAM/BRGEW',
		'NET_WEIGHT' :			'./IDOC/E1MARAM/NTGEW',
		'WIDTH' :				'./IDOC/E1MARAM/E1MARMM/LAENG',
		'HEIGHT' :				'./IDOC/E1MARAM/E1MARMM/BREIT',
		'LENGTH' :				'./IDOC/E1MARAM/E1MARMM/HOEHE',
		'UNIT_DIMENSION' :		'./IDOC/E1MARAM/E1MARMM/MEABM',
		'STANDARD_COST' :		'./IDOC/E1MARAM/E1MBEWM/STPRS',
		'COST_UNIT' :			'./IDOC/E1MARAM/E1MBEWM/PEINH',
		'FAMILY' :				'./IDOC/E1MARAM/ZTEXTS/FAMILY',
		'WEIGHT_UNIT' :			'./IDOC/E1MARAM/GEWEI',
		'FAMILY_DESCRIPTION' :	'./IDOC/E1MARAM/ZTEXTS/FTEXT'
	}
	
	# Get PK from idoc
	element = root.find(fieldMap['PLANT'])
	if not element is None:
		valuesMap['PLANT'] = element.text
	element = root.find(fieldMap['PART_NUM'])
	if not element is None:
		valuesMap['PART_NUM'] = element.text

	# Check to see if it already exists
	results = readSAPMaterialMaster(valuesMap['PLANT'], valuesMap['PART_NUM'])
	found = results.rowCount > 0
	
	# Idoc may be partial, init the fields from the db record 
	if found:
		row = system.dataset.toPyDataSet(results)[0]
		for field in fieldMap:
			try:
				valuesMap[field] = row[field]
			except:
				pass
			
	# Get values from idoc
	for field in fieldMap:
		element = root.find(fieldMap[field])
		if not element is None:
			valuesMap[field] = element.text
			
	docnum = str(valuesMap['DOCNUM']) # str() in case the idoc is mal-formed and has no doc number
	
	# Cleanup: Look for the english description if it wasn't first
	if valuesMap['LANGUAGE'] != 'E':
		elementList = root.findall('./IDOC/E1MARAM/E1MAKTM')
		for e in elementList:
			e_language = e.find('./SPRAS')
			e_description = e.find('./MAKTX')
			if not e_language is None and e_language.text == 'E' and not e_description is None:
				valuesMap['DESCRIPTION'] = e_description.text
				break

	try:
		eleList = root.findall('./IDOC/E1MARAM/E1MARMM')
		for e in eleList:
			e_unitMeasure = e.find('./MEINS')
			e_width = e.find('./LAENG')
			e_height = e.find('./BREIT')
			e_length = e.find('./HOEHE')
			e_dimension = e.find('./MEABM')
			if e_unitMeasure == valuesMap['UNIT_MEASURE']:
				valuesMap['WIDTH'] = e_width
				valuesMap['HEIGHT'] = e_height
				valuesMap['LENGTH'] = e_length
				valuesMap['UNIT_DIMENSION'] = e_dimension
				break
	except: 
		pass
				
	try:
		# insert or update
		if found:
			case = '1'
			rc = updateSAPMaterialMaster(case, valuesMap['PLANT'], valuesMap['PART_NUM'], valuesMap['REVISION'], valuesMap['MATERIAL_GROUP'], 
					valuesMap['DESCRIPTION'], valuesMap['PRODUCT_UPC'], valuesMap['UNIT_MEASURE'], 
					valuesMap['BRAND_NAME'], valuesMap['BRAND_DESCRIPTION'], valuesMap['PLATFORM'], valuesMap['PLATFORM_DESCRIPTION'],
					valuesMap['BUY_CODE'], valuesMap['LONG_DESCRIPTION'], valuesMap['STORAGE_LOC'], None, 'SAP Admin',
					valuesMap['STANDARD_COST'], valuesMap['COST_UNIT'], valuesMap['WIDTH'], valuesMap['HEIGHT'], valuesMap['LENGTH'], 
					valuesMap['UNIT_DIMENSION'], valuesMap['PART_STATUS'], valuesMap['PROCUREMENT'], valuesMap['MRP'], 
					valuesMap['GROSS_WEIGHT'], valuesMap['NET_WEIGHT'], valuesMap['FAMILY'], valuesMap['WEIGHT_UNIT'], valuesMap['FAMILY_DESCRIPTION']
					)
		else:
			rc = insertSAPMaterialMaster(valuesMap['PLANT'], valuesMap['PART_NUM'], valuesMap['REVISION'], valuesMap['MATERIAL_GROUP'], 
						valuesMap['DESCRIPTION'], valuesMap['PRODUCT_UPC'], valuesMap['UNIT_MEASURE'], 
						valuesMap['BRAND_NAME'], valuesMap['BRAND_DESCRIPTION'], valuesMap['PLATFORM'], valuesMap['PLATFORM_DESCRIPTION'],
						valuesMap['BUY_CODE'], valuesMap['LONG_DESCRIPTION'], valuesMap['STORAGE_LOC'], None, 'SAP Admin',
					    valuesMap['STANDARD_COST'], valuesMap['COST_UNIT'], valuesMap['WIDTH'], valuesMap['HEIGHT'], valuesMap['LENGTH'], 
						valuesMap['UNIT_DIMENSION'], valuesMap['PART_STATUS'], valuesMap['PROCUREMENT'], valuesMap['MRP'], 
						valuesMap['GROSS_WEIGHT'], valuesMap['NET_WEIGHT'], valuesMap['FAMILY'], valuesMap['WEIGHT_UNIT'], valuesMap['FAMILY_DESCRIPTION']
						)
	except Exception, err:
		message = 'Material Master Idoc(' + docnum + ') insert/update error for p/n: ' + valuesMap['PLANT'] + ' ' + getExceptionCauseString(err)
		rc = False
				
	return rc, message


def processBillOfMaterial(root):
	"""
	processBillOfMaterial will read the needed Idoc fields and either update an existing Material Master
		and its associated items or create a new ones.
	"""
	rc = True
	message = ''
		
	# Default values 
	valuesMap = {
		'BOM_NUM' : 		None,
		'PLANT' : 			None,
		'PART_NUM' : 		None,
		'BOM_ALTERNATIVE' : None,
		'QTY' : 			None,
		'UNIT_MEASURE' : 	None,
		'START_DATE' : 		None,
		'END_DATE' : 		None,
		'DOCNUM' : 			None
	}
	# Paths for each field, 
	fieldMap = {
		'BOM_NUM' : 		'./IDOC/E1STZUM/STLNR',
		'PLANT' : 			'./IDOC/E1STZUM/E1MASTM/WERKS',
		'PART_NUM' : 		'./IDOC/E1STZUM/E1MASTM/MATNR',
		'BOM_ALTERNATIVE' : './IDOC/E1STZUM/E1MASTM/STLAL',
		'QTY' : 			'./IDOC/E1STZUM/E1STKOM/BMENG_C',
		'UNIT_MEASURE' : 	'./IDOC/E1STZUM/E1STKOM/BMEIN',
		'START_DATE' : 		'./IDOC/E1STZUM/E1STKOM/DATUV',
		'END_DATE' : 		'./IDOC/E1STZUM/E1STKOM/Z1STKOM/DATUB',
		'DOCNUM' : 			'./IDOC/EDI_DC40/DOCNUM'		
	}
	# Get values from idoc
	for field in fieldMap:
		element = root.find(fieldMap[field])
		if not element is None:
			valuesMap[field] = element.text
					
	docnum = str(valuesMap['DOCNUM']) # str() in case the idoc is mal-formed and has no doc number
	bomNum = str(valuesMap['BOM_NUM'])
						
	# Check to see if parent part exists (optional, allows for clear and simple error message)  
	parentPartResults = readSAPMaterialMaster(valuesMap['PLANT'], valuesMap['PART_NUM'])
	if parentPartResults.rowCount == 0:
		message = 'BOM(' + bomNum + ') Idoc(' + docnum + ') insert error: Material Master record does not exist for [' + valuesMap['PLANT'] + '] ' + valuesMap['PART_NUM']
		rc = False
		return rc, message

	# Check to see if the parent is a 'FERT' (sales model) material and keep fields to update the 'ZHAL' children
	pySet = system.dataset.toPyDataSet(parentPartResults)
	parentPart = pySet[0]
	if parentPart["MATERIAL_GROUP"] != 'FERT':
		parentPart = None

	# Check to see if the BOM already exists
	results = readSAPBOM(valuesMap['PLANT'], valuesMap['PART_NUM'])
	found = False
	for row in range(results.rowCount):
		# bomNum and plant are not repeated for each item, so later we will re-use the values from the parent part/bom
		bomNum = results.getValueAt(row, 0)
		bomAlternative = results.getValueAt(row, 3) 
		plant = results.getValueAt(row, 1) 
		if valuesMap['BOM_NUM'] == bomNum and valuesMap['BOM_ALTERNATIVE'] == bomAlternative:
			found = True
			break

	try:
		# insert or update
		if found: 
			rc = updateSAPBOM(bomNum, plant, valuesMap['PART_NUM'], valuesMap['BOM_ALTERNATIVE'],
					 valuesMap['QTY'], valuesMap['UNIT_MEASURE'], valuesMap['START_DATE'], valuesMap['END_DATE'],
					 ET.tostring(root), 'SAP Admin')
		else:
			# bomNum and plant are note repeated for each item, so later we will re-use the values from the parent part/bom
			bomNum = valuesMap['BOM_NUM']
			bomAlternative = valuesMap['BOM_ALTERNATIVE']
			plant = valuesMap['PLANT']
			rc = insertSAPBOM(bomNum, plant, valuesMap['PART_NUM'], valuesMap['BOM_ALTERNATIVE'],
					 valuesMap['QTY'], valuesMap['UNIT_MEASURE'], valuesMap['START_DATE'], valuesMap['END_DATE'],
					 ET.tostring(root), 'SAP Admin')
	except Exception, err:
		message = 'BOM(' + bomNum + ') Idoc(' + docnum + ') insert/update error for p/n ' + plant + '/' + valuesMap['PART_NUM'] + ' ' + getExceptionCauseString(err)
		rc = False

	if rc:
		# Collect all of the item elements
		itemList = root.findall('./IDOC/E1STZUM/E1STPOM')
		# get paths for each item's field
		itemFieldMap = {
			'PART_NUM' : 		'./IDNRK',
			'ITEM_NUM' : 		'./POSNR',
			'QTY' : 			'./MENGE_C',
			'UNIT_MEASURE' : 	'./MEINS',
			'START_DATE' : 		'./DATUV',
			'END_DATE' : 		'./Z1STPOM/DATUB',
			'ITEM_CATEGORY' :	'./POSTP',
			'COST_RELEVANCY' :  './SANKA',
			'ITEM_NODE' :       './Z1STPOM/STLKN',
			'COUNTER' :			'./Z1STPOM/STPOZ',
			'NEXT_BOM' :		'./Z1STPOM/NEXT_BOM'
		}
		# Get existing BOM items
		itemResults = readSAPBOMItems(bomNum, bomAlternative)
		itemIdList = []
		# print 'p/n:',
		for itemRow in range(itemResults.rowCount):
			itemIdList.append(itemResults.getValueAt(itemRow, 0))
			# print itemResults.getValueAt(itemRow, 5), type(itemResults.getValueAt(itemRow, 8)), toSAPDate(itemResults.getValueAt(itemRow, 9)))
		
		# print 'ok'
		
		for itemElement in itemList:				
			# (Re)Init Default values 
			itemsValuesMap = {
				'PART_NUM' : 		None,
				'ITEM_NUM' : 		None,
				'QTY' : 			None,
				'UNIT_MEASURE' : 	None,
				'START_DATE' : 		None,
				'END_DATE' : 		None,
				'ITEM_CATEGORY' :	None,
				'COST_RELEVANCY' :  None,
				'ITEM_NODE' : 		None,
				'COUNTER' : 		None,
				'NEXT_BOM' :		None
			}
			# Get values from idoc
			for itemField in itemFieldMap:
				element = itemElement.find(itemFieldMap[itemField])
				if not element is None:
					itemsValuesMap[itemField] = element.text
#					print element
#					print itemsValuesMap[itemField]
			
			if 'T' == itemsValuesMap['ITEM_CATEGORY']:
				continue  # 'T' Text items are for reference only and do not have part numbers
			
			# Check to see if this item exists
			itemFound = False
			for itemRow in range(itemResults.rowCount):
				itemStartDate = toSAPDate(itemResults.getValueAt(itemRow, "START_DATE"))
				itemEndDate = toSAPDate(itemResults.getValueAt(itemRow, "END_DATE"))
				if (itemResults.getValueAt(itemRow, "BOM_NUM") == bomNum and itemResults.getValueAt(itemRow, "ITEM_NODE") == itemsValuesMap['ITEM_NODE']
					and itemResults.getValueAt(itemRow, "COUNTER") == itemsValuesMap['COUNTER']
				   ):
						itemId = itemResults.getValueAt(itemRow, 0)	
						if itemId in itemIdList:  # keep looking if we've already removed a match for the item, else remove this one
							itemFound = True
							itemIdList.remove(itemId)
							break
			
			try:
				childPartResults = None
				# insert or update
				if itemFound:
					rc = updateSAPBOMItem(itemId, bomNum, bomAlternative, plant, itemsValuesMap['PART_NUM'], itemsValuesMap['ITEM_NUM'],
							 itemsValuesMap['QTY'], itemsValuesMap['UNIT_MEASURE'], itemsValuesMap['START_DATE'], itemsValuesMap['END_DATE'], 
							 'SAP Admin', itemsValuesMap['ITEM_CATEGORY'], itemsValuesMap['COST_RELEVANCY'], itemsValuesMap['NEXT_BOM'], itemsValuesMap['ITEM_NODE'], itemsValuesMap['COUNTER'])
					if not rc:
						message = 'BOM(' + bomNum + ') Idoc(' + docnum + ') item update error: no matching record.'
					elif not parentPart is None:  # Parent is a sales model (FERT), we may have to update some fields in the child
						childPartResults = readSAPMaterialMaster(plant, itemsValuesMap['PART_NUM'])
				else:
					# Check to see if child part exists (optional, allows for clear and simple error message) 
					childPartResults = readSAPMaterialMaster(plant, itemsValuesMap['PART_NUM'])
					if childPartResults.rowCount == 0:
						message = 'BOM(' + bomNum + ') Idoc(' + docnum + ') item insert error: Material Master record does not exist for [' + str(plant) + '] ' + str(itemsValuesMap['PART_NUM'])
						rc = False
					
					if rc:					
						rc = insertSAPBOMItem(bomNum, bomAlternative, plant, itemsValuesMap['PART_NUM'], itemsValuesMap['ITEM_NUM'],
								itemsValuesMap['QTY'], itemsValuesMap['UNIT_MEASURE'], itemsValuesMap['START_DATE'], itemsValuesMap['END_DATE'], 
								'SAP Admin', itemsValuesMap['ITEM_CATEGORY'], itemsValuesMap['COST_RELEVANCY'], itemsValuesMap['NEXT_BOM'], itemsValuesMap['ITEM_NODE'], itemsValuesMap['COUNTER'])
						if not rc:
							message = 'BOM(' + bomNum + ') Idoc(' + docnum + ') item insert error: no record processed.'
				
				childPart = None			
				if rc and childPartResults and childPartResults.rowCount > 0:
					pySet = system.dataset.toPyDataSet(childPartResults)
					childPart = pySet[0]
					if childPart["MATERIAL_GROUP"] != 'ZHAL':
						childPart = None	
										
				if rc and parentPart and childPart:
					# Update child part with parent part information for brand, platform, and upc					
					brandDescription = parentPart['BRAND_DESCRIPTION'] if not parentPart['BRAND_DESCRIPTION'] is None else parentPart['BRAND_NAME']
					platformDescription = parentPart['PLATFORM_DESCRIPTION'] if not parentPart['PLATFORM_DESCRIPTION'] is None else parentPart['PLATFORM']
					
					# Update child part with parent part information for brand, platform, and upc
					case = '2'					  					
					updateSAPMaterialMaster(case, childPart['PLANT'], childPart['PART_NUM'], childPart['REVISION'], childPart['MATERIAL_GROUP'], 
							childPart['DESCRIPTION'], parentPart['PRODUCT_UPC'], childPart['UNIT_MEASURE'], 
							parentPart['BRAND_NAME'], brandDescription, parentPart['PLATFORM'], platformDescription,
							childPart['BUY_CODE'], childPart['LONG_DESCRIPTION'], childPart['STORAGE_LOC'], ET.tostring(root), 'SAP Admin')
			except Exception, err:
				message = 'BOM(' + bomNum + ') Idoc(' + docnum + ') item insert/update error for p/n: ' + str(plant) + '/' + str(itemsValuesMap['PART_NUM']) + ', item: ' + str(itemsValuesMap['ITEM_NUM']) + '; ' + getExceptionCauseString(err)
				rc = False
			
			if not rc:
				break
				
	if rc:
		# mark any unreferenced items as deleted
		for bomItemId in itemIdList:
			rc &= deleteSAPBOMItem(bomItemId, 'SAP Admin')
	return rc, message

def checkForDuplicate():
	"Checks for duplicates and sends out error message is duplicates exist"
	
	part_num, bom_num = readDeleteBOMItemAndBOM()
	
	if part_num is not None:
		errorMessage = "Duplicate BOM(" + bom_num + ") and part number(" + part_num + ") were found and deleted in SQL tables [dbo].[SAPBillOfMaterial] and [dbo].[SAPBOMItem]"
		sendDuplicateNotification(errorMessage)
			
def sendDuplicateNotification(errorMessage):
#	recipients = system.tag.readBlocking("[default]Configuration/SAPIdocErrorNotificationEmailList")[0].value
	appName = system.tag.readBlocking("[default]Site/Configuration/App Name")[0].value
	fromTo = system.tag.readBlocking("[default]Configuration/Email")[0].value
	body = "<HTML><BODY><H1>Lodestar SAP Idoc Duplicate BOM/Part Number</H1>"
	body += 'Duplicate BOM and Part Number were found at ' + str(datetime.now()) + ' on the server ' + appName +".  The message returned is  <font color='blue'>" + errorMessage + "</font></BODY></HTML>"
	
	try:
		shared.Lodestar.MailLists.sendEmail("SAP Idoc", None, body, "Lodestar SAP Idoc Duplicate BOM/Part Number", fromTo)
	except Exception, e:
		print "shared.Lodestar.MailLists.sendEmail error: ", getExceptionCauseString(e)		