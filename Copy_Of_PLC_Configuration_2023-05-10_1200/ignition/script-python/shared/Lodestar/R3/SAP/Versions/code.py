from shared.Lodestar.R3.Util import clientScope, updateSAPTags, sap2mesDateFormat
from shared.Lodestar.R3.Config import projectName
from shared.Lodestar.R3.Log import insertSAPLogger, insertSAPChangeLog

primarySAPSource = shared.Lodestar.R3.Config.getPrimarySAPSource()

def getProductionVersions(plant,sapSource=primarySAPSource):
	#update tags to notify of execution
	updateSAPTags('Production Version',True)

	if clientScope():
		from com.inductiveautomation.ignition.client.gateway_interface import GatewayException
	else:
		GatewayException = None
		
	try:
		adatu = '0000-00-00'
		bdatu = '0000-00-00'
		prdat = '0000-00-00'
		ws = system.ws.runWebService("SAP-%s/getProductionVersions" % sapSource,
		None,
		{
		  'ZFM_PP_MES_MKAL': {
			'I_WERKS': plant,
			'T_MKAL': {
			  'item': [{
				'ADATU': adatu,
				'ALORT': "",
				'BDATU': bdatu,
				'BSTMA': 0.0,
				'BSTMI': 0.0,
				'ELPRO': "",
				'MANDT': "",
				'MATNR': "",
				'MDV01': "",
				'MDV02': "",
				'MKSP': "",
				'PRDAT': prdat,
				'PRFG_F': "",
				'PRFG_G': "",
				'PRFG_R': "",
				'PRFG_S': "",
				'PRVBE': "",
				'RGEKZ': "",
				'SERKZ': "",
				'STLAL': "",
				'STLAN': "",
				'TEXT1': "",
				'VERID': "",
				'WERKS': ""
			  }]
			}
		  }
		})
		#convert web service call object to dictionary
		wsDict = system.ws.toDict(ws)

		#drill down into the items returned by ws
		pvList = wsDict['Root']['ZFM_PP_MES_MKALResponse']['T_MKAL']['item']

		#create headers for new dataset and initialize list of row lists
		headers = ['VERSION','PLANT','MATERIAL','PRODLINE','CLIENT','PLANID','VERSIONLOCKED','DATELASTCHECKED','TEXT','USAGE','REMALLOWED','ALTERNATIVE','BACKFLUSH','VALIDFROM','VALIDTO',
					'FROMLOTSIZE','TOLOTSIZE','DEFAULTSTORAGEAREA','ISSUESTORAGELOC','RECSTORAGELOC','CHECKSTATUS','CHECKSTATUSPREPLAN','CHECKSTATUSRATEPLAN','CHECKSTATUSBOM']
		newData = []
		
		#loop through list and create dataset row for each
		for pvDict in pvList:
			validFrom = pvDict["ADATU"]
			recStorLoc = pvDict["ALORT"]
			validTo = pvDict["BDATU"]
			toLotSize = int(pvDict["BSTMA"])
			fromLotSize = int(pvDict["BSTMI"])
			issueStorLoc = pvDict["ELPRO"]
			client = pvDict["MANDT"]
			material = str(pvDict["MATNR"])
			prodLine = str(pvDict["MDV01"])
			planID = pvDict["MDV02"]
			prodVersLocked = pvDict["MKSP"]
			dateLastCheck = pvDict["PRDAT"]
			checkStatus = pvDict["PRFG_F"]
			checkStatusPrePlan = pvDict["PRFG_G"]
			checkStatusRatePlan = pvDict["PRFG_R"]
			checkStatusBOM = pvDict["PRFG_S"]
			defaultStorageArea = pvDict["PRVBE"]
			backflush = pvDict["RGEKZ"]
			remAllowed = pvDict["SERKZ"]
			alternative = str(pvDict["STLAL"])
			usage = pvDict["STLAN"]
			text = str(pvDict["TEXT1"])
			prodVersion = str(pvDict["VERID"])
			plant = pvDict["WERKS"]
			if prodVersion != '':
				rowList = [prodVersion,plant,material,prodLine,client,planID,prodVersLocked,dateLastCheck,text,usage,remAllowed,alternative,backflush,validFrom,
							validTo,fromLotSize,toLotSize,defaultStorageArea,issueStorLoc,recStorLoc,checkStatus,checkStatusPrePlan,checkStatusRatePlan,checkStatusBOM]
				for i in range(len(rowList)):
					if rowList[i] == '':
						rowList[i] = None

				newData.append(rowList)
		##create and return new dataset
		nds = system.dataset.toDataSet(headers,newData)
		
		return nds
		
	except GatewayException, error:
		print str(error)
		insertSAPLogger('Production Version','ERROR','SAP Error - %s' % error.getMessage(),2)
		return None
		
	except Exception, e:
		print str(e)
		insertSAPLogger('Production Version','ERROR','SAP Unavailable',2)
		return None

def processProductionVersions(prodVersions,sapSource=primarySAPSource):
	#initialize error variable used for updating execution tags
	error = False 
	#verify production versions have been successfully returned by web service
	if prodVersions is not None and prodVersions.rowCount > 0:
		#get all existing production versions to check for existence, create list of production versions for easy comparison
		params = {'SAPSOURCE':sapSource}
		
		if clientScope():
			exProdVersions = system.db.runNamedQuery('Scheduling/getAllProductionVersions',params)
		else:
			exProdVersions = system.db.runNamedQuery(projectName,'Scheduling/getAllProductionVersions',params)
		
		if exProdVersions is not None: #make sure query is successful
			#initialize list to append all existing production versions
			exProdVersionsList = []
			prodVersionHeaders = system.dataset.getColumnHeaders(exProdVersions)
			if exProdVersions.rowCount > 0: #production versions exist in sap table, add each production version to list
				for row in range(exProdVersions.rowCount):
					prodVersion = exProdVersions.getValueAt(row,'VERSION')
					material = exProdVersions.getValueAt(row,'MATERIAL')
					exProdVersionsList.append([prodVersion,material])
	
	
			#initialize list to add all new production versions to for deletion check later
			newProdVersionsList = []
			
			#loop through production versions returned by web service and update sap table as needed
			for row in range(prodVersions.rowCount):
				
				prodVersion = prodVersions.getValueAt(row,'VERSION')
				plant = prodVersions.getValueAt(row,'PLANT')
				material = prodVersions.getValueAt(row,'MATERIAL')
				prodLine = prodVersions.getValueAt(row,'PRODLINE')
				client = prodVersions.getValueAt(row,'CLIENT')
				planID = prodVersions.getValueAt(row,'PLANID')
				verLocked = prodVersions.getValueAt(row,'VERSIONLOCKED')
				dateLastChecked = prodVersions.getValueAt(row,'DATELASTCHECKED')
				text = prodVersions.getValueAt(row,'TEXT')
				usage = prodVersions.getValueAt(row,'USAGE')
				remAllowed = prodVersions.getValueAt(row,'REMALLOWED')
				alternative = prodVersions.getValueAt(row,'ALTERNATIVE')
				backflush = prodVersions.getValueAt(row,'BACKFLUSH')
				validFrom = prodVersions.getValueAt(row,'VALIDFROM')
				validTo = prodVersions.getValueAt(row,'VALIDTO')
				fromLotSize = prodVersions.getValueAt(row,'FROMLOTSIZE')
				toLotSize = prodVersions.getValueAt(row,'TOLOTSIZE')
				defStorage = prodVersions.getValueAt(row,'DEFAULTSTORAGEAREA')
				issueStorage = prodVersions.getValueAt(row,'ISSUESTORAGELOC')
				recStorage = prodVersions.getValueAt(row,'RECSTORAGELOC')
				checkStatus = prodVersions.getValueAt(row,'CHECKSTATUS')
				checkStatusPre = prodVersions.getValueAt(row,'CHECKSTATUSPREPLAN')
				checkStatusRate = prodVersions.getValueAt(row,'CHECKSTATUSRATEPLAN')
				checkStatusBOM = prodVersions.getValueAt(row,'CHECKSTATUSBOM')
				deleted = False
				#format date fields
				dateLastChecked = sap2mesDateFormat(dateLastChecked)
				validFrom = sap2mesDateFormat(validFrom)
				validTo = sap2mesDateFormat(validTo)
				
				#aggregate new production version values in list for comparison against existing production versions
				sapProdVersion = [prodVersion,plant,material,prodLine,client,planID,verLocked,dateLastChecked,text,usage,remAllowed,alternative,backflush,validFrom,validTo,fromLotSize,toLotSize,
									defStorage,issueStorage,recStorage,checkStatus,checkStatusPre,checkStatusRate,checkStatusBOM,deleted,sapSource]
				
				#append production version to new list
				newProdVersionsList.append([prodVersion,material])
				
				#check for production version existence and insert/update/delete accordingly
				if [prodVersion,material] in exProdVersionsList: #production version already exists in sap table
					#compare values between existing production version and newly pulled production version
					#get index of existing production version in existing dataset
					exIndex = exProdVersionsList.index([prodVersion,material])
					#initialize list for adding existing production version item values
					exProdVersionItem = []
					#add values to existing production version item list
					#log = system.util.getLogger('PROD VER TEST')
					for col in range(exProdVersions.columnCount):
						val = exProdVersions.getValueAt(exIndex,col)
						if prodVersionHeaders[col] in ['DATELASTCHECKED','VALIDFROM','VALIDTO']:
							if val is not None:					
								val = system.date.fromMillis(val.getTime())
							#log.info(str(val))
						exProdVersionItem.append(val)
					#create dictionary to hold all update variables
					changes = { 'PRODLINE':None,
								'CLIENT':None,
								'PLANID':None,
								'VERSIONLOCKED':None,
								'DATELASTCHECKED':None,
								'TEXT':None,
								'USAGE':None,
								'REMALLOWED':None,
								'ALTERNATIVE':None,
								'BACKFLUSH':None,
								'VALIDFROM':None,
								'VALIDTO':None,
								'FROMLOTSIZE':None,
								'TOLOTSIZE':None,
								'DEFAULTSTORAGEAREA':None,
								'ISSUESTORAGELOC':None,
								'RECSTORAGELOC':None,
								'CHECKSTATUS':None,
								'CHECKSTATUSPREPLAN':None,
								'CHECKSTATUSRATEPLAN':None,
								'CHECKSTATUSBOM':None,
								'DELETED':None
								}
					#first check if any changes exist. If no changes, no update is needed
					if sapProdVersion == exProdVersionItem: #item match
						pass #no changes identified, no update is needed for this item
					else: #at least one change has been identified, determine what has changed and update accordingly
						#loop through values of each order and check for changes
						for i in range(len(sapProdVersion)):
							oldValue = exProdVersionItem[i]
							newValue = sapProdVersion[i]
							itemName = prodVersionHeaders[i]
							
							if newValue == oldValue:
								changes[itemName] = newValue
							else:
								changes[itemName] = newValue
								insertSAPChangeLog('Production Version','%s-%s' % (prodVersion,material),itemName.upper(),oldValue,newValue,'')
						#update production version in production version table
						updateProductionVersionItem(prodVersion,material,changes,sapSource)
									
				else: #new production version
					#insert new production version into production version table
					newProductionVersionItem(prodVersion,plant,material,prodLine,client,planID,verLocked,dateLastChecked,text,usage,remAllowed,alternative,backflush,validFrom,validTo,fromLotSize,toLotSize,
														defStorage,issueStorage,recStorage,checkStatus,checkStatusPre,checkStatusRate,checkStatusBOM,sapSource)
					
			#if a production version exists in the mes, but isn't returned in the sap response, the production version was deleted in SAP
			for prodVersion in exProdVersionsList:
				if prodVersion not in newProdVersionsList:
					#production version was not returned, we need to remove it from MES
					deleteProductionVersionItem(prodVersion[0],prodVersion[1],sapSource)
					
		else: #error returning existing production version from MES
			insertSAPLogger('Production Version','ERROR','Error retrieving current Production Version',2)
			error = True
	else:
		#no records retreived, or an error occurred
		error = True
	#update tags to notify of execution stop due to error
	updateSAPTags('Production Version',False,error)
	
def newProductionVersionItem(prodVersion,plant,material,prodLine,client,planID,verLocked,dateLastChecked,text,usage,remAllowed,alternative,backflush,validFrom,validTo,fromLotSize,toLotSize,
																	defStorage,issueStorage,recStorage,checkStatus,checkStatusPre,checkStatusRate,checkStatusBOM,sapSource=primarySAPSource):
	args = {'VERSION':prodVersion,
			'PLANT':plant,
			'MATERIAL':material,
			'PRODLINE':prodLine,
			'CLIENT':client,
			'PLANID':planID,
			'VERSIONLOCKED':verLocked,
			'DATELASTCHECKED':dateLastChecked,
			'TEXT':text,
			'USAGE':usage,
			'REMALLOWED':remAllowed,
			'ALTERNATIVE':alternative,
			'BACKFLUSH':backflush,
			'VALIDFROM':validFrom,
			'VALIDTO':validTo,
			'FROMLOTSIZE':fromLotSize,
			'TOLOTSIZE':toLotSize,
			'DEFAULTSTORAGEAREA':defStorage,
			'ISSUESTORAGELOC':issueStorage,
			'RECSTORAGELOC':recStorage,
			'CHECKSTATUS':checkStatus,
			'CHECKSTATUSPREPLAN':checkStatusPre,
			'CHECKSTATUSRATEPLAN':checkStatusRate,
			'CHECKSTATUSBOM':checkStatusBOM,
			'SAPSOURCE':sapSource
			}
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/insertProductionVersion',args)
	else:
		success = system.db.runNamedQuery(projectName,'Scheduling/insertProductionVersion',args)
		
	if success:
		insertSAPLogger('Production Version','Add','Added: %s-%s' % (prodVersion,material), 1)
		return True
	else:
		insertSAPLogger('Production Version','ERROR','Error adding: %s-%s' % (prodVersion,material), 2)
		return False
		
def updateProductionVersionItem(prodVersion,material,changes,sapSource=primarySAPSource):
	changes['VERSION'] = prodVersion
	changes['MATERIAL'] = material
	changes['SAPSOURCE'] = sapSource
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/updateProductionVersion',changes)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/updateProductionVersion',changes)
		
	if success:
		insertSAPLogger('Production Version','UPDATE','Updated: %s-%s' % (prodVersion,material), 1)
		return True
	else:
		insertSAPLogger('Production Version','ERROR','Error updating: %s-%s' % (prodVersion,material), 2)
		return False
		
def deleteProductionVersionItem(prodVersion,material,sapSource=primarySAPSource):
	changes = { 'VERSION':prodVersion,
				'MATERIAL':material,
				'DELETED':True,
				'SAPSOURCE':sapSource
				}
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/deleteProductionVersion',changes)
	else:
		success = system.db.runNamedQuery(projectName,'Scheduling/deleteProductionVersion',changes)
		
	if success:
		insertSAPLogger('Production Version','DELETE','Deleted: %s-%s' % (prodVersion,material), 1)
		return True
	else:
		insertSAPLogger('Production Version','ERROR','Error deleting: %s-%s' % (prodVersion,material), 2)
		return False
