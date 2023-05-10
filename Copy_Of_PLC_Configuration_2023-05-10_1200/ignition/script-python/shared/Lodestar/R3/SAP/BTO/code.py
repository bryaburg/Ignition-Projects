from shared.Lodestar.R3.Util import clientScope, updateSAPTags
from shared.Lodestar.R3.Config import projectName
from shared.Lodestar.R3.Log import insertSAPLogger, insertSAPChangeLog

primarySAPSource = shared.Lodestar.R3.Config.getPrimarySAPSource()

def getBTOCodes(sapSource=primarySAPSource):
	#update tags to notify of execution
	updateSAPTags('BTO Codes',True)
	if clientScope():
		from com.inductiveautomation.ignition.client.gateway_interface import GatewayException
	else:
		GatewayException = None
		
	try:
		ws = system.ws.runWebService("SAP-%s/getBTOCodes" % sapSource,
		None,
		{
		  'ZFM_MES_BTOCDE': {
			'T_BTOCDE': {
			  'item': [{
				'BTOCDE': "",
				'DESCRIPTION': "",
				'MANDT': ""
			  }]
			}
		  }
		})
		#convert web service call object to dictionary
		wsDict = system.ws.toDict(ws)
		
		#drill down into the items returned by ws
		btoList = wsDict['Root']['ZFM_MES_BTOCDEResponse']['T_BTOCDE']['item']
	
		#create headers for new dataset and initialize list of row lists
		headers = ['BTOCDE','DESCRIPTION','MANDT']
		newData = []
		
		#loop through list and create dataset row for each 
		for btoDict in btoList:
			code = btoDict['BTOCDE']
			desc = btoDict['DESCRIPTION']
			manDt = btoDict['MANDT']
		
			rowList = [code,desc,manDt]
			newData.append(rowList)
		##create and return new bto dataset
		nds = system.dataset.toDataSet(headers,newData)
		
		return nds
		
	except GatewayException, error:
		insertSAPLogger('BTO Codes','ERROR','SAP Error - %s' % error.getMessage(),2)
		return None
		
	except:
		insertSAPLogger('BTO Codes','ERROR','SAP Unavailable',2)
		return None

def processBTOCodes(btoCodes,sapSource=primarySAPSource):
	#initialize error variable used for updating execution tags
	error = False 	
	#verify boh has been successfully returned by web service
	if btoCodes is not None and btoCodes.rowCount > 0:
		#get all existing boh to check for existence, create list of materials for easy comparison
		params = {'SAPSOURCE':sapSource}
		if clientScope():
			exBTOCodes = system.db.runNamedQuery('Scheduling/getAllBTOCodes',params)
		else:	
			exBTOCodes = system.db.runNamedQuery(projectName,'Scheduling/getAllBTOCodes',params)
		
		if exBTOCodes is not None: #make sure query is successful
			#initialize list to append all existing materials
			exBTOCodesList = []
			btoCodeHeaders = system.dataset.getColumnHeaders(exBTOCodes)
			if exBTOCodes.rowCount > 0: #bto codes exist in sap table, add each code to list
				for row in range(exBTOCodes.rowCount):
					btoCode = exBTOCodes.getValueAt(row,'BTOCODE')
					exBTOCodesList.append(btoCode)
	
	
			#initialize list to add all new bto codes to for deletion check later
			newBTOCodesList = []
			
			#loop through bto codes returned by web service and update sap table as needed
			for row in range(btoCodes.rowCount):
				
				btoCode = btoCodes.getValueAt(row,'BTOCDE')
				description = btoCodes.getValueAt(row,'DESCRIPTION')
				mandt = str(btoCodes.getValueAt(row,'MANDT'))
				deleted = False
				#aggregate new bto code values in list for comparison against existing bto codes
				sapBTOCode = [btoCode,description,mandt,deleted,sapSource]
				
				#append code to new list
				newBTOCodesList.append(btoCode)	
				
				#check for code existence and insert/update/delete accordingly
				if btoCode in exBTOCodesList: #bto code already exists in sap table
					#compare values between existing bto code and newly pulled bto code
					#get index of existing bto code in existing order dataset
					exIndex = exBTOCodesList.index(btoCode)
					#initialize list for adding existing bto code item values
					exBTOCodeItem = []
					#add values to existing bto code item list
					for col in range(exBTOCodes.columnCount):
						val = exBTOCodes.getValueAt(exIndex,col)
						exBTOCodeItem.append(val)
					#create dictionary to hold all update variables
					changes = {'DESCRIPTION':None,
								'MANDT':None,
								'DELETED':None
								}
					#first check if any changes exist. If no changes, no update is needed
					if sapBTOCode == exBTOCodeItem: #item match
						pass #no changes identified, no update is needed for this item
					else: #at least one change has been identified, determine what has changed and update accordingly
						#loop through values of each order and check for changes
						for i in range(len(sapBTOCode)):
							oldValue = exBTOCodeItem[i]
							newValue = sapBTOCode[i]
							itemName = btoCodeHeaders[i]
							if newValue == oldValue:
								changes[itemName] = newValue
							else:
								changes[itemName] = newValue
								insertSAPChangeLog('BTO Codes',btoCode,itemName.upper(),oldValue,newValue,'')
						#update bto code in bto codes table
						updateBTOCodeItem(btoCode,changes,sapSource)
							
				else: #new bto code
					#insert new bto code into bto code table
					newBTOCodeItem(btoCode,description,mandt,sapSource)
					
			#if a bto code exists in the mes, but isn't returned in the sap response, the bto code was deleted in SAP
			for btoCode in exBTOCodesList:
				if btoCode not in newBTOCodesList:
					#bto code was not returned, we need to remove it from MES
					deleteBTOCodeItem(btoCode,sapSource)
	
		else: #error returning existing bto codes from MES
			insertSAPLogger('BTO Codes','ERROR','Error retrieving current BTO Codes',2)
			error = True
	else:
		#no records retreived, or an error occurred
		error = True
	#update tags to notify of execution stop due to error
	updateSAPTags('BTO Codes',False,error)
				
def newBTOCodeItem(btoCode,description,mandt,sapSource=primarySAPSource):
	args = {'BTOCODE':btoCode,
			'DESCRIPTION':description,
			'MANDT':mandt,
			'SAPSOURCE':sapSource
			}
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/insertBTOCode',args)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/insertBTOCode',args)
		
	if success:
		insertSAPLogger('BTO Codes','ADD','Added: %s' % btoCode, 1)
		return True
	else:
		insertSAPLogger('BTO Codes','ERROR','ERROR adding: %s' % btoCode, 2)
		return False
		
def updateBTOCodeItem(btoCode,changes,sapSource=primarySAPSource):
	changes['BTOCODE'] = btoCode
	changes['SAPSOURCE'] = sapSource
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/updateBTOCode',changes)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/updateBTOCode',changes)
		
	if success:
		insertSAPLogger('BTO Codes','UPDATE','Updated: %s' % btoCode, 1)
		return True
	else:
		insertSAPLogger('BTO Codes','ERROR','Error updating: %s' % btoCode, 1)
		return False
		
def deleteBTOCodeItem(btoCode,sapSource=primarySAPSource):
	changes = { 'BTOCODE':btoCode,
				'DELETED':True,
				'SAPSOURCE':sapSource
				}
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/deleteBTOCode',changes)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/deleteBTOCode',changes)
		
	if success:
		insertSAPLogger('BTO Codes','DELETE','Deleted: %s' % btoCode, 1)
		return True
	else:
		insertSAPLogger('BTO Codes','ERROR','Error deleting: %s' % btoCode, 2)
		return False