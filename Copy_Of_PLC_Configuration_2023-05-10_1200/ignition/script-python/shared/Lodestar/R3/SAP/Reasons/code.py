from shared.Lodestar.R3.Util import clientScope, updateSAPTags
from shared.Lodestar.R3.Config import projectName
from shared.Lodestar.R3.Log import insertSAPLogger, insertSAPChangeLog

primarySAPSource = shared.Lodestar.R3.Config.getPrimarySAPSource()

def getReasonCodes(sapSource=primarySAPSource):
	#update tags to notify of execution
	updateSAPTags('Reason Codes',True)

	if clientScope():
		from com.inductiveautomation.ignition.client.gateway_interface import GatewayException
	else:
		GatewayException = None
		
	try:
		ws = system.ws.runWebService("SAP-%s/getReasonCodes" % sapSource,
		None,
		{
		  'ZFM_PP_MES_QPCD': {
			'I_CODEGRUPPE': "PLNERR",
			'I_KATALOGART': "R",
			'T_QPCD': {
			  'item': [{
				'CODE': "",
				'KURZTEXT': ""
			  }]
			}
		  }
		})
		
		#convert web service call object to dictionary
		wsDict = system.ws.toDict(ws)
		
		#drill down into the items returned by ws
		rcList = wsDict['Root']['ZFM_PP_MES_QPCDResponse']['T_QPCD']['item']
		
		#create headers for new dataset and initialize list of row lists
		headers = ['TEXT','CODE']
		newData = []
		
		#loop through list and create dataset row for each
		for rcDict in rcList:
			text = rcDict['KURZTEXT']
			code = str(rcDict['CODE'])
		
			rowList = [text,code]
			newData.append(rowList)
		
		##create and return new dataset
		nds = system.dataset.toDataSet(headers,newData)
		
		return nds
		
	except GatewayException, error:
		insertSAPLogger('Reason Codes','ERROR','SAP Error - %s' % error.getMessage(),2)
		return None
		
	except:
		insertSAPLogger('Reason Codes','ERROR','SAP Unavailable',2)
		return None

def processReasonCodes(reasonCodes,sapSource=primarySAPSource):
	#initialize error variable used for updating execution tags
	error = False 
	#verify reason codes have been successfully returned by web service
	if reasonCodes is not None and reasonCodes.rowCount > 0:
		#get all existing reason codes to check for existence, create list of codes for easy comparison
		params = {'SAPSOURCE':sapSource}
		if clientScope():
			exReasonCodes = system.db.runNamedQuery('Scheduling/getAllReasonCodes',params)
		else:	
			exReasonCodes = system.db.runNamedQuery(projectName,'Scheduling/getAllReasonCodes',params)
		
		if exReasonCodes is not None: #make sure query is successful
			#initialize list to append all existing codes
			exReasonCodesList = []
			reasonCodeHeaders = system.dataset.getColumnHeaders(exReasonCodes)
			if exReasonCodes.rowCount > 0: #codes exist in sap table, add each code to list
				for row in range(exReasonCodes.rowCount):
					reasonCode = exReasonCodes.getValueAt(row,'CODE')
					exReasonCodesList.append(reasonCode)
	
	
			#initialize list to add all new codes to for deletion check later
			newReasonCodesList = []
			
			#loop through codes returned by web service and update sap table as needed
			for row in range(reasonCodes.rowCount):
				
				reasonCode = reasonCodes.getValueAt(row,'CODE')
				text = reasonCodes.getValueAt(row,'TEXT')
				deleted = False
				#aggregate new code values in list for comparison against existing codes
				sapReasonCode = [text,reasonCode,deleted,sapSource]
				
				#append code to new list
				newReasonCodesList.append(reasonCode)
				
				#check for code existence and insert/update/delete accordingly
				if reasonCode in exReasonCodesList: #code already exists in sap table
					#compare values between existing code and newly pulled code
					#get index of existing code in existing order dataset
					exIndex = exReasonCodesList.index(reasonCode)
					#initialize list for adding existing code item values
					exReasonCodeItem = []
					#add values to existing code item list
					for col in range(exReasonCodes.columnCount):
						val = exReasonCodes.getValueAt(exIndex,col)
						exReasonCodeItem.append(val)
					#create dictionary to hold all update variables
					changes = {'TEXT':None,
								'DELETED':None
								}
					#first check if any changes exist. If no changes, no update is needed
					if sapReasonCode == exReasonCodeItem: #item match
						pass #no changes identified, no update is needed for this item
					else: #at least one change has been identified, determine what has changed and update accordingly
						#loop through values of each order and check for changes
						for i in range(len(sapReasonCode)):
							oldValue = exReasonCodeItem[i]
							newValue = sapReasonCode[i]
							itemName = reasonCodeHeaders[i]
							
							if newValue == oldValue:
								changes[itemName] = newValue
							else:
								changes[itemName] = newValue
								insertSAPChangeLog('Reason Codes',reasonCode,itemName.upper(),oldValue,newValue,'')
						#update bto code in bto codes table
						updateReasonCodeItem(reasonCode,changes,sapSource)
							
				else: #new reason code
					#insert new code into reason code table
					newReasonCodeItem(reasonCode,text,sapSource)
					
			#if a reason code exists in the mes, but isn't returned in the sap response, the reason code was deleted in SAP
			for reasonCode in exReasonCodesList:
				if reasonCode not in newReasonCodesList:
					#Reason code was not returned, we need to remove it from MES
					deleteReasonCodeItem(reasonCode,sapSource)
	
		else: #error returning existing reason codes from MES
			insertSAPLogger('Reason Codes','ERROR','Error retrieving current Reason Codes',2)
			error = True
	else:
		#no records retreived, or an error occurred
		error = True
	#update tags to notify of execution stop due to error
	updateSAPTags('Reason Codes',False,error)
	
def newReasonCodeItem(reasonCode,text,sapSource=primarySAPSource):
	args = {'CODE':reasonCode,
			'TEXT':text,
			'SAPSOURCE':sapSource
			}
			
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/insertReasonCode',args)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/insertReasonCode',args)
		
	if success:
		insertSAPLogger('Reason Codes','ADD','Added: %s' % reasonCode, 1)
		return True
	else:
		insertSAPLogger('Reason Codes','ERROR','Error adding: %s' % reasonCode, 2)
		return False
		
def updateReasonCodeItem(reasonCode,changes,sapSource=primarySAPSource):
	changes['CODE'] = reasonCode
	changes['SAPSOURCE'] = sapSource
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/updateReasonCode',changes)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/updateReasonCode',changes)
		
	if success:
		insertSAPLogger('Reason Codes','UPDATE','Updated: %s' % reasonCode, 1)
		return True
	else:
		insertSAPLogger('Reason Codes','ERROR','Error updating: %s' % reasonCode, 2)
		return False
		
def deleteReasonCodeItem(reasonCode,sapSource=primarySAPSource):
	changes = { 'CODE':reasonCode,
				'DELETED':True,
				'SAPSOURCE':sapSource
				}
				
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/deleteReasonCode',changes)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/deleteReasonCode',changes)
		
	if success:
		insertSAPLogger('Reason Codes','DELETE','Deleted: %s' % reasonCode, 1)
		return True
	else:
		insertSAPLogger('Reason Codes','ERROR','Error deleting: %s' % reasonCode, 2)
		return False