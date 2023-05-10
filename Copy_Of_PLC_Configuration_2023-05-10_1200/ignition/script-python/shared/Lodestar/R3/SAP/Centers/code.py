from shared.Lodestar.R3.Util import clientScope, updateSAPTags
from shared.Lodestar.R3.Config import projectName
from shared.Lodestar.R3.Log import insertSAPLogger, insertSAPChangeLog

primarySAPSource = shared.Lodestar.R3.Config.getPrimarySAPSource()

def getWorkCenters(plant,sapSource=primarySAPSource):
	#update tags to notify of execution
	updateSAPTags('Work Centers',True)

	if clientScope():
		from com.inductiveautomation.ignition.client.gateway_interface import GatewayException
	else:
		GatewayException = None
		
	try:
		ws = system.ws.runWebService("SAP-%s/getWorkCenters" % sapSource,
		None,
		{
		  'ZFM_PP_MES_CRHD': {
			'I_WERKS': plant,
			'T_CRHD': {
			  'item': [{
				'ARBPL': "",
				'KTEXT': ""
			  }]
			}
		  }
		})
		#convert web service call object to dictionary
		wsDict = system.ws.toDict(ws)
		
		#drill down into the items returned by ws
		wcList = wsDict['Root']['ZFM_PP_MES_CRHDResponse']['T_CRHD']['item']
	
		#create headers for new dataset and initialize list of row lists
		headers = ['NAME','DESCRIPTION']
		newData = []
		
		#loop through list and create dataset row for each
		for wcDict in wcList:
			description = wcDict['KTEXT']
			name = str(wcDict['ARBPL'])
			if name != '':
				rowList = [name,description]
		
				newData.append(rowList)
		##create and return new dataset
		nds = system.dataset.toDataSet(headers,newData)
		
		return nds
		
	except GatewayException, error:
		insertSAPLogger('Work Centers','ERROR','SAP Error - %s' % error.getMessage(),2)
		return None
		
	except:
		insertSAPLogger('Work Centers','ERROR','SAP Unavailable',2)
		return None

def processWorkCenters(workCenters,sapSource=primarySAPSource):
	#initialize error variable used for updating execution tags
	error = False 
	#verify work centers have been successfully returned by web service
	if workCenters is not None and workCenters.rowCount > 0:
		#get all existing work centers to check for existence, create list of work centers for easy comparison
		params = {'SAPSOURCE':sapSource}
		if clientScope():
			exWorkCenters = system.db.runNamedQuery('Scheduling/getAllWorkCenters',params)
		else:
			exWorkCenters = system.db.runNamedQuery(projectName,'Scheduling/getAllWorkCenters',params)
		
		if exWorkCenters is not None: #make sure query is successful
			#initialize list to append all existing work centers
			exWorkCentersList = []
			workCenterHeaders = system.dataset.getColumnHeaders(exWorkCenters)
			if exWorkCenters.rowCount > 0: #work centers exist in sap table, add each work center to list
				for row in range(exWorkCenters.rowCount):
					workCenter = exWorkCenters.getValueAt(row,'NAME')
					exWorkCentersList.append(workCenter)
	
	
			#initialize list to add all new work centers to for deletion check later
			newWorkCentersList = []
			
			#loop through work centers returned by web service and update sap table as needed
			for row in range(workCenters.rowCount):
				
				workCenter = workCenters.getValueAt(row,'NAME')
				description = workCenters.getValueAt(row,'DESCRIPTION')
				deleted = False
				#aggregate new work center values in list for comparison against existing work centers
				sapWorkCenter = [workCenter,description,deleted,sapSource]
				
				#append work center to new list
				newWorkCentersList.append(workCenter)
				
				#check for work center existence and insert/update/delete accordingly
				if workCenter in exWorkCentersList: #work center already exists in sap table
					#compare values between existing work center and newly pulled work center
					#get index of existing work center in existing dataset
					exIndex = exWorkCentersList.index(workCenter)
					#initialize list for adding existing work center item values
					exWorkCenterItem = []
					#add values to existing work center item list
					for col in range(exWorkCenters.columnCount):
						val = exWorkCenters.getValueAt(exIndex,col)
						exWorkCenterItem.append(val)
					#create dictionary to hold all update variables
					changes = {'DESCRIPTION':None,
								'DELETED':None
								}
					#first check if any changes exist. If no changes, no update is needed
					if sapWorkCenter == exWorkCenterItem: #item match
						pass #no changes identified, no update is needed for this item
					else: #at least one change has been identified, determine what has changed and update accordingly
						#loop through values of each order and check for changes
						for i in range(len(sapWorkCenter)):
							oldValue = exWorkCenterItem[i]
							newValue = sapWorkCenter[i]
							itemName = workCenterHeaders[i]
							
							if newValue == oldValue:
								changes[itemName] = newValue
							else:
								changes[itemName] = newValue
								insertSAPChangeLog('Work Centers',workCenter,itemName.upper(),oldValue,newValue,'')
						#update work center in work center table
						updateWorkCenterItem(workCenter,changes,sapSource)
							
				else: #new Work Center
					#insert new work center into work center table
					newWorkCenterItem(workCenter,description,sapSource)
					
			#if a work center exists in the mes, but isn't returned in the sap response, the work center was deleted in SAP
			for workCenter in exWorkCentersList:
				if workCenter not in newWorkCentersList:
					#work center was not returned, we need to remove it from MES
					deleteWorkCenterItem(workCenter,sapSource)
	
		else: #error returning existing work center from MES
			insertSAPLogger('Work Centers','ERROR','Error retrieving current Work Centers',2)
			error = True
	else:
		#no records retreived, or an error occurred
		error = True
	#update tags to notify of execution stop due to error
	updateSAPTags('Work Centers',False,error)
	
def newWorkCenterItem(workCenter,description,sapSource=primarySAPSource):
	args = {'NAME':workCenter,
			'DESCRIPTION':description,
			'SAPSOURCE':sapSource
			}
			
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/insertWorkCenter',args)
	else:
		success = system.db.runNamedQuery(projectName,'Scheduling/insertWorkCenter',args)
		
	if success:
		insertSAPLogger('Work Centers','ADD','Added: %s' % workCenter, 1)
		return True
	else:
		insertSAPLogger('Work Centers','ERROR','Error adding: %s' % workCenter, 2)
		return False
		
def updateWorkCenterItem(workCenter,changes,sapSource=primarySAPSource):
	changes['NAME'] = workCenter
	changes['SAPSOURCE'] = sapSource
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/updateWorkCenter',changes)
	else:
		success = system.db.runNamedQuery(projectName,'Scheduling/updateWorkCenter',changes)
		
	if success:
		insertSAPLogger('Work Centers','UPDATE','Updated: %s' % workCenter, 1)
		return True
	else:
		insertSAPLogger('Work Centers','ERROR','Error updating: %s' % workCenter, 2)
		return False
		
def deleteWorkCenterItem(workCenter,sapSource=primarySAPSource):
	changes = { 'NAME':workCenter,
				'DELETED':True,
				'SAPSOURCE':sapSource
				}
				
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/deleteWorkCenter',changes)
	else:
		success = system.db.runNamedQuery(projectName,'Scheduling/deleteWorkCenter',changes)
		
	if success:
		insertSAPLogger('Work Centers','DELETE','Deleted: %s' % workCenter, 1)
		return True
	else:
		insertSAPLogger('Work Centers','ERROR','Error deleting: %s' % workCenter, 2)
		return False