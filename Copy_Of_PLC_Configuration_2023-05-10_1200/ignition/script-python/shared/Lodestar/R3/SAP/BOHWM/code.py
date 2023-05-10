from shared.Lodestar.R3.Util import clientScope, updateSAPTags, sap2mesDateFormat2, sap2mesDateFormat3, sapdateformat
from shared.Lodestar.R3.Config import projectName
from shared.Lodestar.R3.Log import insertSAPLogger, insertSAPChangeLog

primarySAPSource = shared.Lodestar.R3.Config.getPrimarySAPSource()

def getBOHWM(WM,sapSource=primarySAPSource):
	#update tags to notify of execution
	updateSAPTags('BOHWM',True)
	if clientScope():
		from com.inductiveautomation.ignition.client.gateway_interface import GatewayException
	else:
		GatewayException = None

#	now = system.date.now()
#	print "Start" + str(now)
	try:
		#call web service
		ws = system.ws.runWebService("SAP-%s/getInventoryWM" % sapSource,
		None,
		{
		  'ZfmMesBohWm': {
			'ILgnum': WM,
			'TLqua': {
			  'item': [{
				'Lgnum': "",
				'Lqnum': "",
				'Matnr': "",
				'Werks': "",
				'Bestq': "",
				'Sobkz': "",
				'Sonum': "",
				'Lgtyp': "",
				'Lgpla': "",
				'Wdatu': "0000-00-00",
				'Meins': "",
				'Gesme': 0.0,
				'Verme': 0.0,
				'Lenum': "",
				'Vbeln': "",
				'Posnr': ""
			  }]
			}
		  }
		})
				
		#convert web service call object to dictionary
		wsDict = system.ws.toDict(ws)

		#drill down into return
		returnDict =  wsDict['Root']['ZfmMesBohWmResponse']['TLqua']
		#check if there were return results
		if returnDict != '':
			#drill down into the items returned by ws
			materialList = returnDict['item']
			
			#create headers for new dataset and initialize list of row lists
			headers = ['WAREHOUSE','QUANT','MATERIAL','PLANT','STOCKCATEGORY','SPECIALSTOCK','SPECSTOCKNUM','STORAGETYPE','STORAGEBIN','GRDATE','BASEUNIT','TOTALSTOCK','AVAILABLESTOCK','STORAGEUNIT','DELIVERY','DELITEM']
			newData = []
			
			#loop through material list and create dataset row for each material
			for materialDict in materialList:
				warehouse		= str(materialDict['Lgnum'])	
				quant 			= str(materialDict['Lqnum'])
				material		= str(materialDict['Matnr'])	
				plant			= str(materialDict['Werks'])
				stockCategory	= str(materialDict['Bestq'])		
				specialStock	= str(materialDict['Sobkz'])		
				specStockNum	= str(materialDict['Sonum'])		
				storageType		= str(materialDict['Lgtyp'])	
				storageBin		= str(materialDict['Lgpla'])	
				grDate			= materialDict['Wdatu']
				baseUnit		= str(materialDict['Meins'])	
				totalStock		= round(materialDict['Gesme'],3)	
				availableStock	= round(materialDict['Verme'],3)		
				storageUnit		= str(materialDict['Lenum'])	
				delivery		= str(materialDict['Vbeln'])	
				delItem			= str(materialDict['Posnr'])
				rowList = [warehouse,quant,material,plant,stockCategory,specialStock,specStockNum,storageType,storageBin,grDate,baseUnit,totalStock,availableStock,storageUnit,delivery,delItem]
				newData.append(rowList)
			
			##create and return new bohwm dataset
			nds = system.dataset.toDataSet(headers,newData)
			nds = system.dataset.sort(nds, "quant")
#			Get rid of rows for testing purposes
#			rowsToDelete = [i for i in range(10, nds.rowCount)] # delete every index after 20
#			nds = system.dataset.deleteRows(nds, rowsToDelete)	
			return nds

		else: #no results were returned
			insertSAPLogger('BOHWM','UPDATE','No Results retrieved from SAP', 1)
			return None
	except GatewayException, error:
		insertSAPLogger('BOHWM','ERROR','SAP Error - %s' % error.getMessage(),2)
		return None
		
	except:
		insertSAPLogger('BOHWM','ERROR','SAP Unavailable',2)
		return None
		
		
		
def processBOHWM(bohwm,sapSource=primarySAPSource):
	#initialize error variable used for updating execution tags
	error = False 
	#verify boh has been successfully returned by web service
	if bohwm is not None and bohwm.rowCount > 0:
		#get all existing boh to check for existence, create list of materials for easy comparison
		params = {'SAPSOURCE':sapSource}
		
		if clientScope():
			exBOHWM = system.db.runNamedQuery('Scheduling/getAllBOHWM',params)
		else:	
			exBOHWM = system.db.runNamedQuery(projectName,'Scheduling/getAllBOHWM',params)
#		Get rid of rows for testing purposes
#		rowsToDelete = [i for i in range(20, exBOHWM.rowCount)] # delete every index after 20
#		exBOHWM = system.dataset.deleteRows(exBOHWM, rowsToDelete)

		if exBOHWM is not None: #make sure query is successful
			#initialize list to append all existing materials
			exBOHWMList = []
			if exBOHWM.rowCount > 0: #boh exists in sap intermediary table, add each material to list
				for row in range(exBOHWM.rowCount):
					quant = exBOHWM.getValueAt(row,'QUANT')
					totalStock = exBOHWM.getValueAt(row,'TOTALSTOCK')			#added - compare list for update
					availableStock = exBOHWM.getValueAt(row,'AVAILABLESTOCK')	#added - compare list for update
					rowList = [quant, totalStock, availableStock]				#added - compare list for update
					exBOHWMList.append(rowList)									#added - compare list for update
			#initialize list to add all new orders to for deletion check later
			newBOHWMList = []
			newWMList = []
			#loop through orders returned by web service and update intermediary sap table as needed
			for row in range(bohwm.rowCount):				
				warehouse		= 		bohwm.getValueAt(row,'WAREHOUSE')
				quant 			= 		bohwm.getValueAt(row,'QUANT')
				material		= 		bohwm.getValueAt(row,'MATERIAL')
				plant			= 		bohwm.getValueAt(row,'PLANT')
				stockCategory	= 		bohwm.getValueAt(row,'STOCKCATEGORY')
				specialStock	= 		bohwm.getValueAt(row,'SPECIALSTOCK')
				specStockNum	= 		bohwm.getValueAt(row,'SPECSTOCKNUM')
				storageType		= 		bohwm.getValueAt(row,'STORAGETYPE')
				storageBin		= 		bohwm.getValueAt(row,'STORAGEBIN')
				grDate			= 		bohwm.getValueAt(row,'GRDATE')
				grDate 			= 		sap2mesDateFormat2(grDate)
				baseUnit		= 		bohwm.getValueAt(row,'BASEUNIT')
				totalStock		= round(bohwm.getValueAt(row,'TOTALSTOCK'), 3)
				availableStock	= round(bohwm.getValueAt(row,'AVAILABLESTOCK'), 3)
				storageUnit		= 		bohwm.getValueAt(row,'STORAGEUNIT')
				delivery		= 		bohwm.getValueAt(row,'DELIVERY')
				delItem			= 		bohwm.getValueAt(row,'DELITEM')
				delItem			= 		sap2mesDateFormat3(delItem)
			
				#aggregate new boh values in list for comparison against existing boh
				sapBOHWM = [warehouse,quant,material,plant,stockCategory,specialStock,specStockNum,storageType,storageBin,grDate,baseUnit,totalStock,availableStock,storageUnit,delivery,delItem]
				rowList = [quant, totalStock, availableStock]				
				newBOHWMList.append(rowList)
				newWMList.append(sapBOHWM)

			exHeaders = ['QUANT','TOTALSTOCK','AVAILABLESTOCK']

			for index, (newQ, exQ) in enumerate(map(None,newBOHWMList,exBOHWMList)):
				if newQ is not None:
					if newQ not in exBOHWMList:
						exQuants = []
						for i in exBOHWMList:
							exQuants.append(i[0])
							if newQ[0] == i[0]:
#								print "Quant " + str(i[0]) + " to be updated"
								updateBOHWMItem(newQ,sapSource)
								for i2 in range(len(newQ)):
									newValue = newQ[i2]
									oldValue = i[i2]
									itemName = exHeaders[i2]
#									print itemName + " OldValue " + str(oldValue) + " NewValue " + str(newValue) 
									if oldValue != newValue:
										insertSAPChangeLog('BOHWM',newQ[0],itemName.upper(),oldValue,newValue,'')
						if newQ is not None:
							if newQ[0] not in exQuants:
#								print "Quant " + str(newQ[0]) + " to be inserted"
								val = newWMList[index]
								newBOHWMItem(val,sapSource)
				if exQ is not None:
					newQuants = []
					for i in newBOHWMList:
						newQuants.append(i[0])
					if exQ[0] not in newQuants:
#						print "Quant " + str(exQ[0]) + " to be deleted"
						deleteBOHWMItem(exQ[0],sapSource)
#			now = system.date.now()
#			print "End" + str(now)

		else: #error returning existing boh from MES
			insertSAPLogger('BOHWM','ERROR','Error retrieving current BOHWM',2)
			error = True
	else:
		#no records retreived, or an error occurred
		error = True
	#update tags to notify of execution stop due to error
	updateSAPTags('BOHWM',False,error)
	
def newBOHWMItem(val,sapSource=primarySAPSource):

	args = {'warehouse':val[0],
			'quant':val[1],
			'material':val[2],
			'plant':val[3],
			'stockCategory':val[4],
			'specialStock':val[5],
			'specStockNum':val[6],
			'storageType':val[7],
			'storageBin':val[8],
			'grDate':val[9],
			'baseUnit':val[10],
			'totalStock':val[11],
			'availableStock':val[12],
			'storageUnit':val[13],
			'delivery':val[14],
			'delItem':val[15],
			'sapSource':sapSource 
			}
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/insertBOHWM',args)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/insertBOHWM',args)
			
	if success:
		insertSAPLogger('BOHWM','ADD','Added %s to inventory' % val[1], 1)
		return True
	else:
		insertSAPLogger('BOHWM','ERROR','Error adding %s to inventory' % val[1], 2)
		return False

#	y = ['WAREHOUSE','MATERIAL','PLANT','STOCKCATEGORY','SPECIALSTOCK','SPECSTOCKNUM','STORAGETYPE','STORAGEBIN','GRDATE','BASEUNIT','STORAGEUNIT','DELIVERY','DELITEM']	
def updateBOHWMItem(quant,sapSource=primarySAPSource):
	args = {'QUANT':quant[0],
			'TOTALSTOCK':quant[1],
			'AVAILABLESTOCK':quant[2],
			'SAPSOURCE':sapSource
			}

	if clientScope():
		success = system.db.runNamedQuery('Scheduling/updateBOHWM',args)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/updateBOHWM',args)
		
	if success:
		insertSAPLogger('BOHWM','UPDATE','Updated: %s' % quant[0], 1)
		return True
	else:
		insertSAPLogger('BOHWM','ERROR','Error updating: %s' % quant[0], 2)
		return False
	
def deleteBOHWMItem(quant,sapSource=primarySAPSource):
	args = {'QUANT':quant,
			'SAPSOURCE':sapSource
			}
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/deleteBOHWM',args)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/deleteBOHWM',args)
		
	if success:
		insertSAPLogger('BOHWM','DELETE','Deleted: %s' % quant, 1)
		return True
	else:
		insertSAPLogger('BOHWM','ERROR','Error deleting: %s' % quant, 2)
		return False