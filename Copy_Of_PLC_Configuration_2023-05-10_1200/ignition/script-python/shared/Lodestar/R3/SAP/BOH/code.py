from shared.Lodestar.R3.Util import clientScope, updateSAPTags
from shared.Lodestar.R3.Config import projectName
from shared.Lodestar.R3.Log import insertSAPLogger, insertSAPChangeLog

primarySAPSource = shared.Lodestar.R3.Config.getPrimarySAPSource()

def getBOH(plant,material='',location='0001',sapSource=primarySAPSource):
	#update tags to notify of execution
	updateSAPTags('BOH',True)
	if clientScope():
		from com.inductiveautomation.ignition.client.gateway_interface import GatewayException
	else:
		GatewayException = None
		
	if material is None:
		materialStr = ''
	else:
		materialStr = material
	try:
		#call web service
		ws = system.ws.runWebService("SAP-%s/getBOH" % sapSource,
		None,
		{
		  'ZFM_MES_BOH': {
			'I_LGORT': location,
			'I_MATNR': materialStr,
			'I_WERKS': plant,
			'T_MATRL_O': {
			  'item': [{
				'KLABS':0.0,
				'LABST': 0.0,
				'LGORT': "",
				'MATNR': "",
				'WERKS': ""
			  }]
			}
		  }
		})
		
		#convert web service call object to dictionary
		wsDict = system.ws.toDict(ws)

		#drill down into return
		returnDict =  wsDict['Root']['ZFM_MES_BOHResponse']['T_MATRL_O']
		#check if there were return results
		if returnDict != '':
			#drill down into the items returned by ws
			materialList = returnDict['item']
			
			#create headers for new dataset and initialize list of row lists
			headers = ['PLANT','STORAGELOCATION','MATERIAL','VENDORSTOCK','WHIRLPOOLSTOCK','TOTALSTOCK']
			newData = []
			
			#loop through material list and create dataset row for each material
			for materialDict in materialList:
				wpQty = round(materialDict['LABST'],1)
				vendQty = round(materialDict['KLABS'],1)
				totalQty = wpQty + vendQty
				plant = str(materialDict['WERKS'])
				location = str(materialDict['LGORT'])
				material = str(materialDict['MATNR'])
				rowList = [plant,location,material,vendQty,wpQty,totalQty]
				newData.append(rowList)
				
			
			##create and return new boh dataset
			nds = system.dataset.toDataSet(headers,newData)

			return nds		
		else: #no results were returned
			insertSAPLogger('BOH','UPDATE','No Results retrieved from SAP', 1)
			return None
	except GatewayException, error:
		insertSAPLogger('BOH','ERROR','SAP Error - %s' % error.getMessage(),2)
		return None
		
	except:
		insertSAPLogger('BOH','ERROR','SAP Unavailable',2)
		return None
		
def processBOH(boh,plantParam=None,locationParam=None,materialParam=None,sapSource=primarySAPSource):
	#initialize error variable used for updating execution tags
	error = False 
	#verify boh has been successfully returned by web service
	if boh is not None and boh.rowCount > 0:
		#get all existing boh to check for existence, create list of materials for easy comparison
		params = {'SAPSOURCE':sapSource}
		
		if clientScope():
			exBOH = system.db.runNamedQuery('Scheduling/getAllBOH',params)
		else:	
			exBOH = system.db.runNamedQuery(projectName,'Scheduling/getAllBOH',params)
		
		if exBOH is not None: #make sure query is successful
			#initialize list to append all existing materials
			exBOHList = []
			bohHeaders = system.dataset.getColumnHeaders(exBOH)
			if exBOH.rowCount > 0: #boh exists in sap intermediary table, add each material to list
				for row in range(exBOH.rowCount):
					material = exBOH.getValueAt(row,'MATERIAL')
					plant = exBOH.getValueAt(row,'PLANT')
					location = exBOH.getValueAt(row,'STORAGELOCATION')
					exBOHList.append([material,plant,location])


			#initialize list to add all new orders to for deletion check later
			newBOHList = []
			
			#loop through orders returned by web service and update intermediary sap table as needed
			for row in range(boh.rowCount):
				
				plant = boh.getValueAt(row,'PLANT')
				location = boh.getValueAt(row,'STORAGELOCATION')
				material = boh.getValueAt(row,'MATERIAL')
				vendQty = round(boh.getValueAt(row,'VENDORSTOCK'), 1)
				wpQty = round(boh.getValueAt(row,'WHIRLPOOLSTOCK'), 1)
				totalQty = round(boh.getValueAt(row,'TOTALSTOCK'), 1)
				
				#aggregate new boh values in list for comparison against existing boh
				sapBOH = [material,plant,location,vendQty,wpQty,totalQty,sapSource]
				
				#append order to new order list
				newBOHList.append([material,plant,location])	
				
				#check for order existence and insert/update/delete accordingly
				if [material,plant,location] in exBOHList: #order already exists in intermediary sap table
					#compare values between existing order and newly pulled order
					#get index of existing order in existing order dataset
					exIndex = exBOHList.index([material,plant,location])
					#initialize list for adding existing boh item values
					exBOHItem = []
					#add values to existing boh item list
					exHeaders = system.dataset.getColumnHeaders(exBOH)
					for col in range(exBOH.columnCount):
						val = exBOH.getValueAt(exIndex,col)
						
						if exHeaders[col] in ('VENDORSTOCK','WHIRLPOOLSTOCK','TOTALSTOCK'):
							try:
								val = round(val,1)
							except:
								pass
						
						exBOHItem.append(val)
					#create dictionary to hold all update variables
					changes = {'PLANT':None,
								'STORAGELOCATION':None,
								'BALANCEONHAND':None
								}
					#first check if any changes exist. If no changes, no update is needed
					#log = system.util.getLogger('BOH TEST')

					if sapBOH == exBOHItem: #item match
						pass #no changes identified, no update is needed for this item
					else: #at least one change has been identified, determine what has changed and update accordingly
						#loop through values of each order and check for changes
						for i in range(len(sapBOH)):
							oldValue = exBOHItem[i]
							newValue = sapBOH[i]
							itemName = bohHeaders[i]
	
							if newValue == oldValue:
								changes[itemName] = newValue
							else:
								changes[itemName] = newValue
								insertSAPChangeLog('BOH',material,itemName.upper(),oldValue,newValue,'')
						#update material in boh table
						updateBOHItem(plant,material,changes,location,sapSource)
					
				else: #new order
					#insert new material into boh table
					newBOHItem(plant,location,material,vendQty,wpQty,totalQty,sapSource)
					
			#if a material exists in the mes, but isn't returned in the sap response, the material was deleted in SAP
			for material in exBOHList:
				if material not in newBOHList:
					#material was not returned, check to see what locations/material were requested
					#get row index of material and lookup location of the existing order
					bohIndex = exBOHList.index(material)
					loc = exBOH.getValueAt(bohIndex,'STORAGELOCATION')
					plt = exBOH.getValueAt(bohIndex,'PLANT')
					#delete = False
#					if plantParam is not None: #make sure a plant was passed as a parameter, check that plant parameter matches result from existing boh
#						if plt == plantParam:
#							if materialParam is not None: #a material parameter was passsed in, check if material matches parameter
#								if material == materialParam:
#									if loc == locationParam: #material matches parameter and location parameter, so we should delete
#										delete = True
#							elif locationParam is not None:
#								if loc == locationParam:#location matches, so we should delete
#									delete = True
#					if delete:
					if plt == plantParam:
						deleteBOHItem(plt,material[0],material[2],sapSource)
		else: #error returning existing boh from MES
			insertSAPLogger('BOH','ERROR','Error retrieving current BOH',2)
			error = True
	else:
		#no records retreived, or an error occurred
		error = True
	#update tags to notify of execution stop due to error
	updateSAPTags('BOH',False,error)
	
def newBOHItem(plant,location,material,vendQty,wpQty,totalQty,sapSource=primarySAPSource):
	args = {'plant':plant,
			'location':location,
			'material':material,
			'vendQty':vendQty,
			'wpQty':wpQty,
			'totalQty':totalQty,
			'sapSource':sapSource 
			}
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/insertBOH',args)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/insertBOH',args)
		
	if success:
		insertSAPLogger('BOH','ADD','Added %s to inventory' % material, 1)
		return True
	else:
		insertSAPLogger('BOH','ERROR','Error adding %s to inventory' % material, 2)
		return False
		
def updateBOHItem(plant,material,changes,location,sapSource=primarySAPSource):
	changes['PLANT'] = plant
	changes['MATERIAL'] = material
	changes['STORAGELOCATION'] = location
	changes['SAPSOURCE'] = sapSource
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/updateBOH',changes)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/updateBOH',changes)
		
	if success:
		insertSAPLogger('BOH','UPDATE','Updated: %s' % material, 1)
		return True
	else:
		insertSAPLogger('BOH','ERROR','Error updating: %s' % material, 2)
		return False
	
def deleteBOHItem(plant,material,location,sapSource=primarySAPSource):
	args = {'PLANT':plant,
			'MATERIAL':material,
			'STORAGELOCATION':location,
			'SAPSOURCE':sapSource
			}
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/deleteBOH',args)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/deleteBOH',args)
		
	if success:
		insertSAPLogger('BOH','DELETE','Deleted: %s' % material, 1)
		return True
	else:
		insertSAPLogger('BOH','ERROR','Error deleting: %s' % material, 2)
		return False