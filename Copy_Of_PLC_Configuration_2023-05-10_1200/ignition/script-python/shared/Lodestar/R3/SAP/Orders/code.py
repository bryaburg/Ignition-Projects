from shared.Lodestar.R3.Util import clientScope, updateSAPTags, sap2mesDateFormat
from shared.Lodestar.R3.Log import insertSAPLogger, insertSAPOrderChangeLog
from shared.Lodestar.R3.Config import projectName

primarySAPSource = shared.Lodestar.R3.Config.getPrimarySAPSource()

def getPlannedOrders(plant,ferts,halbs,startDate,endDate,sapSource=primarySAPSource):
	startTimeStamp = system.date.now()
	print 'getPlannedOrders', system.date.format(startTimeStamp, 'HH:mm:ss.SSS')
	#update tags to notify of execution
	updateSAPTags('Planned Orders',True)
	
	if clientScope():
		from com.inductiveautomation.ignition.client.gateway_interface import GatewayException
	else:
		GatewayException = None
	#call web service
	try:
		ws = system.ws.runWebService("SAP-%s/getPlannedOrders" % sapSource,
			None,
			{
			  'ZFM_PP_MES_ORDER_DATA': {
				'CRTD_PO': "",
				'DETAILONLY': "",
				'END_DATE': endDate,
				'FERTS': ferts,
				'HALBS': halbs,
				'HEADERONLY': "X",
				'IDNUMBER': 0,
				'MATERIAL': "",
				'NUMBERDAYS': "",
				'OBART': "1",
				'ORDERDETAIL': {
				  'item': [{
					'IDNUMBER': 0,
					'OPER': "",
					'OPERQTY': 0.0,
					'PLANT': plant,
					'SEQNUM': "",
					'WORKCENTER': ""
				  }]
				},
				'ORDERHEADER': {
				  'item': [{
					'ALTBOM': "",
					'BTOBLOCKIND': "",
					#'BTOREMARK': "", removed from BAPI due to \r values causing formatting issues
					'CHNCODE': "",
					'CONFIRMEDQTY': 0.0,
					'DELIVEREDQTY': 0.0,
					'IDNUMBER': "",
					'MATERIAL': "",
					'MATTYPE': "",
					'MRPCTLR': "",
					'ORDERNUMBER': "",
					'ORDERTYPE': "",
					'PEDTR': "0000-00-00",
					'PLANT': "",
					'PRODCTLR': "",
					'PRODLINE': "",
					'PSTTR': "0000-00-00",
					'REMQTY': 0.0,
					'REQDATE': "",
					'SEQNR': "",
					'SLOC': "",
					'STATUS': "",
					'TEXT1': "",
					'USERREMARK': "",
					'VERSION': "",
					'ZCOMMENT': "",
					'ZSENT': "",
					'ZZDTCD': "0000-00-00"
				  }]
				},
				'PLANT': plant,
				'PRODUCTIONLINE': "",
				'RELEASED_PO': "",
				'REMORDERS': "X",
				'START_DATE': startDate,
				'STOCKPLNORDERS': "X",
				'TECO_PO': "",
				'WORKCENTER': ""
			  }
			})
		#print ws
		#convert web service call object to dictionary
		try:
			wsDict = system.ws.toDict(ws)
			#try and process the return in XML format
			
			orderList = wsDict['Root']['ZFM_PP_MES_ORDER_DATAResponse']['ORDERHEADER']['item']
			
			#initialize header list and dataset list
			headers = []
			newData = []
			#loop through and create header list dataset rows to build order dataset
			for order in orderList:
				#initialize rowlist for each order
				rowList = []
				for item in order:
					if item not in headers: #add unique header to headers list
						headers.append(item)
					#get value of item
					value = order[item]
					#datatype conversion handling
					if item not in ('CONFIRMEDQTY','DELIVEREDQTY','PEDTR','PSTTR','REMQTY','REQDATE','ZZDTCD'):
						value = str(value)
					
					rowList.append(value)
		
				newData.append(rowList)
		
			#create and return planned orders dataset
			nds = system.dataset.toDataSet(headers,newData)
			
			endTimeStamp = system.date.now()
			print 'getPlannedOrders', system.date.format(endTimeStamp, 'HH:mm:ss.SSS'), system.date.millisBetween(startTimeStamp, endTimeStamp), 'millis'
			return nds
		except Exception, e:
			insertSAPLogger('Planned Orders','ERROR','Error processing SAP response. ' + str(e),2)
			return None
	except GatewayException, error:
		insertSAPLogger('Planned Orders','ERROR','%s' % error.getMessage(),2)
		return None
	except Exception, e:
		insertSAPLogger('Planned Orders','ERROR',str(e),2)
		return None

def processPlannedOrders(newOrders,ferts,halbs,startDate,endDate,sapSource=primarySAPSource):
	#initialize error variable used for updating execution tags
	startTimeStamp = system.date.now()
	print 'processPlannedOrders', system.date.format(startTimeStamp, 'HH:mm:ss.SSS')

	error = False 
	#verify orders have been successfully returned by web service
	if newOrders is not None and newOrders.rowCount > 0:
		#get all existing orders to check for existence, create list of order numbers for easy comparison
		params = {'sapSource':sapSource, 'startDate':startDate}
		if clientScope():
			exOrders = system.db.runNamedQuery('Scheduling/getFirmPeriodOrders',params)
		else:	
			exOrders = system.db.runNamedQuery(projectName,'Scheduling/getFirmPeriodOrders',params)
		
		if exOrders is not None: #make sure query is successful
			#initialize list to append all existing orders
			exOrderList = []
			orderHeaders = system.dataset.getColumnHeaders(exOrders)
			#print ' exOrderHeaders', orderHeaders
			if exOrders.rowCount > 0: #planned orders exist in sap intermediary table, add each order number to list
				for row in range(exOrders.rowCount):
					order = exOrders.getValueAt(row,'ORDERNUMBER')
					exOrderList.append(order)
			#print ' ', exOrderList
			#initialize list to add all new orders to for deletion check later
			newOrderList = []
			
			#loop through orders returned by web service and update intermediary sap table as needed
			for row in range(newOrders.rowCount):
				
				plant = newOrders.getValueAt(row,'PLANT')
				orderNumber = newOrders.getValueAt(row,'ORDERNUMBER')
				orderType = newOrders.getValueAt(row,'ORDERTYPE')
				material = newOrders.getValueAt(row,'MATERIAL')
				altBom = newOrders.getValueAt(row,'ALTBOM')
				version = newOrders.getValueAt(row,'VERSION')
				zComment = newOrders.getValueAt(row,'ZCOMMENT')
				prodLine = newOrders.getValueAt(row,'PRODLINE')
				psttr = newOrders.getValueAt(row,'PSTTR')
				remQty = newOrders.getValueAt(row,'REMQTY')
				reqDate = newOrders.getValueAt(row,'REQDATE')
				confirmedQty = newOrders.getValueAt(row,'CONFIRMEDQTY')
				mrpCtlr = newOrders.getValueAt(row,'MRPCTLR')
				prodCtlr = newOrders.getValueAt(row,'PRODCTLR')
				idNumber = newOrders.getValueAt(row,'IDNUMBER')
				sloc = newOrders.getValueAt(row,'SLOC')
				status = newOrders.getValueAt(row,'STATUS')
				matType = newOrders.getValueAt(row,'MATTYPE')
				seqNr = newOrders.getValueAt(row,'SEQNR')
				deliveredQty = newOrders.getValueAt(row,'DELIVEREDQTY')
				#btoreMark = newOrders.getValueAt(row,'BTOREMARK') removed from BAPI due to \r values causing formatting issues
				btoreMark = ''
				btoblockInd = newOrders.getValueAt(row,'BTOBLOCKIND')
				zSent = newOrders.getValueAt(row,'ZSENT')
				userRemark = newOrders.getValueAt(row,'USERREMARK')
				pedtr = newOrders.getValueAt(row,'PEDTR')
				zzdtcd = newOrders.getValueAt(row,'ZZDTCD')
				text1 = newOrders.getValueAt(row,'TEXT1')
				chnCode = newOrders.getValueAt(row,'CHNCODE')
				deleted = False
				deletedSource = ''
				#format date fields
				psttr = sap2mesDateFormat(psttr)
				reqDate = sap2mesDateFormat(reqDate)
				pedtr = sap2mesDateFormat(pedtr)
				
				#aggregate new order values in list for comparison against existing orders
				sapOrder = [plant,orderNumber,orderType,material,altBom,version,prodLine,remQty,reqDate,confirmedQty,mrpCtlr,prodCtlr,
										idNumber,sloc,status,matType,seqNr,deliveredQty,btoreMark,btoblockInd,zSent,userRemark,pedtr,zzdtcd,text1,chnCode,psttr,zComment,sapSource,deleted,deletedSource]
				
				#append order to new order list
				newOrderList.append(orderNumber)	
				#check for order existence and insert/update/delete accordingly
				if orderNumber in exOrderList: #order already exists in intermediary sap table
					#compare values between existing order and newly pulled order
					#get index of existing order in existing order dataset
					exOrderIndex = exOrderList.index(orderNumber)
					#initialize list for adding existing order values
					exOrder = []
					#add values to existing order list
					for col in range(exOrders.columnCount):
						val = exOrders.getValueAt(exOrderIndex,col)
						if orderHeaders[col] in ('PEDTR','PSTTR','REQDATE'):
							val = system.date.parse(val)
						exOrder.append(val)
					#create dictionary to hold all update variables
					changes = {'REMQTY':None,
								'REQDATE':None,
								'STATUS':None,
								'SEQNR':None,
								'USERREMARK':None,
								'PEDTR':None,
								'ZZDTCD':None,
								'TEXT1':None,
								'CHNCODE':None,
								'PSTTR':None,
								'ZCOMMENT':None
								}
					#first check if any changes exist. If no changes, no update is needed
					if sapOrder == exOrder[:-2]: #last 2 indexes of existing order are excluded (these are the mesWorkOrderUUID and mesScheduleUUID, which do not exist in SAP)
						#orderNum = sapOrder[1]
						#print orderNum, ' no changes'
						pass #no changes identified, no update is needed for this planned order
					else: #at least one change has been identified, determine what has changed and update accordingly
						#loop through values of each order and check for changes
						print ' change detected'
						for i in range(len(sapOrder)):
							oldValue = exOrder[i]
							newValue = sapOrder[i]
							itemName = orderHeaders[i]
							
							if newValue == oldValue:
								changes[itemName] = newValue
							else:
								print '  ', itemName, oldValue, newValue
								changes[itemName] = newValue
								
								#insertSAPOrderChangeLog(orderNumber,itemName.upper(),oldValue,newValue,'')
						#update order in plannedorders table
						updateSAPOrder(orderNumber,changes,sapSource)
						#print 'updating order', orderNumber, changes
				else: #new order
					#insert new order into plannedorders table
					newSAPOrder(plant,orderNumber,orderType,material,altBom,version,prodLine,remQty,reqDate,confirmedQty,mrpCtlr,prodCtlr,
									idNumber,sloc,status,matType,seqNr,deliveredQty,btoreMark,btoblockInd,zSent,userRemark,pedtr,zzdtcd,text1,chnCode,psttr,zComment,sapSource)
					#print 'newOrder', orderNumber
			#mark any planned orders in the mes within the startdate and enddate as deleted if they do not exist in the sap return
			#if an order exists in the mes, but isn't returned in the sap response (within passed ferts,halbs,startdate,enddate parameters), the order was deleted in SAP
			startDate = sap2mesDateFormat(str(startDate))
			endDate = sap2mesDateFormat(str(endDate))
			#print ' newOrderList', newOrderList	

			for order in exOrderList:
				if order not in newOrderList:
					print ' ', order, 'not in newOrderList'
					#order was not returned, 
					#print '%s not in sap response' % order
					#check to see what order types were requested
					#get row index of order and lookup material type of the existing order
					orderIndex = exOrderList.index(order)
					matType = exOrders.getValueAt(orderIndex,'MATTYPE')
					reqDate = exOrders.getValueAt(orderIndex,'REQDATE')
					#parse sql timestamp req date into java date
					reqDate = system.date.parse(reqDate)
					#print str(startDate), reqDate
					print ' ', startDate, reqDate, endDate
					if startDate <= reqDate and reqDate <= endDate: 
						print 'dates in range'
						if ferts == 'X' and halbs == 'X':
							#both material types were requested, so this order was deleted in sap
							print 'ferts and halbs, deleting order', order
							deleteSAPOrder(order,'SAP',sapSource)
					
						elif ferts == 'X':
							#only ferts was requested, check if this is a ferts order
							if matType == 'FERT':
								#this is a ferts order, so this order was deleted in sap
								print 'fert deleting order', order
								deleteSAPOrder(order,'SAP',sapSource)
	
						elif halbs == 'X':
							#only halbs was requested, check if this is a halbs order
							if matType == 'HALB':
								#this is a halbs order, so this order was deleted in sap
								print 'halb deleting order', order
								deleteSAPOrder(order,'SAP',sapSource)
			endTimeStamp = system.date.now()
			print 'processPlannedOrders', system.date.format(endTimeStamp, 'HH:mm:ss.SSS'), system.date.millisBetween(startTimeStamp, endTimeStamp), 'millis'			
		else: #error returning existing orders from database
			insertSAPLogger('Planned Orders','ERROR','Error retrieving existing orders to process new SAP orders',2)
			error = True
	else:
		#no records retreived, or an error occurred
		error = True
	#update tags to notify of execution stop due to error
	updateSAPTags('Planned Orders',True,error)
	return error	

def newSAPOrder(plant,orderNumber,orderType,material,altBom,version,prodLine,remQty,reqDate,confirmedQty,mrpCtlr,prodCtlr,
						idNumber,sloc,status,matType,seqNr,deliveredQty,btoreMark,btoblockInd,zSent,userRemark,pedtr,zzdtcd,text1,chnCode,psttr,zComment,sapSource=primarySAPSource):
	#add new row to sap planned order intermediary table, return 1 if order is new, return 0 if insert was ignored because it already exists
	args = {'PLANT':plant,
			'ORDERNUMBER':orderNumber,
			'ORDERTYPE':orderType,
			'MATERIAL':material,
			'ALTBOM':altBom,
			'VERSION':version,
			'PRODLINE':prodLine,
			'REMQTY':remQty,
			'REQDATE':reqDate,
			'CONFIRMEDQTY':confirmedQty,
			'MRPCTLR':mrpCtlr,
			'PRODCTLR':prodCtlr,
			'IDNUMBER':idNumber,
			'SLOC':sloc,
			'STATUS':status,
			'MATTYPE':matType,
			'SEQNR':seqNr,
			'DELIVEREDQTY':deliveredQty,
			'BTOREMARK':btoreMark,
			'BTOBLOCKIND':btoblockInd,
			'ZSENT':zSent,
			'USERREMARK':userRemark,
			'PEDTR':pedtr,
			'ZZDTCD':zzdtcd,
			'TEXT1':text1,
			'CHNCODE':chnCode,
			'PSTTR':psttr,
			'ZCOMMENT':zComment,
			'SAPSOURCE':sapSource
			}
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/insertSAPOrder',args)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/insertSAPOrder',args)
	
	if success:
		conf = 'Added new MES order: %s' % orderNumber
		type = 'ADD'
		severity = 1
	else:
		conf = 'Error adding MES order: %s' % orderNumber
		type = 'ERROR'
		severity = 2
	
	insertSAPLogger('Planned Orders',type,conf,severity)
	
	return success
	
def updateSAPOrder(orderNumber,changes,sapSource=primarySAPSource):
	
	changes['ORDERNUMBER'] = orderNumber
	changes['SAPSOURCE'] = sapSource
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/updateSAPOrder',changes)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/updateSAPOrder',changes)
		
	if success:
		type = 'UPDATE'
		conf = 'Updated MES order: %s' % orderNumber
		severity = 1
	else:
		type = 'ERROR'
		conf = 'Error MES SAP order: %s' % orderNumber
		severity = 2
	
	insertSAPLogger('Planned Orders',type,conf,severity)
	
	return success
	
def deleteSAPOrder(orderNumber,source,sapSource=primarySAPSource):
	#this functin does not actually delete orders from the SAP_PLANNEDORDERS table, but marks them as deleted via updating the DELETED and DELETEDSOURCE columns
	params = {'orderNumber':orderNumber,'source':source,'sapSource':sapSource}
	
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/deleteSAPOrder',params)
	else:	
		success = system.db.runNamedQuery(projectName,'Scheduling/deleteSAPOrder',params)
		
	if success:
		conf = 'Successfully Deleted SAP order from MES: %s' % orderNumber
		type = 'DELETE'
		severity = 1
	else:
		conf = 'Error Deleting SAP order from MES: %s' % orderNumber
		type = 'ERROR'
		severity = 2
	
	insertSAPLogger('Planned Orders',type,conf,severity)
		
	return success


def postUpdateSAPOrder(plant,orderNumber,material,prodLine,prodVersion,seqNr,chnCode,userRemark,startDate='0000-00-00',endDate='0000-00-00',qty=0.0,comment="",sapSource=primarySAPSource):
	if clientScope():
		from com.inductiveautomation.ignition.client.gateway_interface import GatewayException
	else:
		GatewayException = None
		
	try:
		ws = system.ws.runWebService("SAP-%s/updatePlannedOrders" % sapSource,
			None,
			{
			  'ZFM_PP_MES_ORD_DATA_UPD': {
				'T_RETURN': {
				  'item': [{
					'ID': "",
					'LOG_MSG_NO': "",
					'LOG_NO': "",
					'MESSAGE': "",
					'MESSAGE_V1': "",
					'MESSAGE_V2': "",
					'MESSAGE_V3': "",
					'MESSAGE_V4': "",
					'NUMBER': "",
					'TYPE': ""
				  }]
				},
				'T_TABLE': {
				  'item': [{
					'BTOBLOCKIND': "",
					'CHNCODE': chnCode,
					'MATNR': material,
					'NAUFFX': "",
					'NUSERREMARK': userRemark,
					'OGSMNG': qty,
					'OMDV01': prodLine,
					'OPEDTR': endDate,
					'OPSTTR': startDate,
					'OSEQNR': seqNr,
					'OVERID': prodVersion,
					'PAART': "",
					'PLNUM': orderNumber,
					'PLWRK': plant,
					'XBLNR': "",
					'ZCHANGE': "X",
					'ZCOMMENT': comment,
					'ZCREATE': "",
					'ZDELETE': ""
				  }]
				}
			  }
			})
		success = False
		try:
			ws = system.ws.toDict(ws)
			message = ws['Root']['ZFM_PP_MES_ORD_DATA_UPDResponse']['T_RETURN']['item'][-1]['MESSAGE']
			if 'Changed Successfully' not in message:
				severity = 2
				type = 'ERROR'
				success = False
			else:
				severity = 1
				type = 'UPDATE'
				success = True
			insertSAPLogger('Planned Orders',type,'SAP update: %s - %s' % (orderNumber,message),severity)
		except:
			message = 'No message return from SAP'
			insertSAPLogger('Planned Orders','ERROR','Error Updating Order: %s' % orderNumber, 2)
		return success, message
	except GatewayException, error:
		insertSAPLogger('Planned Orders','ERROR','Update Order: SAP ERROR - %s' % error.getMessage(),2)
		return False, str(error)
	except:
		insertSAPLogger('Planned Orders','ERROR','Update Order: SAP Unavailable',2)
		return False, 'SAP Unavailable'

def postCreateSAPOrder(plant,material,chnCode,qty,prodLine,startDate,endDate,seqNr,prodVersion,userRemark,comment,sapSource=primarySAPSource):
	if clientScope():
		from com.inductiveautomation.ignition.client.gateway_interface import GatewayException
	else:
		GatewayException = None
		
	try:
		ws = system.ws.runWebService("SAP-%s/updatePlannedOrders" % sapSource,
			None,
			{
			  'ZFM_PP_MES_ORD_DATA_UPD': {
				'T_RETURN': {
				  'item': [{
					'ID': "",
					'LOG_MSG_NO': "",
					'LOG_NO': "",
					'MESSAGE': "",
					'MESSAGE_V1': "",
					'MESSAGE_V2': "",
					'MESSAGE_V3': "",
					'MESSAGE_V4': "",
					'NUMBER': "",
					'TYPE': ""
				  }]
				},
				'T_TABLE': {
				  'item': [{
					'BTOBLOCKIND': "",
					'CHNCODE': chnCode,
					'MATNR': material,
					'NAUFFX': "X",
					'NUSERREMARK': userRemark,
					'OGSMNG': qty,
					'OMDV01': prodLine,
					'OPEDTR': endDate,
					'OPSTTR': startDate,
					'OSEQNR': seqNr,
					'OVERID': prodVersion,
					'PAART': "LA",
					'PLNUM': "",
					'PLWRK': plant,
					'XBLNR': "",
					'ZCHANGE': "",
					'ZCOMMENT': comment,
					'ZCREATE': "X",
					'ZDELETE': ""
				  }]
				}
			  }
			})
		success = False
		#print ws
		try:
			ws = system.ws.toDict(ws)
			message = ws['Root']['ZFM_PP_MES_ORD_DATA_UPDResponse']['T_RETURN']['item'][-1]['MESSAGE']
			orderNumber = ws['Root']['ZFM_PP_MES_ORD_DATA_UPDResponse']['T_RETURN']['item'][-1]['MESSAGE_V2']
			if 'Created Successfully' not in message:
				severity = 2
				type = 'ERROR'
				success = False
			else:
				severity = 1
				type = 'CREATE'
				success = True
			insertSAPLogger('Planned Orders',type,'Create Order:%s - %s' % (orderNumber,message),severity)
			return success, message
		except:
			message = 'No message return from SAP'
			insertSAPLogger('Planned Orders','ERROR',message, 2)
			return success, message
	except GatewayException, error:
		insertSAPLogger('Planned Orders','ERROR','Error creating order - %s' % error,2)
		return False, str(error)
	except:
		insertSAPLogger('Planned Orders','ERROR','Create Order: SAP Unavailable',2)
		return False, 'SAP Unavailable'
	

def postDeleteSAPOrder(orderNumber,material,chnCode,seqNr,sapSource=primarySAPSource):
	if clientScope():
		from com.inductiveautomation.ignition.client.gateway_interface import GatewayException
	else:
		GatewayException = None
		
	try:
		ws = system.ws.runWebService("SAP-%s/updatePlannedOrders" % sapSource,
		None,
		{	
		  'ZFM_PP_MES_ORD_DATA_UPD': {
			'T_RETURN': {
			  'item': [{
				'ID': "",
				'LOG_MSG_NO': "",
				'LOG_NO': "",
				'MESSAGE': "",
				'MESSAGE_V1': "",
				'MESSAGE_V2': "",
				'MESSAGE_V3': "",
				'MESSAGE_V4': "",
				'NUMBER': "",
				'TYPE': ""
			  }]
			},
			'T_TABLE': {
			  'item': [{
				'BTOBLOCKIND': "",
				'CHNCODE': chnCode,
				'MATNR': material,
				'NAUFFX': "",
				'NUSERREMARK': "",
				'OGSMNG': 0.0,
				'OMDV01': "",
				'OPEDTR': '0000-00-00',
				'OPSTTR': '0000-00-00',
				'OSEQNR': seqNr,
				'OVERID': "",
				'PAART': "",
				'PLNUM': orderNumber,
				'PLWRK': "",
				'XBLNR': "",
				'ZCHANGE': "",
				'ZCOMMENT': "",
				'ZCREATE': "",
				'ZDELETE': "X"
			  }]
			}
		  }
		})
		success = False
		print ws
		try:
			ws = system.ws.toDict(ws)
			message = ws['Root']['ZFM_PP_MES_ORD_DATA_UPDResponse']['T_RETURN']['item'][-1]['MESSAGE']
			orderNumber = ws['Root']['ZFM_PP_MES_ORD_DATA_UPDResponse']['T_RETURN']['item'][-1]['MESSAGE_V2']
			if 'Deleted Successfully' not in message:
				severity = 2
				type = 'ERROR'
				success = False
			else:
				severity = 1
				type = 'DELETE'
				success = True
			insertSAPLogger('Planned Orders',type,'Delete Order: %s - %s' % (orderNumber,message),severity)
			return success, message
		except:
			message = 'No message return from SAP'
			insertSAPLogger('Planned Orders','ERROR','Error deleting order', 2)
			return success, message

	except GatewayException, error:
		insertSAPLogger('Planned Orders','ERROR','Error deleting order - %s' % error.getMessage(),2)
		return False, str(error)
	except:
		insertSAPLogger('Planned Orders','ERROR','Delete Order: SAP Unavailable',2)
		return False, 'SAP Unavailable'


def getSAPOrder(orderNumber,sapSource=primarySAPSource):
	
	args = {'orderNumber':orderNumber}
	
	return system.db.runNamedQuery('Scheduling/getSAPOrder',args)
