from java.lang import Exception as JavaException
from shared.Lodestar.R3.Config import projectName, getActiveSAPSource, activeFGLines, getLinePath
from shared.Lodestar.R3.Util import clientScope, updateSAPTags, isCurrentDay, timestampToDate, getOpRequestUUID, dsColToList, isStraggler
from shared.Lodestar.R3.OneByOne import create1x1, update1x1, get1x1End, delete1x1
from shared.Lodestar.R3.Production import getScheduleMeta, getProdDayScheduleEntries, getNextProductionDays, getShiftSchedule
from shared.Lodestar.R3.Log import insertScheduleLogger, insertSAPLogger

primarySAPSource = shared.Lodestar.R3.Config.getPrimarySAPSource()

def syncSAPSchedule(execType):
	inProgress = system.tag.readBlocking('SAP/Planned Orders/In Progress')[0].value
	if not inProgress:
		source = 'Planned Orders'
		insertSAPLogger(source, 'Sync', 'Starting ' + execType + ' Sync', 1)
			
		updateSAPTags(source, True)
		
		try:
			sapSource = shared.Lodestar.R3.Config.getActiveSAPSource()
			plant = shared.Lodestar.R3.Config.getPlantCode()
			ferts = 'X'
			halbs = 'X'
			startDate = system.date.now()
			startDate = system.date.setTime(startDate,0,0,0)
			startDateStr = system.date.format(startDate,'yyyy-MM-dd')
		
			firmDays = system.tag.readBlocking('SAP/Firm Days')[0].value
			endDate = system.date.addDays(startDate, firmDays)
			endDateStr = system.date.format(endDate,'yyyy-MM-dd')
			#get planned orders from SAP
			plannedOrders = shared.Lodestar.R3.SAP.Orders.getPlannedOrders(plant, ferts, halbs, startDateStr, endDateStr, sapSource)
			#process SAP orders and update database
			error = shared.Lodestar.R3.SAP.Orders.processPlannedOrders(plannedOrders, ferts, halbs, startDateStr, endDateStr, sapSource)
			
			if error:
				insertSAPLogger('Planned Orders','ERROR','Unable to get SAP orders. Schedule updates aborted.',2)
				updateSAPTags(source, False, error)
			else:
				plcBlackoutActive = system.tag.readBlocking('SAP/PLC Blackout/Active')[0].value
				if not plcBlackoutActive:
					shared.Lodestar.R3.Tag.populateAllModelInfo()
					syncFlag = system.tag.readBlocking('R3/R3_New_Schedule_Sync_Flag')[0].value
					if syncFlag:
						system.tag.writeBlocking('R3/R3_New_Schedule_Sync_Flag', 0)
						
				primarySource = shared.Lodestar.R3.Config.getPrimarySAPSource()
				
				if sapSource == primarySource:
					#schedule all runs and create 1x1
					for line in activeFGLines:
						linePath = shared.Lodestar.R3.Config.getLinePath(line)
						datePath = '%s/SAP/D1/ReqDate' %linePath
						startDate = system.tag.readBlocking(datePath)[0].value
						
						dates, dPaths = getNextProductionDays(startDate, line)
						for d in dates:
							shared.Lodestar.R3.Scheduling.updateLineSchedule(line, d, linePath, sapSource)
#						if execType == 'Manual' or execType == 'Automatic':
#							checkUpdateD1Serials(line, linePath)
#							checkUpdateD2Serials(line, linePath)

					shared.Lodestar.R3.Tag.populateAllScheduleTags()
				checkUpdateCurrentRun(linePath)
				error = system.tag.readBlocking('SAP/Planned Orders/SAP Request Error')[0].value
				updateSAPTags(source, False, error)
				insertSAPLogger(source, 'Sync', execType + ' Sync Complete', 1)
					
		except (Exception, JavaException), e:
			updateSAPTags(source, False, True)
			insertSAPLogger(source, 'ERROR', str(e), 2)
			raise
			
def updateLineSchedule(prodLine, reqDate, linePath, sapSource=primarySAPSource):
	print 'updateLineSchedule for %s on %s' %(prodLine, str(reqDate))
	print '------------------'
	params = {'prodLine':prodLine,'reqDate':reqDate, 'sapSource':sapSource}	
	if clientScope():
		orders = system.db.runNamedQuery('Scheduling/getLineOrdersByReqDateAndSource',params)
	else:
		orders = system.db.runNamedQuery(projectName,'Scheduling/getLineOrdersByReqDateAndSource',params)

	if orders is not None and orders.rowCount > 0:
		scheduledTotal = 0
		for row in range(orders.rowCount):	

			qty = orders.getValueAt(row,'REMQTY')
			scheduledTotal += qty

		meta = getScheduleMeta(prodLine, reqDate, scheduledTotal)

		checkUpdateSchedule(orders, meta, prodLine, reqDate, sapSource)
		checkUpdate1x1s(orders, meta, prodLine)
		return None

def checkUpdateSchedule(orders, meta, prodLine, reqDate, sapSource=primarySAPSource):
	print 'checkUpdateSchedule()'
	sapSchedule = getSAPSchedule(prodLine, reqDate, sapSource)
	print ' sapSchedule', sapSchedule
	if sapSchedule == None or sapSchedule.rowCount <= 0:
		createSAPSchedule(orders, meta, prodLine)
	else:
		updateSAPSchedule(orders, sapSchedule, meta, prodLine, sapSource)
				
def checkUpdate1x1s(orders, meta, prodLine):
	print 'checkUpdate1x1s()'
	secondsPerUnit = meta['prodSecsPerUnit']
	breakTimes = meta['breakTimes']
	for row in range(orders.rowCount):
		orderNumber = orders.getValueAt(row, 'ORDERNUMBER')
		print ' ', orderNumber
		orderUUID = orders.getValueAt(row, 'MESWORKORDERUUID')
		materialName = orders.getValueAt(row, 'MATERIAL')
		quantity = orders.getValueAt(row, 'REMQTY')
		seqNr = orders.getValueAt(row, 'SEQNR')
		scheduleUUID = orders.getValueAt(row, 'MESSCHEDULEUUID')
		startDate = getScheduleItemStart(orderNumber)		
		
		existing1x1 = shared.Lodestar.R3.OneByOne.get1x1(orderNumber)
		exists = existing1x1 is not None and existing1x1.rowCount >0
		
		if exists:
			existingQty = existing1x1.rowCount
			if quantity != existingQty:
				shared.Lodestar.R3.OneByOne.update1x1(prodLine, orderNumber, orderUUID, materialName, quantity, existingQty, seqNr, scheduleUUID, startDate, secondsPerUnit, breakTimes)
		else:
			shared.Lodestar.R3.OneByOne.create1x1(prodLine, orderNumber, orderUUID, materialName, quantity, seqNr, scheduleUUID, startDate, secondsPerUnit, breakTimes)
	
def createSAPSchedule(orders, meta, prodLine):
	print ' createSchedule()'
	if orders is not None and meta is not None:
		startDate = meta['startDate']

		for row in range(orders.rowCount):
			orderNumber = orders.getValueAt(row, 'ORDERNUMBER')
			orderUUID = orders.getValueAt(row, 'MESWORKORDERUUID')
			materialName = orders.getValueAt(row, 'MATERIAL')
			requestedDate = orders.getValueAt(row, 'REQDATE')
			quantity = orders.getValueAt(row, 'REMQTY')
			seqNr = orders.getValueAt(row, 'SEQNR')
			scheduleUUID = None
			secondsPerUnit = meta['secondsPerUnit']
			breakTimes = meta['breakTimes']
			
			millisToAdd = (secondsPerUnit * quantity) * 1000
			endDate = system.date.addMillis(startDate, int(millisToAdd))
			createSAPScheduleItem(orderNumber, quantity, startDate, endDate)
					
			startDate = endDate
			
def createSAPScheduleItem(orderNumber, quantity, startDate, endDate):
	print ' createSAPScheduleItem()', orderNumber
	params = {'orderNumber': orderNumber, 'quantity': quantity, 'estimatedStart': startDate, 'estimatedEnd': endDate}
	if clientScope():
		system.db.runNamedQuery('Scheduling/insertSAPScheduleItem', params)
	else:
		system.db.runNamedQuery(projectName, 'Scheduling/insertSAPScheduleItem', params)
								
def updateSAPSchedule(orders, sapSchedule, meta, prodLine, sapSource=primarySAPSource):
	print ' updateSchedule()'
	scheduledOrders = dsColToList(sapSchedule, 'OrderNumber')
	print scheduledOrders
	returnedOrders = dsColToList(orders, 'ORDERNUMBER')
	print returnedOrders
	
	if scheduledOrders == returnedOrders:
		# sequence remains the same, nothing added or removed
		print 'order lists match'
		for row in range(orders.rowCount):
			schedQty = sapSchedule.getValueAt(row, 'Quantity')
			returnedQty = orders.getValueAt(row, 'REMQTY')
			if schedQty != returnedQty:				
				recreateSAPScheduleItems(row, orders, sapSchedule, prodLine, meta) # delete that row and all afters. recreate them
				break
	else:
		deleteSAPScheduleItems(0, sapSchedule)
		createSAPSchedule(orders, meta, prodLine)
		
def recreateSAPScheduleItems(startRow, orders, scheduleItems, prodLine, meta):
	print ' recreateSAPScheduleItems()'
	deleteSAPScheduleItems(startRow, scheduleItems)
	startDate = scheduleItems.getValueAt(startRow, 'EstimatedStartDateTime')

	for row in range(startRow, orders.rowCount):
		orderNumber = orders.getValueAt(row, 'ORDERNUMBER')
		orderUUID = orders.getValueAt(row, 'MESWORKORDERUUID')
		materialName = orders.getValueAt(row, 'MATERIAL')
		requestedDate = orders.getValueAt(row, 'REQDATE')
		quantity = orders.getValueAt(row, 'REMQTY')
		seqNr = orders.getValueAt(row, 'SEQNR')
		scheduleUUID = None
		secondsPerUnit = meta['secondsPerUnit']
		breakTimes = meta['breakTimes']
		
		millisToAdd = (secondsPerUnit * quantity) * 1000
		endDate = system.date.addMillis(startDate, int(millisToAdd))
		createSAPScheduleItem(orderNumber, quantity, startDate, endDate)
		
		startDate = endDate

def deleteSAPScheduleItems(startRow, items):
	print ' deleteSAPScheduleItems() from', startRow
	for row in range(startRow, items.rowCount):
		orderNumber = items.getValueAt(row, 'OrderNumber')
		deleteSAPScheduleItem(orderNumber)
					
def deleteSAPScheduleItem(orderNumber):
	params = {'orderNumber': orderNumber}
	if clientScope():
		system.db.runNamedQuery('Scheduling/deleteSAPScheduleItem', params)
	else:
		system.db.runNamedQuery(projectName, 'Scheduling/deleteSAPScheduleItem', params)
	
def getSAPSchedule(prodLine, reqDate, sapSource=primarySAPSource):
	params = {'prodLine': prodLine, 'reqDate': reqDate, 'sapSource': sapSource}
	if clientScope():
		schedule = system.db.runNamedQuery('Scheduling/getSAPSchedule', params)
	else:
		schedule = system.db.runNamedQuery(projectName, 'Scheduling/getSAPSchedule', params)
	return schedule													
				
def getScheduledTotal(line, reqDate, sapSource=primarySAPSource):
	params = {'reqDate':reqDate, 'prodLine': line, 'sapSource':sapSource}
	if clientScope():
		schedTotal = system.db.runNamedQuery('Scheduling/getReqDateSchedTotal', params)
	else:
		schedTotal = system.db.runNamedQuery(projectName, 'Scheduling/getReqDateSchedTotal', params)
	return schedTotal
	
def startRun(linePath, modelInfo, index):
	orderNumber = modelInfo['orderNumber']
	orderUUID = shared.Lodestar.R3.Util.getWorkOrderUUID(orderNumber)
	material = modelInfo['material']
	quantity = modelInfo['count']
	seqNr = modelInfo['seqNr']
	startDate = system.date.now()
	
	opRequestUUID = scheduleWorkOrder(linePath, orderNumber, orderUUID, material, quantity, seqNr, startDate)
	opRequest = system.mes.loadMESObject(opRequestUUID)
	system.mes.oee.beginOEERun(opRequest)
	
	updateRunStart(orderNumber)
	setRunActive(linePath)
	
def updateRunStart(orderNumber):
	queryName = 'Scheduling/updateSAPScheduleItemStart'
	params = {'orderNumber':orderNumber}
	if clientScope():
		system.db.runNamedQuery(queryName, params)
	else:
		system.db.runNamedQuery(projectName, queryName, params)
		
def updateRunEnd(orderNumber, endDateTime):
	currentDateTime = system.date.now()
	queryName = 'Scheduling/updateSAPScheduleItemEnd'
	params = {'orderNumber':orderNumber, 'endDateTime': endDateTime}
	if clientScope():
		system.db.runNamedQuery(queryName, params)
	else:
		system.db.runNamedQuery(projectName, queryName, params)
	return currentDateTime
#	endDateTime = system.date.now()
#	queryName = 'Scheduling/updateSAPScheduleItemEnd'
#	params = {'orderNumber':orderNumber, 'endDateTime': endDateTime}
#	if clientScope():
#		system.db.runNamedQuery(queryName, params)
#	else:
#		system.db.runNamedQuery(projectName, queryName, params)
#	return endDateTime
	
def adjustRunTimeEstimates(linePath, newStartTime, currentSeq):
	prodLine = linePath.split('\\')[-1]
	reqDate = system.tag.readBlocking(linePath +'/SAP/D1/ReqDate')[0].value
	sapSource = getActiveSAPSource()
	
	seqPath = linePath+'/Run Counter/Sequence Number'
	currentSequence = system.tag.readBlocking(seqPath)[0].value
	
	schedTotal = getScheduledTotal(prodLine, reqDate, sapSource)
	meta = getScheduleMeta(prodLine, reqDate, schedTotal)
	secsPerUnit = meta['secondsPerUnit']
	
	remainingSchedule = getRemainingSAPSchedule(prodLine, reqDate, currentSeq, sapSource)
	itemStart = newStartTime
	for row in range(remainingSchedule.rowCount):
		orderNumber = remainingSchedule.getValueAt(row, 'OrderNumber')
		qty = remainingSchedule.getValueAt(row, 'Quantity')
		millisToAdd = secsPerUnit * 1000
		itemEnd = system.date.addMillis(itemStart, int(millisToAdd * qty))
		updateRunEstimates(orderNumber, itemStart, itemEnd)
		
		itemStart = itemEnd
			
def updateRunEstimates(orderNumber, estimatedStart, estimatedEnd):
	queryName = 'Scheduling/updateSAPScheduleItemEstimates'
	params = {'orderNumber': orderNumber, 'newEstimatedStart': estimatedStart, 'newEstimatedEnd': estimatedEnd}
	if clientScope():
		system.db.runNamedQuery(queryName, params)
	else:
		system.db.runNamedQuery(projectName, queryName, params)
		
def setRunActive(linePath):
	basePath = linePath + '/Production Counts'
	activePath = '%s/Run Active' %basePath
	
	system.tag.writeBlocking(activePath, 1)
		
def getRemainingSAPSchedule(prodLine, reqDate, currentSeq, sapSource=primarySAPSource):
	queryName = 'Scheduling/getRemainingSAPSchedule'
	params =  {'prodLine': prodLine, 'reqDate': reqDate, 'seqNr': currentSeq, 'sapSource': sapSource}
	if clientScope():
		schedule = system.db.runNamedQuery(queryName, params)
	else:
		schedule = system.db.runNamedQuery(projectName, queryName, params)
	return schedule
	
def getScheduleItemStart(orderNumber):
	queryName = 'Scheduling/getScheduleItemStart'
	params = {'orderNumber': orderNumber}
	if clientScope():
		start = system.db.runNamedQuery(queryName, params)
	else:
		start = system.db.runNamedQuery(projectName, queryName, params)
	return start
	
def getOrderSeqNr(orderNumber):
	queryName = 'Scheduling/getOrderSeqNr'
	params = {'orderNumber': orderNumber}
	if clientScope():
		seqNr = system.db.runNamedQuery(queryName, params)
	else:
		seqNr = system.db.runNamedQuery(projectName, queryName, params)
	return seqNr
	
def updateRunProducedQty(orderNumber, producedQuantity):
	queryName = 'Scheduling/updateRunProducedQty'
	params = {'orderNumber':orderNumber, 'producedQty': producedQuantity}
	if clientScope():
		system.db.runNamedQuery(queryName, params)
	else:
		system.db.runNamedQuery(projectName, queryName, params)
		
def getReqDateOrdersByMaterial(prodLine, reqDate):
			
	sapSource = shared.Lodestar.R3.Config.getActiveSAPSource()
	params = {'prodLine':prodLine,'reqDate':reqDate, 'sapSource':sapSource}	
	if clientScope():
		orders = system.db.runNamedQuery('Scheduling/getLineOrdersByReqDateAndSource',params)
	else:
		orders = system.db.runNamedQuery(projectName,'Scheduling/getLineOrdersByReqDateAndSource',params)
		
	ordersDict = {}
	for row in range(orders.rowCount):
		orderNumber = orders.getValueAt(row, 'ORDERNUMBER')
		material = orders.getValueAt(row, 'MATERIAL')
		
		if material in ordersDict.keys():
			matOrders = ordersDict[material]
			matOrders.append(orderNumber)
			ordersDict[material] = matOrders
		else:
			ordersDict[material] = [orderNumber]
				
	return ordersDict

def joinOrdersByMaterial(d2Orders, d3Orders):
	joinedOrders = d2Orders
	for material in d3Orders:
		if material in d2Orders.keys():
			matOrders = d2Orders[material]
			matOrders.extend(d3Orders[material])
			joinedOrders[material] = matOrders
		else:
			joinedOrders[material] = d3Orders[material]
	return joinedOrders
		
def getReqDateModelOrder(prodLine, model, reqDate):
	sapSource = shared.Lodestar.R3.Config.getActiveSAPSource()
	queryName = 'Scheduling/getReqDateModelOrder'
	params = {'prodLine': prodLine, 'material': model, 'reqDate': reqDate, 'sapSource': sapSource}
	
	if clientScope():
		order = system.db.runNamedQuery(queryName, params)
	else:
		order = system.db.runNamedQuery(projectName, queryName, params)
	return order
	
def getModelOrderBeforeSeqNr(orders, startRow, seqNr, material):
	# returns the ordernumber from the first instance prior to passed
	# seqNr with a matching material 
	print 'getModelOrder', startRow, seqNr, material
	for row in range(startRow, -1, -1):
		rowSeq = int(orders.getValueAt(row, 'SEQNR'))
		print 'rowSeq', rowSeq
		if rowSeq < seqNr:
			rowMat = orders.getValueAt(row, 'MATERIAL')
			if rowMat == material:
				orderNumber = orders.getValueAt(row, 'ORDERNUMBER')
				return orderNumber
	return None
	
def getReqDateModelOrderBySeq(prodLine, model, reqDate, seqNr):
	sapSource = shared.Lodestar.R3.Config.getActiveSAPSource()
	queryName = 'Scheduling/getReqDateModelOrderBySeq'
	params = {'prodLine': prodLine, 'material': model, 'reqDate': reqDate, 'sapSource': sapSource, 'seqNr': seqNr}
	
	if clientScope():
		order = system.db.runNamedQuery(queryName, params)
	else:
		order = system.db.runNamedQuery(projectName, queryName, params)
	return order
				
def checkUpdateD1Serials(prodLine, linePath):
	# get orders from PLC schedule to exclude stragglers
	orders = system.tag.readBlocking(linePath+'/PLC SCHED/ModelInfoDs')[0].value
	orders = shared.Lodestar.R3.Util.filterDataSet(orders, "{'DATE OFFSET': '0'}")
	reqDate = system.tag.readBlocking(linePath+'/SAP/D1/ReqDate')[0].value
	
	currentSequence = int(system.tag.readBlocking(linePath+'/Run Counter/Sequence Number')[0].value)
	print currentSequence
	stragglers = shared.Lodestar.R3.Tag.getStragglersList(linePath)
	
	for row in range(orders.rowCount):	
		orderNumber = orders.getValueAt(row, 'ORDERNUMBER')
		orderQuantity = shared.Lodestar.R3.Scheduling.getScheduleQuantity(orderNumber)
		material = orders.getValueAt(row, 'MATERIAL')

		straggler = shared.Lodestar.R3.Util.isStragglerByModel(linePath, material, orderQuantity)
		if not straggler:
			print orderNumber
			serialRange = shared.Lodestar.R3.OneByOne.getSerialRange(orderNumber)
			print serialRange
			if serialRange == ' - ': # no serials populated
				seqNr = orders.getValueAt(row, 'SEQNR')
				print 'seqNr', seqNr
				if seqNr > currentSequence:
					
					prevOrder = getModelOrderBeforeSeqNr(orders, row, seqNr, material)		
					if prevOrder is not None:
						print 'prevOrder', prevOrder
						# previously generated serial
						lastSerial = shared.Lodestar.R3.OneByOne.getLast1x1Serial(prevOrder)
						if lastSerial is None:
							startSerial = shared.Lodestar.R3.Serial.generateSerial(prodLine, reqDate)
						else:
							startSerial = shared.Lodestar.R3.Serial.incrementSerial(lastSerial)		
					else:
						# from zspace
						lastSerial = shared.Lodestar.R3.Serial.getLastSerial(prodLine, material)
						startSerial = shared.Lodestar.R3.Serial.incrementSerial(lastSerial)
						print 'from zspace', lastSerial
					valid = shared.Lodestar.R3.Serial.isSerialValid(startSerial, reqDate)
					if not valid:
						startSerial = shared.Lodestar.R3.Serial.generateSerial(prodLine, reqDate)
					startSerial = shared.Lodestar.R3.Serial.populateSerialNumbers(prodLine, orderNumber, startSerial)
					# potentially adjust any orders with the same model after this one
					shared.Lodestar.R3.Serial.rePopulateModelSerials(prodLine, material, startSerial, seqNr)

def checkUpdateD2Serials(prodLine, linePath):
	reqDate = system.tag.readBlocking(linePath+'/SAP/D2/ReqDate')[0].value
	orders = getReqDateOrders(prodLine, reqDate)
	stragglers = shared.Lodestar.R3.Tag.getStragglersList(linePath)

	for row in range(orders.rowCount):	
		orderNumber = orders.getValueAt(row, 'ORDERNUMBER')
		print orderNumber
		orderQuantity = shared.Lodestar.R3.Scheduling.getScheduleQuantity(orderNumber)
		material = orders.getValueAt(row, 'MATERIAL')
		straggler = shared.Lodestar.R3.Util.isStragglerByModel(linePath, material, orderQuantity)

		if not straggler:
			serialRange = shared.Lodestar.R3.OneByOne.getSerialRange(orderNumber)
			if serialRange == ' - ': # no serials populated
				seqNr = orders.getValueAt(row, 'SEQNR')
				prevOrder = getModelOrderBeforeSeqNr(orders, row, int(seqNr), material)		
				if prevOrder is None:
					# material not found in D2 orders
					d1 = system.tag.readBlocking(linePath+'/SAP/D1/ReqDate')[0].value
					d1Orders = getReqDateOrders(prodLine, d1)
					if d1Orders is not None and d1Orders.rowCount > 0:
						lastSeq = d1Orders.getValueAt(d1Orders.rowCount - 1, 'SEQNR')
						lastSeq = int(lastSeq) + 1
						prevOrder = getModelOrderBeforeSeqNr(d1Orders, d1Orders.rowCount -1, lastSeq, material)		
	
				if prevOrder is not None:
					print 'prevOrder', prevOrder
					# previously generated serial
					lastSerial = shared.Lodestar.R3.OneByOne.getLast1x1Serial(prevOrder)
					print 'lastSerial', lastSerial
					if lastSerial is None:
						startSerial = shared.Lodestar.R3.Serial.generateSerial(prodLine, reqDate)
					else:
						startSerial = shared.Lodestar.R3.Serial.incrementSerial(lastSerial)		
				else:
					# from zspace
					lastSerial = shared.Lodestar.R3.Serial.getLastSerial(prodLine, material)
					startSerial = shared.Lodestar.R3.Serial.incrementSerial(lastSerial)
				valid = shared.Lodestar.R3.Serial.isSerialValid(startSerial, reqDate)
				if not valid:
					startSerial = shared.Lodestar.R3.Serial.generateSerial(prodLine, reqDate)
				print 'startSerial', startSerial
				startSerial = shared.Lodestar.R3.Serial.populateSerialNumbers(prodLine, orderNumber, startSerial)
				# potentially adjust any orders with the same model after this one
				shared.Lodestar.R3.Serial.rePopulateReqDateModelSerials(prodLine, material, startSerial, seqNr, reqDate)
										
def getOrderReqDate(orderNumber):
	queryName = 'Scheduling/getOrderReqDate'
	params = {'orderNumber': orderNumber}
	if clientScope():
		reqDate = system.db.runNamedQuery(queryName, params)
	else:
		reqDate = system.db.runNamedQuery(projectName, queryName, params)
	return reqDate
	
def getReqDateOrders(prodLine, reqDate):
	sapSource = shared.Lodestar.R3.Config.getActiveSAPSource()
	
	queryName = 'Scheduling/getLineOrdersByReqDateAndSource'
	params = {'prodLine': prodLine, 'reqDate': reqDate, 'sapSource': sapSource}
	if clientScope():
		orders = system.db.runNamedQuery(queryName, params)
	else:
		orders = system.db.runNamedQuery(projectName, queryName, params)
	return orders
	
def getScheduleQuantity(orderNumber):
	queryName = 'Scheduling/getOrderScheduledQty'
	params = {'orderNumber': orderNumber}
	if clientScope():
		quantity = system.db.runNamedQuery(queryName, params)
	else:
		quantity = system.db.runNamedQuery(projectName, queryName, params)
	return quantity
			
def checkUpdateCurrentRun(linePath):
	basePath = linePath + '/Run Counter/'
	orderPath = basePath + 'Order Number'
	schedCountPath = basePath  + 'Run Scheduled Count'
	
	vals = system.tag.readAll([orderPath, schedCountPath])
	curOrder = vals[0].value
	curScheduled = vals[1].value
		
	actQuantity = shared.Lodestar.R3.Scheduling.getScheduleQuantity(curOrder)
	
	if curScheduled != actQuantity:
		system.tag.writeBlocking(schedCountPath, actQuantity)