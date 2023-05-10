import itertools as IT
from shared.Lodestar.R3.Config import projectName, getLinePath, activeFGLines
from shared.Lodestar.R3.Util import clientScope, stripMillis, timestampToDate

def create1x1(prodLine, orderNumber, orderUUID, materialName, quantity, seqNr, scheduleUUID, startDate, secondsPerUnit, breakTimes):

	materialObj = shared.Lodestar.R3.Material.getMaterialDef(materialName)
	if materialObj is not None: #Make sure we found the material def
		description = materialObj.getDescription()
	else:
		description = ''
	scheduleState = 'Scheduled'

	millisPerUnit = round(secondsPerUnit * 1000)
	timestamp = system.date.now()
	#generate start of query, we will need to build an insert query to add all rows in single transaction
	# NEED TO ACCOUNT FOR MAXIMUM ROW INSERT COUNT OF 1000 ROWS. WILL BREAK UP INTO 500 UNIT QUERIES USING ORDERCHUNKER FUNCTION
	quantitySeq = range(quantity)
	quantityChunks = list(orderChunker(500, quantitySeq))
	
	for chunk in quantityChunks:
		
		query = """
				INSERT INTO Schedule1x1
			( Name
			 ,Description
			 ,Enabled
			 ,Material
			 ,ScheduleUUID
			 ,WorkOrder
			 ,WorkOrderUUID
			 ,ProdLine
			 ,SequenceNumber
			 ,OriginalBeginDateTime
			 ,OriginalEndDateTime
			 ,ScheduleState
			 ,Timestamp
			 ,MissedScans)
			 
			 VALUES 
			 """
		#create list of tuples (one for each row) to add into query
		rowList = []
		for unit in chunk:
			name = unit + 1
			endDate = system.date.addMillis(startDate,int(millisPerUnit))

			for breakEvent in breakTimes:
				breakStart = breakEvent['start']
				breakEnd = breakEvent['end']
					
				if system.date.isBetween(endDate, breakStart, breakEnd): #system.date.isAfter(endDate, breakStart):
					# calculate number of seconds into break to account for
					secsPast = system.date.secondsBetween(breakStart, endDate)
					endDate = system.date.addSeconds(breakEnd, secsPast)
					breakTimes.remove(breakEvent)
					break
			if name == quantity: #last unit
				endDate = stripMillis(endDate)		
					
			row = ( str(name),
					str(description),
					1,
					str(materialName),
					str(scheduleUUID),
					str(orderNumber),
					str(orderUUID),
					str(prodLine),
					str(seqNr),
					str(system.date.format(startDate ,'yyyy-MM-dd HH:mm:ss')),
					str(system.date.format(endDate ,'yyyy-MM-dd HH:mm:ss')),
					str(scheduleState),
					str(system.date.format(timestamp ,'yyyy-MM-dd HH:mm:ss')),
					0)
	
			rowList.append(row)
			startDate = endDate
		#add all rows into query
		for row in rowList:
			query += str(row) + ','
		query = query.rstrip(',')
		
		success = system.db.runUpdateQuery(query)
	print ' 1x1End', system.date.format(stripMillis(endDate), 'yyyy-MM-dd HH:mm:ss.SSS')
	return True, stripMillis(endDate)

def update1x1(prodLine, orderNumber, orderUUID, materialName, newQty, existingQty, seqNr, scheduleUUID, startDate, secondsPerUnit, breakTimes):	
	print 'update1x1()'
	if newQty > existingQty:
		numUnits = newQty - existingQty
		addUnitsTo1x1(orderNumber, numUnits)
	else:
		deleteUnitsFrom1x1(orderNumber, newQty)
#	if lastSerial != None:
#		startSerial = shared.Lodestar.R3.Serial.incrementSerial(lastSerial)
#		shared.Lodestar.R3.Serial.rePopulateModelSerials(prodLine, materialName, startSerial, seqNr)
	
def delete1x1(orderNumber):
	params = {'orderNumber':orderNumber}
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/delete1x1ByOrder',params)
	else:
		success = system.db.runNamedQuery(projectName,'Scheduling/delete1x1ByOrder',params)
	return success

def recreate1x1(prodLine, orderNumber, orderUUID, materialName, quantity, seqNr, scheduleUUID, startDate, secondsPerUnit, breakTimes):
	print 'recreate1x1()', orderNumber, quantity
	params = {'orderNumber': orderNumber}
	if clientScope():
		success = system.db.runNamedQuery('Scheduling/delete1x1',params)
	else:
		success = system.db.runNamedQuery(projectName,'Scheduling/delete1x1',params)
	if success:
		return create1x1(prodLine, orderNumber, orderUUID, materialName, quantity, seqNr, scheduleUUID, startDate, secondsPerUnit, breakTimes)
	return True, None

def get1x1(orderNumber):
	queryName = 'Scheduling/get1x1'
	params = {'order': orderNumber}
	if clientScope():
		results = system.db.runNamedQuery(queryName, params)
	else:
		results = system.db.runNamedQuery(projectName, queryName, params)
	return results
	
def getReqDate1x1(prodLine, reqDate):
	sapSource = shared.Lodestar.R3.Config.getActiveSAPSource()
	
	queryName = 'Scheduling/getReqDate1x1'
	params = {'prodLine': prodLine, 'reqDate': reqDate, 'sapSource': sapSource}
	if clientScope():
		oneByOne = system.db.runNamedQuery(queryName, params)
	else:
		oneByOne = system.db.runNamedQuery(projectName, queryName, params)
	return oneByOne

def exists1x1(orderNumber):
	results = get1x1(orderNumber)
	if results is None:
		return False
	else:
		return results.rowCount > 0
	
def get1x1End(workOrder):
	params = {'workOrder': workOrder}
	if clientScope():
		end = system.db.runNamedQuery('Scheduling/get1x1End', params)
	else:
		end = system.db.runNamedQuery(projectName,'Scheduling/get1x1End', params)
	return stripMillis(timestampToDate(end))
	
def get1x1InProgressCount(orderNumber):
	queryName = 'Scheduling/get1x1InProgressCount'
	params = {'orderNumber': orderNumber}
	if clientScope():
		count = system.db.runNamedQuery(queryName, params)
	else:
		count = system.db.runNamedQuery(projectName, queryName, params)
	return count
		
def get1x1SerialCount(orderNumber):
	queryName = 'Scheduling/get1x1SerialCount'
	params = {'orderNumber': orderNumber}
	if clientScope():
		serialCount = system.db.runNamedQuery(queryName, params)
	else:
		serialCount = system.db.runNamedQuery(projectName, queryName, params)
	return serialCount
	
def getSerialRange(orderNumber):
	params = {'workOrder': orderNumber}
	if clientScope():
		serialRange = system.db.runNamedQuery('Scheduling/get1x1SerialRange', params)
	else:
		serialRange = system.db.runNamedQuery(projectName, 'Scheduling/get1x1SerialRange', params)
	return serialRange
	
def getSerialForUnit(workOrder, unit):
	queryName = 'Scheduling/getSerialForUnit'
	params = {'workOrder': workOrder, 'unit': unit}
	if clientScope():
		serial = system.db.runNamedQuery(queryName, params)
	else:
		serial = system.db.runNamedQuery(projectName, queryName, params)
	return serial
	
def getCompletedQuantity(orderNumber):
	params = {'workOrder': orderNumber}
	if clientScope():
		qty = system.db.runNamedQuery('Scheduling/get1x1CompleteQty', params)
	else:
		qty = system.db.runNamedQuery(projectName, 'Scheduling/get1x1CompleteQty', params)
	return qty
	
def orderChunker(n, iterable):
	#chunk a range of numbers by n. Primarily built to segment orders by their quantity
	#in order to perform a series of multiple db row inserts and avoid the 1000 row limit
	iterable = iter(iterable)
	return iter(lambda: list(IT.islice(iterable, n )), [])
	
def get1x1IDs(orderNumber):
	queryName = 'Scheduling/get1x1IDs'
	params = {'orderNumber': orderNumber}
	if clientScope():
		ids = system.db.runNamedQuery(queryName, params)
	else:
		ids = system.db.runNamedQuery(projectName, queryName, params)
	return ids
				
def set1x1Serial(unitID, serialNumber):
	queryName = 'Scheduling/set1x1SerialByID'
	params = {'id': unitID, 'serialNumber': serialNumber}
	if clientScope():
		system.db.runNamedQuery(queryName, params)
	else:
		system.db.runNamedQuery(projectName, queryName, params)
	
def getLast1x1Serial(orderNumber):
	queryName = 'Scheduling/getLast1x1Serial'
	params = {'orderNumber': orderNumber}
	if clientScope():
		lastSerial = system.db.runNamedQuery(queryName, params)
	else:
		lastSerial = system.db.runNamedQuery(projectName, queryName, params)
	return lastSerial
			
def copyAllCarryOvers():
	for line in activeFGLines:
		linePath = getLinePath(line)
		copyCarryOvers(line, linePath)
			
def copyCarryOvers(prodLine, linePath):
	print 'copyCarryOvers', prodLine
	activeSeq = system.tag.readBlocking(linePath + '/PLC SCHED/Pre-blackout Active SeqNr')[0].value
	print ' active seq', str(activeSeq)
	activeSeq = str(activeSeq).zfill(14)
	carryOvers = None
	sched = system.tag.readBlocking(linePath + '/PLC SCHED/Pre-blackout Schedule')[0].value
	# starting point to look for D2 carry overs. first non straggler seq, subtract 1 to include it in results
	searchSeq = getStartSeq(linePath) - 1
	for row in range(sched.rowCount):
		rowSeq = sched.getValueAt(row, 'SEQNR')
		if int(rowSeq) >= int(activeSeq):
			orderNumber = sched.getValueAt(row, 'ORDERNUMBER')
			print orderNumber
			material = sched.getValueAt(row, 'MATERIAL')
			delQty = sched.getValueAt(row, 'DELIVEREDQTY')
			dateOffSet = sched.getValueAt(row, 'DATE OFFSET')

			carryOvers = get1x1CarryOvers(orderNumber, delQty) # unitnums > delQty with scans
			carryOverOrders = []
			scans = get1x1Scans(orderNumber)
			hasScans = scans is not None and scans.rowCount > 0
			if dateOffSet == 0:	
				if hasScans:
					nextOrder, existingScans, searchSeq = checkCopyCarryOvers(prodLine, linePath, material, orderNumber, carryOvers, searchSeq)
					if nextOrder != '':
						carryOverOrders.append(nextOrder)
						
						# potentially repopulate over production scans (carry over and overproduction)
						if existingScans is not None and existingScans.rowCount > 0:
							
							startUnit = carryOvers.rowCount + 1 + 1
							endUnit = existingScans.rowCount + startUnit 
							print 'startUnit', startUnit, 'endUnit', endUnit
							destinations = getOverProdDestinations(nextOrder, startUnit, endUnit)
							copyMultiple(nextOrder, existingScans, destinations)
#							for row in range(existingScans.rowCount):
#								print 'copy timestamps'
#								exSerial = existingScans.getValueAt(row, 'SerialNumber')
#								exScanner1 = existingScans.getValueAt(row, 'BeginDateTime')
#								exScanner2 = existingScans.getValueAt(row, 'EndDateTime')
#								exScheduleState = existingScans.getValueAt(row, 'ScheduleState')
#								
#								set1x1Scans(nextOrder, exScanner1, exScanner2, exScheduleState, exSerial)
			else:
				print 'overproduction'
				# over production
				if rowSeq == activeSeq:
					print ' rowseq == active'
					print ' delQTy', delQty
					carryOverPath = linePath + '/PLC SCHED/CarryOvers'
					carryOvers = system.tag.readBlocking(carryOverPath)[0].value
					print ' before delete', carryOvers
					if carryOvers is not None and carryOvers.rowCount > 0:
						rowsToDelete = []
						for row in range(carryOvers.rowCount):
							unitNum = carryOvers.getValueAt(row, 'Name')
							if int(unitNum) <= delQty:
								rowsToDelete.append(row)
						print ' rowstodelete', rowsToDelete
						carryOvers = system.dataset.deleteRows(carryOvers, rowsToDelete)
						print ' after delete', carryOvers
				if delQty > 0 and orderNumber not in carryOverOrders:
					if carryOvers is not None and carryOvers.rowCount > 0:
						print ' iterating', carryOvers
						for row in range(carryOvers.rowCount):
							unit = str(row + 1)
							serial = carryOvers.getValueAt(row, 'SerialNumber')
							print serial
							scanner1 = carryOvers.getValueAt(row, 'BeginDateTime')
							scanner2 = carryOvers.getValueAt(row, 'EndDateTime')
							state = carryOvers.getValueAt(row, 'ScheduleState')
							print '  copying', unit, serial, scanner1, scanner2, state	
							copy1x1Unit(unit, orderNumber, serial, scanner1, scanner2, state)		
				
def checkCopyCarryOvers(prodLine, linePath, material, orderNumber, carryOvers, searchSeq):		
	print '  ', 'checkCopyCarryOvers'
	#lastSerial = ''
	#lastUnit = ''
	nextOrder = ''
	existingScans = None
	if carryOvers is not None and carryOvers.rowCount > 0:
		reqDate = system.tag.readBlocking(linePath+'/SAP/D2/ReqDate')[0].value
		orderInfo = shared.Lodestar.R3.Scheduling.getReqDateModelOrderBySeq(prodLine, material, reqDate, searchSeq)
		if orderInfo is None or orderInfo.rowCount <= 0:
			shared.Lodestar.R3.Log.insertSAPLogger('Planned Orders', 'Sync', 'Could not find D2 Order for ' + material, 1)
		else:
			nextOrder = orderInfo.getValueAt(0, 'ORDERNUMBER')
			searchSeq = orderInfo.getValueAt(0, 'SEQNR')
			searchSeq = int(searchSeq)
			
			shared.Lodestar.R3.Log.insertSAPLogger('Planned Orders', 'Sync', 'Copying 1x1 carry overs from '+orderNumber+' to '+nextOrder, 1)
			existingScans = get1x1Scans(nextOrder)
			# copy carrovers into nextOrder
			
			numUnits = carryOvers.rowCount
			destinations = getCarryOverDestinations(nextOrder, numUnits)
			try:
				copyMultiple(nextOrder, carryOvers, destinations)
			except:
				shared.Lodestar.R3.Log.insertSAPLogger('Planned Orders', 'Sync', 'Error copying carry overs', 2)
				raise
	else:
		shared.Lodestar.R3.Log.insertSAPLogger('Planned Orders', 'Sync', 'No carry overs identified', 1)
	return nextOrder, existingScans, searchSeq

def get1x1CarryOvers(orderNumber, deliveredQty):
	queryName = 'Scheduling/get1x1CarryOvers'
	params = {'orderNumber': orderNumber, 'deliveredQty': deliveredQty}
	if clientScope():
		results = system.db.runNamedQuery(queryName, params)
	else:
		results = system.db.runNamedQuery(projectName, queryName, params)
	return results
	
def copy1x1Unit(unit, orderNumber, serial, scanner1, scanner2, state):
	queryName = 'Scheduling/copy1x1Unit'
	params = {'unit': unit, 'orderNumber': orderNumber, 'serial': serial, 'scanner1': scanner1, 'scanner2': scanner2, 'state': state}
	
	if clientScope():
		system.db.runNamedQuery(queryName, params)
	else:
		system.db.runNamedQuery(projectName, queryName, params)	
		
def addUnitsTo1x1(orderNumber, numUnits):
	print 'addUnitsTo1x1() ', orderNumber, numUnits
	#lastSerial = getLast1x1Serial(orderNumber)
	
	addMultipleUnits(orderNumber, numUnits)
#	for i in range(numUnits):
#		if lastSerial is not None:
#			newSerial = shared.Lodestar.R3.Serial.incrementSerial(lastSerial)
#		else:
#			newSerial = None
#		insert1x1Unit(orderNumber, None)
		
		#lastSerial = newSerial
	#return lastSerial
	
def insert1x1Unit(orderNumber, newSerial):
	print 'insert1x1Unit() ', orderNumber
	queryName = 'Scheduling/insertExtra1x1'
	params = {'orderNumber': orderNumber, 'newSerial': newSerial}
	if clientScope():
		system.db.runNamedQuery(queryName, params)
	else:	
		system.db.runNamedQuery(projectName, queryName, params)
		
def deleteUnitsFrom1x1(orderNumber, newQty):
	print 'deleteUnitsFrom1x1()'
	queryName = 'Scheduling/deleteExtra1x1'
	params = {'orderNumber': orderNumber, 'qty': newQty}
	if clientScope():
		system.db.runNamedQuery(queryName, params)
	else:
		system.db.runNamedQuery(projectName, queryName, params)
	# new last serial as starting point for serial number correction
	#return getLast1x1Serial(orderNumber)
	
def setScanner1Time(model, serialNumber):
	queryName = 'Scheduling/update1x1S1'
	params = {'model': model, 'serialNumber': serialNumber}
	if clientScope():
		results = system.db.runNamedQuery(queryName, params)
	else:
		results = system.db.runNamedQuery(projectName, queryName, params)
	return results
	
def setScanner2Time(model, serialNumber):
	queryName = 'Scheduling/update1x1S2'
	params = {'model': model, 'serialNumber': serialNumber}
	if clientScope():
		results = system.db.runNamedQuery(queryName, params)
	else:
		results = system.db.runNamedQuery(projectName, queryName, params)
	return results
	
def setOrderS1(orderNumber, material, serialNumber):
	queryName = 'Scheduling/updateOrderS1'
	params = {'orderNumber': orderNumber, 'serialNumber': serialNumber, 'model': material}
	if clientScope():
		results = system.db.runNamedQuery(queryName, params)
	else:
		results = system.db.runNamedQuery(projectName, queryName, params)
	return results
	
def setOrderS2(orderNumber, material, serialNumber):
	queryName = 'Scheduling/updateOrderS2'
	params = {'orderNumber': orderNumber, 'serialNumber': serialNumber, 'model': material}
	if clientScope():
		results = system.db.runNamedQuery(queryName, params)
	else:
		results = system.db.runNamedQuery(projectName, queryName, params)
	return results
		
def setOrderS1Miss(orderNumber, material, serialNumber):
	queryName = 'Scheduling/updateOrderS1Miss'
	params = {'orderNumber': orderNumber, 'serialNumber': serialNumber, 'model': material}
	if clientScope():
		results = system.db.runNamedQuery(queryName, params)
	else:
		results = system.db.runNamedQuery(projectName, queryName, params)
	return results
	
def setOrderS2Miss(orderNumber, material, serialNumber):
	queryName = 'Scheduling/updateOrderS2Miss'
	params = {'orderNumber': orderNumber, 'serialNumber': serialNumber, 'model': material}
	if clientScope():
		results = system.db.runNamedQuery(queryName, params)
	else:
		results = system.db.runNamedQuery(projectName, queryName, params)
	return results
		
def update1x1MissedS1(model, serial):
	queryName = 'Scheduling/update1x1MissedS1'
	params = {'model': model, 'serial': serial}
	if clientScope():
		system.db.runNamedQuery(queryName, params)
	else:
		system.db.runNamedQuery(projectName, queryName, params)
		
def update1x1MissedS2(model, serial):
	queryName = 'Scheduling/update1x1MissedS2'
	params = {'model': model, 'serial': serial}
	if clientScope():
		system.db.runNamedQuery(queryName, params)
	else:
		system.db.runNamedQuery(projectName, queryName, params)
		
def get1x1Scans(orderNumber):
	queryName = 'Scheduling/get1x1ScanCount'
	params = {'workOrder': orderNumber}
	if clientScope():
		scans = system.db.runNamedQuery(queryName, params)
	else:
		scans = system.db.runNamedQuery(projectName, queryName, params)
	return scans
	
def set1x1Scans(orderNumber, scanner1, scanner2, scheduleState, serialNumber):
	queryName = 'Scheduling/update1x1Scans'
	params = {'scanner1': scanner1, 'scanner2': scanner2, 'orderNumber': orderNumber, 'serialNumber': serialNumber, 'scheduleState': scheduleState}
	if clientScope():
		system.db.runNamedQuery(queryName, params)
	else:
		system.db.runNamedQuery(projectName, queryName, params)
		
def save1x1s():
	lines = shared.Lodestar.R3.Config.activeFGLines
	for line in lines:
		linePath = shared.Lodestar.R3.Config.getLinePath(line)
		saveCurrent1x1(linePath)
		
def saveCurrent1x1(linePath):	
	basePath = '%s/PLC SCHED/' %linePath

	indexPath = '%sS2/CurrentIndex' %basePath
	
	currentIndex = system.tag.readBlocking(indexPath)[0].value
	currentModelInfo = shared.Lodestar.R3.Tag.getModelInfo(linePath, str(currentIndex))
	curSeq = currentModelInfo['seqNr']
	
	dateOffset = currentModelInfo['dateOffset']
	
	orderNumber = currentModelInfo['orderNumber']
	oneByOne = shared.Lodestar.R3.OneByOne.get1x1(orderNumber)
	
	tagPath = '%s/CarryOvers' %basePath
	system.tag.writeBlocking(tagPath, oneByOne)
	
def getOrderRepairs(prodLine, orderNumber, engModel):
	queryName = 'Scheduling/getOrderRepairs'
	params = {'prodLine': prodLine, 'orderNumber': orderNumber, 'engModel': engModel}
	if clientScope():
		repairs = system.db.runNamedQuery(queryName, params)
	else:
		repairs = system.db.runNamedQuery(projectName, queryName, params)
	return repairs
	
def updateUnitRepairs(orderNumber, serialNumber, defectLogged, repairComplete):
	queryName = 'Scheduling/updateUnitRepairs'
	params = {'orderNumber': orderNumber, 'serialNumber': serialNumber, 'defectLoggedTime': defectLogged, 'repairCompleteTime': repairComplete}
	if clientScope():
		system.db.runNamedQuery(queryName, params)
	else:
		system.db.runNamedQuery(projectName, queryName, params)
		
def transferRepairs(prodLine):
	linePath = shared.Lodestar.R3.Config.getLinePath(prodLine)
	
	inProduction = system.tag.readBlocking(linePath+'/PLC SCHED/In Production')[0].value
	if inProduction:
		modelInfo = system.tag.readBlocking(linePath+'/PLC SCHED/ModelInfoDs')[0].value
		# get current day orders only
		modelInfo = shared.Lodestar.R3.Util.filterDataSet(modelInfo, "{'DATE OFFSET': '0'}")
		
		for row in range(modelInfo.rowCount):
			orderNumber = modelInfo.getValueAt(row, 'ORDERNUMBER')
			engModel = modelInfo.getValueAt(row, 'MODEL')
			
			print orderNumber, engModel
			orderRepairs = shared.Lodestar.R3.OneByOne.getOrderRepairs(prodLine, orderNumber, engModel)
			if orderRepairs is not None and orderRepairs.rowCount > 0:
				for row in range(orderRepairs.rowCount):
					serialNumber = orderRepairs.getValueAt(row, 'UNIT_SERIAL_NUMBER')
					defectLogged = orderRepairs.getValueAt(row, 'CREATED_ON')
					repairComplete = orderRepairs.getValueAt(row, 'REPAIR_COMPLETED_DATE')
					print serialNumber, defectLogged, repairComplete
					
					shared.Lodestar.R3.OneByOne.updateUnitRepairs(orderNumber, serialNumber, defectLogged, repairComplete)
					
					
def cacheS1Scan(prodLine, orderNumber, model, serialNumber, scanTime):						
	site = shared.Lodestar.R3.Config.getSiteName()
	cachePath = 'Whirlpool MES/%s/Assembly/%s/PLC SCHED/S1Cache' %(site, prodLine)
	cache = system.tag.readBlocking(cachePath)[0].value
	
	row = [orderNumber, serialNumber, model, scanTime, None]
	cache = system.dataset.addRow(cache, row)
	
	system.tag.writeBlocking(cachePath, cache)
	
def cacheS2Scan(prodLine, orderNumber, model, serialNumber, scanTime):
	site = shared.Lodestar.R3.Config.getSiteName()
	cachePath = 'Whirlpool MES/%s/Assembly/%s/PLC SCHED/S2Cache' %(site, prodLine)
	cache = system.tag.readBlocking(cachePath)[0].value
	
	for row in range(cache.rowCount):
		scanTimeVal = cache.getValueAt(row, 'S2')
		if scanTimeVal is None:
			cache = system.dataset.setValue(cache, row, 'S2', scanTime)
			break
	system.tag.writeBlocking(cachePath, cache)
	
	
def cacheScan(prodLine, scanner, scan):						
	orderNumber, model, serialNumber, scanTime = scan
	
	site = shared.Lodestar.R3.Config.getSiteName()
	cachePath = 'Whirlpool MES/%s/Assembly/%s/PLC SCHED/%s/Cache' %(site, prodLine, scanner)
	cache = system.tag.readBlocking(cachePath)[0].value
	
	row = [orderNumber, serialNumber, model, scanTime]
	cache = system.dataset.addRow(cache, row)
	
	system.tag.writeBlocking(cachePath, cache)
	
def writeAllCaches():
	site = shared.Lodestar.R3.Config.getSiteName()
	lines = shared.Lodestar.R3.Config.activeFGLines
	scanners = ['S1', 'S2']
	for line in lines:
		writingPath = 'Whirlpool MES/%s/Assembly/%s/PLC SCHED/WritingCache' %(site, line)
		writingNow = system.tag.readBlocking(writingPath)[0].value
		if not writingNow:
			for scanner in scanners:
				cachePath = 'Whirlpool MES/%s/Assembly/%s/PLC SCHED/%s/Cache' %(site, line, scanner)
				writeCache(line, scanner, cachePath)
				
#def writeCache(prodLine, scanner, cachePath):
#	items = cachePath.split('/')
#	basePath = '/'.join(items[:-1])
#	writingPath = '%s/WritingCache' %basePath
#	system.tag.writeBlocking(writingPath, 1)
#	
#	projectName = shared.Lodestar.R3.Config.getProjectName()
#	
#	cache = system.tag.readBlocking(cachePath)[0].value
#	clearedCache = system.dataset.clearDataset(cache)
#	system.tag.writeBlocking(cachePath, clearedCache)
#	
#	for row in range(cache.rowCount):
#		orderNumber = cache.getValueAt(row, 'OrderNumber')
#		serialNumber = cache.getValueAt(row, 'SerialNumber')
#		if serialNumber is None:
#			query = 'Scheduling/updateOrder%sMiss' %scanner
#		else:
#			query = 'Scheduling/updateOrder%sTime' %(scanner)
#		model = cache.getValueAt(row, 'Model')
#		scanTime = cache.getValueAt(row, 'ScanTime')
#		
#		params = {'orderNumber': orderNumber, 'serialNumber': serialNumber, 'model': model, 'scanTime': scanTime}
#		if shared.Lodestar.R3.Util.clientScope():
#			results = system.db.runNamedQuery(query, params)
#		else:
#			results = system.db.runNamedQuery(projectName, query, params)
#			
#		if results <= 0:
#			msg = 'Cannot assign model:' + str(model) + ' and serial:' + str(serialNumber) + ' for ' + prodLine + ' ' + scanner + ' to order: ' + str(orderNumber)
#			shared.Lodestar.R3.Log.insertSAPLogger('Planned Orders', 'ERROR', msg, 2)
#	system.tag.writeBlocking(writingPath, 0)
	
def writeCache(prodLine, scanner, cachePath):
	items = cachePath.split('/')
	basePath = '/'.join(items[:-1])
	
	writingPath = '%s/WritingCache' %basePath
	system.tag.writeBlocking(writingPath, 1)
	
	cache = system.tag.readBlocking(cachePath)[0].value
	clearedCache = system.dataset.clearDataset(cache)
	system.tag.writeBlocking(cachePath, clearedCache)
	
	projectName = shared.Lodestar.R3.Config.getProjectName()
	schedPath = '/'.join(items[:-2])
	
	seqStart = system.tag.readBlocking(schedPath + '/Starting Sequence')[0].value
	
	sapPath = '/'.join(items[:-3]) + '/SAP'
	
	startDate = system.tag.readBlocking(sapPath + '/D1/ReqDate')[0].value
	endDate = system.tag.readBlocking(sapPath + '/D2/ReqDate')[0].value
	
	sapSource = shared.Lodestar.R3.Config.getActiveSAPSource()
	for row in range(cache.rowCount):
		#orderNumber = cache.getValueAt(row, 'OrderNumber')
		serialNumber = cache.getValueAt(row, 'SerialNumber')
		if serialNumber is None:
			query = 'Scheduling/update1x1Missed%s' %scanner
		else:
			query = 'Scheduling/update1x1%s' %(scanner)
		model = cache.getValueAt(row, 'Model')
		scanTime = cache.getValueAt(row, 'ScanTime')
		
		params = {'serialNumber': serialNumber, 'model': model, 'scanTime': scanTime, 'prodLine': prodLine, 'startDate': startDate, 'endDate': endDate, 'seqStart': seqStart, 'sapSource': sapSource}
		if shared.Lodestar.R3.Util.clientScope():
			results = system.db.runNamedQuery(query, params)
		else:
			results = system.db.runNamedQuery(projectName, query, params)
			
		if results <= 0:
			msg = 'Cannot assign model:' + str(model) + ' and serial:' + str(serialNumber) + ' for ' + prodLine + ' ' + scanner
			shared.Lodestar.R3.Log.insertSAPLogger('Planned Orders', 'ERROR', msg, 2)
	system.tag.writeBlocking(writingPath, 0)
	
def checkCacheOrder(prodLine, fromOrder, toOrder):
	site = shared.Lodestar.R3.Config.getSiteName()

	scanners = ['S1', 'S2']	
	for scanner in scanners:
		cachePath = 'Whirlpool MES/%s/Assembly/%s/PLC SCHED/%s/Cache' %(site, prodLine, scanner)
		cache = system.tag.readBlocking(cachePath)[0].value
		for row in range(cache.rowCount):
			orderNumber = cache.getValueAt(row, 'OrderNumber')
			if orderNumber == fromOrder:
				cache = system.dataset.setValue(cache, row, 'OrderNumber', toOrder)
		system.tag.writeBlocking(cachePath, cache)
		
def addMultipleUnits(orderNumber, numUnits):
	lastUnit = getLastUnit(orderNumber)
	unitNum = int(lastUnit.getValueAt(0, 'Name'))
	mat = lastUnit.getValueAt(0, 'Material')
	prodLine = lastUnit.getValueAt(0,'ProdLine')
	seqNr = lastUnit.getValueAt(0, 'SequenceNumber')
	
	startDate = lastUnit.getValueAt(0, 'OriginalBeginDateTime')
	endDate = lastUnit.getValueAt(0, 'OriginalEndDateTime')
	duration = system.date.secondsBetween(startDate, endDate)
	
	description = ''
	schedId = 'None'
	woId = 'None'
	missedScans = 0
	scheduleState = 'Scheduled'
	timeStamp = system.date.now()
	enabled = True
		
	quantityChunks = list(orderChunker(149, range(numUnits)))
	# prepared queries have a maximum of 2100 paramaters
	# at 14 params per unit, we'll break it up into inserts of 149 ( (2100 / 14) - 1 )
	
	for chunk in quantityChunks:
		query = """
			INSERT INTO Schedule1x1
				(Name
				,Description
				,Material
				,WorkOrder
				,ProdLine
				,SequenceNumber
				,OriginalBeginDateTime
				,OriginalEndDateTime
				,ScheduleState
				,Timestamp
				,Enabled
				,ScheduleUUID
				,WorkOrderUUID
				,MissedScans
			)
			VALUES
		"""
		
				
		values = []
		valueStatements = []
		for i in chunk:
			valueStatements.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			
			unitNum += 1
			startDate = system.date.addSeconds(startDate, duration)
			endDate = system.date.addSeconds(endDate, duration)
			
			newUnit = [unitNum, description, mat, orderNumber, prodLine, seqNr, startDate, endDate, scheduleState, timeStamp, enabled, schedId, woId, missedScans]
			values.extend(newUnit)
			
		statements = ','.join(valueStatements)
		query += statements

		system.db.runPrepUpdate(query, values)


def getLastUnit(orderNumber):
	query = 'Scheduling/getLastUnit'
	params = {'orderNumber': orderNumber}
	if clientScope():
		return system.db.runNamedQuery(query, params)
	else:	
		return system.db.runNamedQuery(projectName, query, params)
		
def copyMultiple(nextOrder, carryOvers, destinations):
	print 'carryOvers ', carryOvers
	print 'destinations ', destinations
	#UPDATE e
	#SET hire_date = t.hire_date
	#FROM dbo.employee e
	#JOIN (
	#    VALUES
	#        ('PMA42628M', '1979-03-15'),
	#        ('PSA89086M', '1988-12-22')
	#) t (emp_id, hire_date) ON t.emp_id = e.emp_id
	

	#carryOvers = stripNoScans(carryOvers)
	#print 'stripped', carryOvers
	numUnits = carryOvers.rowCount
	#destinations = getCarryOverDestinations(nextOrder, numUnits)
	#print 'destinations', destinations
	quantityChunks = list(orderChunker(299, range(numUnits)))	
	# prepared queries have a maximum of 2100 paramaters
	# at 7 params per unit, we'll break it up into inserts of 299 ( (2100 / 7) - 1 )
	
	for chunk in quantityChunks:
		updateStatement = """
		UPDATE 
			s
		SET	
			SerialNumber = t.SerialNumber,
			BeginDateTime = t.BeginDateTime,
			EndDateTime = t.EndDateTime,
			ScheduleState = t.ScheduleState,
			MissedScans = t.MissedScans		
		FROM
			Schedule1x1 s 
		"""
		
		joinStatement = """
		JOIN (
			VALUES
		"""
		
		valueLists = []
		params = []
		for i in chunk:
			valueList = """
			(?, ?, ?, ?, ?, ?)
			"""
			valueLists.append(valueList)
			
			unitId = destinations.getValueAt(i, 'id') 
#			unitNum = str(i + 1)
			serial = carryOvers.getValueAt(i, 'SerialNumber')	
			scanner1 = carryOvers.getValueAt(i, 'BeginDateTime')
			scanner2 = carryOvers.getValueAt(i, 'EndDateTime')
			state = carryOvers.getValueAt(i, 'ScheduleState')
			missedScans = carryOvers.getValueAt(i, 'MissedScans')
	
			print unitId, serial, scanner1, scanner2, state, missedScans
			params.extend([unitId, serial, scanner1, scanner2, state, missedScans])
			
		valueStatement = ",".join(valueLists) + """
		)"""
		
		onStatement = " t (id, SerialNumber, BeginDateTime, EndDateTime, ScheduleState, MissedScans) ON t.id = s.id"
		
		query = updateStatement + joinStatement + valueStatement + onStatement
		print query
		system.db.runPrepUpdate(query, params)
			
def getCarryOverDestinations(workOrder, numUnits):
	query = 'Scheduling/getCarryDestinations'
	params = {'workOrder': workOrder, 'numUnits': numUnits}
	
	if clientScope():
		return system.db.runNamedQuery(query, params)
	else:
		return system.db.runNamedQuery(projectName, query, params)

def getOverProdDestinations(workOrder, startUnit, endUnit):
	query = 'Scheduling/getOverProdDestinations'
	params = {'workOrder': workOrder, 'startUnit': startUnit, 'endUnit': endUnit}
	if clientScope():
		return system.db.runNamedQuery(query, params)
	else:
		return system.db.runNamedQuery(projectName, query, params)
	if clientScope():
		return system.db.runNamedQuery(query, params)
	else:
		return system.db.runNamedQuery(projectName, query, params)
		
def getStartSeq(linePath):
	seqPath = '%s/PLC SCHED/Starting Sequence' %linePath
	return system.tag.readBlocking(seqPath)[0].value
	

def shouldCarryOver():
	# determine if carryover should occur
	# does not need to take place when there is a non production day between
	# two production days (eg, Sunday with prod on Sat and Mon)
	today = system.date.now()
	todReq = system.date.midnight(today)
	#print 'today', todReq
		
	lines = shared.Lodestar.R3.Config.activeFGLines
	for line in lines:
		linePath = shared.Lodestar.R3.Config.getLinePath(line)
		reqPath = '%s/SAP/D1/ReqDate' %linePath
		d1 = system.tag.readBlocking(reqPath)[0].value
		#print line, 'd1', d1
			
		# if d1 is is today (as REQDATE), we are in a production day and need to carryover
		should = system.date.secondsBetween(todReq, d1) == 0
		if should:
			# if any one line is running we need to carry over
			return should
	return False