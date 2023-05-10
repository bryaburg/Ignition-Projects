import re
from java.awt import Color
from shared.Lodestar.R3.Config import projectName, activeFGLines, getActiveSAPSource, getLinePath, activeFGLines, getLineID
from shared.Lodestar.R3.Util import clientScope, dsColToList, isStraggler
from shared.Lodestar.R3.Production import getNextProductionDays, getScheduleMeta, getShiftSchedule, getBreakEvents
from shared.Lodestar.R3.Log import insertSAPLogger
from shared.Lodestar.R3.OneByOne import getSerialRange, getCompletedQuantity
from shared.Lodestar.R3.Material import getMaterialDescription, getEngModel
from shared.Lodestar.R3.Scheduling import getSAPSchedule, getScheduledTotal

primarySAPSource = shared.Lodestar.R3.Config.getPrimarySAPSource()

def populateAllModelInfo():
	insertSAPLogger('Planned Orders', 'Sync', 'Sending schedule to PLC', 1)
	for prodLine in activeFGLines:
		populateModelInfo(prodLine)
		
def populateModelInfo(prodLine):
	# get today's orders for the specified prod line
	date = system.date.setTime(system.date.now(), 0,0,0)
	dateStr = system.date.format(date, 'yyyy-MM-dd HH:mm:ss')
	sapSource = getActiveSAPSource()
	params = {'prodLine': prodLine, 'reqDate':dateStr, 'sapSource':sapSource}
	
	if clientScope():
		orders = system.db.runNamedQuery('Scheduling/getNext25Orders', params) 
	else:
		orders = system.db.runNamedQuery(projectName, 'Scheduling/getNext25Orders', params)
	#print orders
	linePath = getLinePath(prodLine)
	basePath = '%s/PLC SCHED/ModelInfo' %linePath
	dsPath = basePath+'Ds'
	
	forceFullPath = 'R3/Force_Full_Schedule'
	forceFull = system.tag.readBlocking(forceFullPath)[0].value
	
	if forceFull:
		# this is the first write after new schedule, must write the entire UDT
		# in case the PLC index does not drop to 0
		curIndex = 0
		curSeq = 0
		system.tag.writeBlocking(forceFullPath, 0)
	else:
		indexPath = '%s/PLC SCHED/S2/CurrentIndex' %linePath
		
		curIndex = system.tag.readBlocking(indexPath)[0].value
		#print curIndex
		
		seqPath = '%s/PLC SCHED/ModelInfo/%s/SeqNr' %(linePath, str(curIndex))
		curSeq = system.tag.readBlocking(seqPath)[0].value
	#print curSeq
	
	pathsToWrite = []
	valsToWrite = []
	dsHeaders = [ 'MODEL', 'COUNT', 'SEQUENTIAL ORDER', 'ORDERNUMBER', 'SEQNR', 'DATE OFFSET', 'MATERIAL']
	dsVals = []

	stragglerPath = '%s/PLC SCHED/Stragglers' %linePath
	stragglers = system.tag.readBlocking(stragglerPath)[0].value
	
	orderSeq = 0
	rowToWrite = curIndex
	for row in range(orders.getRowCount()):
	#	print 'row', row
	
	#	print ' orderseq', orderSeq
	#	print ' rowToWrite', rowToWrite
		if orderSeq < 20: # only 20 slots in PLC tag, query returns 25 for potential straggler adjustment
			
			if not isStraggler(stragglers, orders, row):
				orderSeq += 1
			
				seqNr = orders.getValueAt(row, 'SEQNR')
									
				orderNumber = orders.getValueAt(row, 'ORDERNUMBER')
				model = orders.getValueAt(row, 'MATERIAL')
				alternative = orders.getValueAt(row, 'ALTBOM')
				count = orders.getValueAt(row, 'REMQTY')
				reqDate = orders.getValueAt(row, 'REQDATE')
				reqDate = system.date.parse(reqDate, 'yyyy-MM-dd HH:mm:ss')
				dateCode = system.date.daysBetween(date, reqDate)
				
				engModel = getEngModel(prodLine, model, alternative)

				vals = [engModel, count, orderSeq, orderNumber, int(seqNr), dateCode, model]
				dsVals.append(vals)
				
				if int(seqNr) >= curSeq:
					# should only write to slots >= current run
					#print ' appending', seqNr

					valsToWrite.extend(vals)
					
					# build tag paths
					item = str(rowToWrite)
					modelPath = basePath + '/%s/Model' %item
					countPath = basePath + '/%s/Count' %item
					orderPath = basePath + '/%s/Order' %item
					numberPath = basePath + '/%s/OrderNumber' %item
					seqNrPath = basePath + '/%s/SeqNr' %item
					datePath = basePath + '/%s/Date' %item
					matPath = basePath + '/%s/Material' %item
					
					rowToWrite += 1
	
					pathsToWrite.extend([modelPath, countPath, orderPath, numberPath, seqNrPath, datePath, matPath])
	#	print '___'
	system.tag.writeAll(pathsToWrite, valsToWrite)
	#print pathsToWrite
	#print valsToWrite
	#print 'equal', len(pathsToWrite) == len(valsToWrite)
	ds = system.dataset.toDataSet(dsHeaders, dsVals)
	system.tag.writeBlocking(dsPath, ds)

def getModelInfo(linePath, item):
	basePath = '%s/PLC SCHED/ModelInfo' %(linePath)
	
	modelPath = basePath + '/%s/Model' %item
	countPath = basePath + '/%s/Count' %item
	orderPath = basePath + '/%s/Order' %item
	numberPath = basePath + '/%s/OrderNumber' %item
	seqNrPath = basePath + '/%s/SeqNr' %item
	datePath = basePath + '/%s/Date' %item
	matPath = basePath + '/%s/Material' %item
	uuidPath = basePath + '/%s/Schedule UUID' %item
	
	paths = [modelPath, countPath, orderPath, numberPath, seqNrPath, datePath, matPath, uuidPath]
	values = system.tag.readAll(paths)
	return {
		'model': values[0].value,
		'count': values[1].value,
		'order': values[2].value,
		'orderNumber': values[3].value,
		'seqNr': values[4].value,
		'dateOffset': values[5].value,
		'material': values[6].value,
		'scheduleUUID': values[7].value
	}

def getModelInfoByMaterial(prodLine):
	linePath = getLinePath(prodLine)
	modelInfo = system.tag.readBlocking(linePath+'/PLC SCHED/ModelInfoDs')[0].value
	# get current day orders only
	modelInfo = shared.Lodestar.R3.Util.filterDataSet(modelInfo, "{'DATE OFFSET': '0'}")
	
	 # group orders by material
	orders = {} # {model: [order, order...],...}
	for row in range(modelInfo.rowCount):
		orderNumber = modelInfo.getValueAt(row, 'ORDERNUMBER')
		material = modelInfo.getValueAt(row, 'MATERIAL')
		
		if material in orders.keys():
			matOrders = orders[material]
			matOrders.append(orderNumber)
			orders[material] = matOrders
		else:
			orders[material] = [orderNumber]
	return orders
		
def populateAllScheduleTags(startDate=None):
	holdCache = system.tag.readBlocking('R3/CacheScans')[0].value
	if not holdCache:
		if startDate == None:
			startDate = system.date.setTime(system.date.now(),0,0,0)
	

		sapSource = getActiveSAPSource()
	
		for line in activeFGLines:
			linePath = getLinePath(line)
			startDate = system.tag.readBlocking(linePath + '/SAP/D1/ReqDate')[0].value
			dates, dPaths = getNextProductionDays(startDate, line)
			for i in range(len(dates)):
				populateScheduleTags(line, linePath, dates[i], dPaths[i], sapSource)
						
def populateScheduleTags(prodLine, linePath, reqDate, dPath, sapSource):
	# populates the dataset tags backing the Power Table and Equipment Schedule View on Assembly window
	currentOrderPath = '%s/Run Counter/Order Number' %linePath
	currentOrder = system.tag.readBlocking(currentOrderPath)[0].value
	
	indexPath = '%s/PLC SCHED/S2/CurrentIndex' %linePath
	curIndex = system.tag.readBlocking(indexPath)[0].value
	
	seqPath = '%s/PLC SCHED/ModelInfo/%s/SeqNr' %(linePath, str(curIndex))
	currentSeq = system.tag.readBlocking(seqPath)[0].value
	
	#currentSeqPath = '%s/Run Counter/Sequence Number' %linePath
	#currentSeq = system.tag.readBlocking(currentSeqPath)[0].value
	
	lineID = getLineID(prodLine)
	tablePath = '/%s/SAP/%s/Orders' %(linePath, dPath)
	eventPath = '/%s/SAP/%s/ScheduleEvents' %(linePath, dPath)
	breakPath = '/%s/SAP/%s/BreakEvents' %(linePath, dPath)
	
	totalSchedPath = '/%s/SAP/%s/Total Scheduled' %(linePath, dPath)
	totalPLCPath = '/%s/SAP/%s/Total PLC' %(linePath, dPath)
	totalSAPPath = '/%s/SAP/%s/Total SAP' %(linePath, dPath)
	totalDiffPath = '/%s/SAP/%s/Total Diff' %(linePath, dPath)
	
	data = getSAPSchedule(prodLine, reqDate, sapSource)

	# values for Equipment Schedule View
	eventHeaders = ['EventID', 'ItemID', 'StartDate', 'EndDate', 'Label', 'PctDone', 'Background']
	eventValues = []
	
	# values for Power Table
	tableHeaders = ['Status', 'Seq Nr', 'SAP Order', 'Material', 'Eng Model', 'Text', 'Description', 'Sched Start', 'Sched End', 'Sched Qty', 'PLC Qty', 'SAP Received Qty', 'PLC - SAP', 'Serial Range', '1x1', 'REQDATE']
	tableValues = []
	
	if data is not None and data.rowCount > 0:
		totalSched = 0
		totalPLC = 0
		totalSAP = 0
		totalDiff = 0
		for row in range(data.getRowCount()):
			eventID = data.getValueAt(row, 'id')
			
			order = data.getValueAt(row, 'OrderNumber')
			reqDate = data.getValueAt(row, 'REQDATE')
			estStartDate = data.getValueAt(row, 'EstimatedStartDateTime') # table
			estEndDate =  data.getValueAt(row, 'EstimatedEndDateTime') # table

			actStartDate = data.getValueAt(row, 'ActualStartDateTime')
			if actStartDate is None:
				startDate = estStartDate # schedule view
			else:
				startDate = actStartDate
				
			actEndDate = data.getValueAt(row, 'ActualEndDateTime')
			if actEndDate is None:
				endDate = estEndDate # schedule view
			else:
				endDate = actEndDate
			
			now = system.date.now()
			
			seqNr = data.getValueAt(row, 'SEQNR')
			#print 'seqNr', seqNr
			#print 'current', currentSeq
			if order == currentOrder:
				background = Color(0, 255, 0) # green
			elif int(str(seqNr)) > int(currentSeq):
				# future order
				background = Color(198, 255, 242) # light blue
			else:
				# past order
				background = Color(64, 100, 231) # dark blue
				
			serialRange = getSerialRange(order)
			
			mesQty = getCompletedQuantity(order)
			totalPLC += mesQty
			
			schedQty = data.getValueAt(row, 'Quantity')
			totalSched += schedQty
			
			sapQty = data.getValueAt(row, 'DELIVEREDQTY')
			totalSAP += sapQty
			
			diff = int(mesQty) - int(sapQty)
			totalDiff += diff
			
			producedQty = data.getValueAt(row, 'ProducedQuantity')
			if producedQty is None:
				producedQty = 0
			pctDone = (producedQty / float(schedQty)) * 100
			
			text = data.getValueAt(row, 'TEXT1')
			material = data.getValueAt(row, 'Material')		
			description = getMaterialDescription(material)
						
			altBOM = data.getValueAt(row, 'ALTBOM')

			engModel = getEngModel(prodLine, material, altBOM)
			
			completeStatus = ''
			if order == currentOrder:
				completeStatus = 'Current'
			else:
				if int(schedQty) != int(sapQty):
					completeStatus = 'Scheduled'
				else:
					completeStatus = 'Complete'
			link = '<html><a>View 1x1'		
			
			# remove the leading zeros from seqnr
			seqNr = data.getValueAt(row, 'SEQNR')
			regex = r'^0*(\d*)$'

			filteredSeqNr = re.search(regex, seqNr).group(1)
			label = filteredSeqNr + ' - ' + engModel

			#tableHeaders = [ 'Status', 'Seq Nr', 'SAP Order', 'Material', 'Eng Model', 'Text', 'Description', 'Sched Start', 'Sched End', 'Sched Qty', 'PLC Qty', 'SAP Received Qty', 'PLC - SAP', 'Serial Range', '1x1']
			tableValues.append([completeStatus, filteredSeqNr, order, material, engModel, text, description, estStartDate, estEndDate, schedQty, mesQty, sapQty, diff, serialRange, link, reqDate])
			eventValues.append([eventID, lineID, startDate, endDate, label, pctDone, background])
		
		tableDs = system.dataset.toDataSet(tableHeaders, tableValues)
		eventDs = system.dataset.toDataSet(eventHeaders, eventValues)
		breakDs = getBreakEvents(prodLine, reqDate)
	else:
		tableDs = system.tag.readBlocking(tablePath)[0].value
		tableDs = system.dataset.clearDataset(data)
		
		eventDs = system.tag.readBlocking(eventPath)[0].value
		eventDs = system.dataset.clearDataset(eventDs)
		
		breakDs = system.tag.readBlocking(breakPath)[0].value
		breakDs = system.dataset.clearDataset(breakDs)
		
		totalSched = 0
		totalPLC = 0
		totalSAP = 0
		totalDiff = 0
		
	system.tag.writeAll([tablePath, eventPath, breakPath, totalSchedPath, totalPLCPath, totalSAPPath, totalDiffPath], [tableDs, eventDs, breakDs, totalSched, totalPLC, totalSAP, totalDiff])
					
def populateAllDates(line, linePath, startDate=None):
	if startDate == None:
		startDate = system.date.setTime(system.date.now(),0,0,0)
	
	sapSource = getActiveSAPSource()

	#for line in activeFGLines:
	#linePath = getLinePath(line)
	shiftSchedule = getShiftSchedule(line)
			
	dates, dPaths = getNextProductionDays(startDate, line)
	for i in range(len(dates)):
		populateDateTag(linePath, line, dates[i], dPaths[i], sapSource)
		
def populateDateTag(linePath, line, date, dPath, sapSource=primarySAPSource):
	
	schedTotal = getScheduledTotal(line, date, sapSource)
	
	if schedTotal == None:
		schedTotal = 0
	meta = getScheduleMeta(line, date, schedTotal) 
	if meta is not None:
		prodStart = meta['startDate']
		prodEnd = meta['endDate']
	else:	
		pastDate = '1970-01-01 00:00:00'
		prodStart = prodEnd = system.date.parse(pastDate, 'yyyy-MM-dd HH:mm:ss')
		
	reqPath = linePath+'/SAP/'+dPath+'/ReqDate'
	startPath = linePath+'/SAP/'+dPath+'/ProductionStart'
	endPath = linePath+'/SAP/'+dPath+'/ProductionEnd'
	
	paths = [reqPath, startPath, endPath]
	values = [date, prodStart, prodEnd]
	system.tag.writeAll(paths, values)
	
def findStragglers():
	# "Stragglers" are orders that were completed on the line but not all units were reported to SAP in DELIVEREDQTY
	activeFGLines = shared.Lodestar.R3.Config.activeFGLines
	for prodLine in activeFGLines:
		linePath = getLinePath(prodLine)
		basePath = '%s/PLC SCHED/' %linePath
		
		activeSeqPath = '%sPre-blackout Active SeqNr' %basePath
		activeSeqNr = system.tag.readBlocking(activeSeqPath)[0].value # active sequence at time of blackout
		
		# pre blackout schedule
		currentSchedulePath = '%sPre-blackout Schedule' %basePath
		currentSchedule = system.tag.readBlocking(currentSchedulePath)[0].value
		
		carryOverHeaders = ['SEQNR', 'ORDERNUMBER', 'MATERIAL', 'ENGMODEL', 'REMQTY', 'DELIVEREDQTY', 'QTYDIFF']
		carryOvers = []
		for row in range(currentSchedule.getRowCount()):
			seqNr = currentSchedule.getValueAt(row, 'SEQNR')
			
			if int(seqNr) < activeSeqNr:
				# must be a sequence before the last of the day
				remQty = currentSchedule.getValueAt(row, 'REMQTY')
				delQty = currentSchedule.getValueAt(row, 'DELIVEREDQTY')
				
				qtyRemaining = remQty - delQty
				if qtyRemaining > 0: # line moved past the order (completed it) but not all units reported to SAP
					orderNumber = currentSchedule.getValueAt(row, 'ORDERNUMBER')
					seqNr = currentSchedule.getValueAt(row, 'SEQNR')
					material = currentSchedule.getValueAt(row, 'MATERIAL')
					engModel = currentSchedule.getValueAt(row, 'ENGMODEL')
					carryOvers.append([seqNr, orderNumber, material, engModel, remQty, delQty, qtyRemaining])
		stragglerDs = system.dataset.toDataSet(carryOverHeaders, carryOvers)
		stragglerPath = '%sStragglers' %basePath
		system.tag.writeBlocking(stragglerPath, stragglerDs)
			
def getStragglersList(linePath):
	stragglerPath = '%s/PLC SCHED/Stragglers' %linePath
	stragglersDs = system.tag.readBlocking(stragglerPath)[0].value
	return dsColToList(stragglersDs, 'ORDERNUMBER')

def writeCurrentSchedules():
	for line in activeFGLines:
		writeCurrentSchedule(line)
		
def writeCurrentSchedule(prodLine):
	linePath = getLinePath(prodLine)
	basePath = '%s/PLC SCHED/' %linePath
	tagPath = '%sPre-blackout Schedule' %basePath
	activeSeqPath = '%sPre-blackout Active SeqNr' %basePath
	indexPath = '%sS2/CurrentIndex' %basePath
	
	# store the current sequence for reference in findCarryOvers
	currentIndex = system.tag.readBlocking(indexPath)[0].value
	currentModelInfo = getModelInfo(linePath, str(currentIndex))
	seqNr = currentModelInfo['seqNr']
	#seqNr = system.tag.readBlocking(seqNrPath)[0].value
	system.tag.writeBlocking(activeSeqPath, seqNr)
	
	date = system.date.setTime(system.date.now(), 0,0,0)
	dateStr = system.date.format(date, 'yyyy-MM-dd HH:mm:ss')
	sapSource = getActiveSAPSource()
	params = {'prodLine': prodLine, 'reqDate':date, 'sapSource':sapSource}
	
	if clientScope():
		orders = system.db.runNamedQuery('Scheduling/getNext25Orders', params) 
	else:
		orders = system.db.runNamedQuery(projectName, 'Scheduling/getNext15Orders', params)
	
	scheduleHeaders = [ 'SEQNR', 'ORDERNUMBER', 'MATERIAL', 'ENGMODEL', 'REMQTY', 'DELIVEREDQTY', 'DATE OFFSET']
	scheduleValues = []
	orderSeq = 0
	for row in range(orders.getRowCount()):
		if orderSeq < 20: # only 20 slots in PLC tag, query returns 25 for potential straggler adjustment

			remQty = orders.getValueAt(row, 'REMQTY')
			delQty = orders.getValueAt(row, 'DELIVEREDQTY')
			orderNumber = orders.getValueAt(row, 'ORDERNUMBER')
			seqNr = orders.getValueAt(row, 'SEQNR')
			material = orders.getValueAt(row, 'MATERIAL')
			alternative = orders.getValueAt(row, 'ALTBOM')
	
			reqDate = orders.getValueAt(row, 'REQDATE')
			reqDate = system.date.parse(reqDate, 'yyyy-MM-dd HH:mm:ss')
			dateOffSet = system.date.daysBetween(date, reqDate)
			
			engModel = getEngModel(prodLine, material, alternative)
			scheduleValues.append([seqNr, orderNumber, material, engModel, remQty, delQty, dateOffSet])
		
			orderSeq += 1
			
	if orders.getRowCount > 0:	
		scheduleDS = system.dataset.toDataSet(scheduleHeaders, scheduleValues)
		system.tag.writeBlocking(tagPath, scheduleDS)
		
def updateRunCounter(linePath, modelInfo, index):
	basePath = linePath + '/Run Counter'

	indexPath = '%s/PLC Index' %basePath
	matOutPath = '%s/Material Out' %basePath
	activePath = '%s/Run Active' %basePath
	runStartPath = '%s/Run Start Count' %basePath
	runCountPath = '%s/Run Scheduled Count' %basePath
	matNamePath = '%s/Material Name' %basePath
	orderPath = '%s/Order Number' %basePath
	seqNrPath = '%s/Sequence Number' %basePath
	startTimePath = '%s/Run Start Time' %basePath
	runActualPath = '%s/Run Actual Count' %basePath
	
	if index == 0:
		# PLC MES counter doesn't reset to 0 until a few minutes into the shift and this function is called exactly at production start time,
		# use 0 for the first sequence of the day
		curMatOut = 0
	else:
		curMatOut = system.tag.readBlocking(matOutPath)[0].value
	runCount = modelInfo['count']
	matName = modelInfo['material']
	orderNumber = modelInfo['orderNumber']
	seqNr = modelInfo['seqNr']
	startTime = system.date.now()
	
	pathsToWrite = [activePath, indexPath, runCountPath, runStartPath,  matNamePath, orderPath,   seqNrPath, startTimePath, runActualPath]
	valsToWrite = [ 1,          index,     runCount,     curMatOut,     matName,     orderNumber, seqNr,     startTime,     0]
	system.tag.writeAll(pathsToWrite, valsToWrite)
	
def clearRunCounter(linePath):
	basePath = linePath + '/Run Counter'
	indexPath = '%s/PLC Index' %basePath
	matOutPath = '%s/Material Out' %basePath
	activePath = '%s/Run Active' %basePath
	runStartPath = '%s/Run Start Count' %basePath
	runCountPath = '%s/Run Scheduled Count' %basePath
	matNamePath = '%s/Material Name' %basePath
	orderPath = '%s/Order Number' %basePath
	seqNrPath = '%s/Sequence Number' %basePath
	startTimePath = '%s/Run Start Time' %basePath	
	runActualPath = '%s/Run Actual Count' %basePath
	
	pathsToWrite = [activePath, indexPath, runStartPath, runCountPath, matNamePath, orderPath, seqNrPath, startTimePath, runActualPath]
	valsToWrite =  [0,          0,         0,            0,            '',          '',        0,         None,          0]
	system.tag.writeAll(pathsToWrite, valsToWrite)
	
def createD0(line):
	linePath = shared.Lodestar.R3.Config.getLinePath(line)
	
	d0Path = '%s/SAP/D0/' %linePath
	d1Path = '%s/SAP/D1/' %linePath
	
	tagPaths = ['BreakEvents', 'Orders', 'ProductionEnd', 'ProductionStart', 'ReqDate', 'ScheduleEvents', 'Total Diff', 'Total SAP', 'Total Scheduled']
	
	pathsToRead = [d1Path + path for path in tagPaths]
	pathsToWrite = [d0Path + path for path in tagPaths]
	
	vals = system.tag.readAll(pathsToRead)
	valsToWrite = [val.value for val in vals]
	
	system.tag.writeAll(pathsToWrite, valsToWrite)
	
def findStartSeqs():
	lines = shared.Lodestar.R3.Config.activeFGLines
	for line in lines:
		findStartSeq(line)
		
def findStartSeq(line):
	# find the first non-straggler sequence for the line
	print line
	linePath = shared.Lodestar.R3.Config.getLinePath(line)
	# TODO
	#     CHANGE THIS TO D2 FOR DEPLOYMENT
	datePath = '%s/SAP/D2/ReqDate' %linePath
	reqDate = system.tag.readBlocking(datePath)[0].value	
	stragglerPath = '%s/PLC SCHED/Stragglers' %linePath
	stragglers = system.tag.readBlocking(stragglerPath)[0].value
	
	orders = shared.Lodestar.R3.Scheduling.getReqDateOrders(line, reqDate)
	
	for row in range(orders.rowCount):
		orderNumber = orders.getValueAt(row, 'ORDERNUMBER')
		isStraggler = shared.Lodestar.R3.Util.isStraggler(stragglers, orders, row)
		if not isStraggler:
			startSeq = orders.getValueAt(row, 'SEQNR')
			seqPath = '%s/PLC SCHED/Starting Sequence' %linePath
			system.tag.writeBlocking(seqPath, int(startSeq))
			print startSeq
			break
			
def shiftDates():
	# copy D1 to D0
	# change reqDate from D1 to D2
	# populate schedule tags
	today = system.date.now()
	todReq = system.date.midnight(today)
	lines = shared.Lodestar.R3.Config.activeFGLines
	for line in lines:
		print line
		linePath = shared.Lodestar.R3.Config.getLinePath(line)
		
		d1Path = '%s/SAP/D1/ReqDate' %linePath		
		d1 = system.tag.readBlocking(d1Path)[0].value
		
		shouldShift = system.date.secondsBetween(todReq, d1) == 0
		if shouldShift:
			# only shift after a production day
			shared.Lodestar.R3.Tag.createD0(line)

			datePath = '%s/SAP/D2/ReqDate' %linePath
			startDate = system.tag.readBlocking(datePath)[0].value
			
			shared.Lodestar.R3.Tag.populateAllDates(line, linePath, startDate)
			
			shared.Lodestar.R3.Tag.populateAllScheduleTags(startDate)