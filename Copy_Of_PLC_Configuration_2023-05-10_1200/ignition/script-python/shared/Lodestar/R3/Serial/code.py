from shared.Lodestar.R3.Config import projectName
from shared.Lodestar.R3.Util import clientScope
from shared.Lodestar.R3.OneByOne import get1x1IDs

def prePopulateSerialNumbers(prodLine):
	print 'prePopulate'
	linePath = shared.Lodestar.R3.Config.getLinePath(prodLine)
	d2 = system.tag.readBlocking(linePath+'/SAP/D2/ReqDate')[0].value
	d3 = system.tag.readBlocking(linePath+'/SAP/D3/ReqDate')[0].value
	
	d2Orders = shared.Lodestar.R3.Scheduling.getReqDateOrdersByMaterial(prodLine, d2)
	d3Orders = shared.Lodestar.R3.Scheduling.getReqDateOrdersByMaterial(prodLine, d3)

	orders = shared.Lodestar.R3.Scheduling.joinOrdersByMaterial(d2Orders, d3Orders)
#	stragglers = shared.Lodestar.R3.Tag.getStragglersList(linePath)

	for material in orders:
		print material
		sapSource = shared.Lodestar.R3.Config.getActiveSAPSource()
		lastSerial = getLastIgnitionSerial(prodLine, material, d2, sapSource) # last serial populated in 1x1
		print ' last serial', lastSerial
		if lastSerial is None:
			lastSerial = getLastSerial(prodLine, material) # last printed serial for model/line from zspace
		if lastSerial is None:
			serial = generateSerial(prodLine, d2)
		else:
			serial = incrementSerial(lastSerial)
			if not isSerialValid(serial, d2):
				serial = generateSerial(prodLine, d2)
		print ' serial', serial
		matOrders = orders[material]		
		for order in matOrders:
			print '  ', order
			orderQuantity = shared.Lodestar.R3.Scheduling.getScheduleQuantity(order)
			straggler = shared.Lodestar.R3.Util.isStragglerByModel(linePath, material, orderQuantity)
			if not straggler:
				serialCount = shared.Lodestar.R3.OneByOne.get1x1SerialCount(order)
				if serialCount <= 0:
					reqDate = shared.Lodestar.R3.Scheduling.getOrderReqDate(order)
					if not isSerialValid(serial, reqDate):
						serial = generateSerial(prodLine, reqDate)
					serial = populateSerialNumbers(prodLine, order, serial)
				else: # already populated
					# set the serial for continuation from the end of the 1x1
					lastSerial = shared.Lodestar.R3.OneByOne.getLast1x1Serial(order)
					if lastSerial is not None:
						serial = incrementSerial(lastSerial)
					else:
						reqDate = shared.Lodestar.R3.Scheduling.getOrderReqDate(order)
						serial = generateSerial(prodLine, reqDate)
					
def populateSerialNumbers(prodLine, orderNumber, startSerial):
	print 'populate'
	print ' startSerial', startSerial
	ids = shared.Lodestar.R3.OneByOne.get1x1IDs(orderNumber)
	
	serial = startSerial
	for row in range(ids.rowCount):
		unitID = ids.getValueAt(row, 'id')
		shared.Lodestar.R3.OneByOne.set1x1Serial(unitID, serial)
		serial = incrementSerial(serial)		
	return serial

def rePopulateModelSerials(prodLine, model, startSerial, alteredSeqNr):
	linePath = shared.Lodestar.R3.Config.getLinePath(prodLine)
	print 'rePopulate', model, startSerial
	alteredSeqNr = int(alteredSeqNr) # the seqNr where 1x1 changed
#	orders = shared.Lodestar.R3.Tag.getModelInfoByMaterial(prodLine)
	
	d1 = system.tag.readBlocking(linePath+'/SAP/D1/ReqDate')[0].value
	d2 = system.tag.readBlocking(linePath+'/SAP/D2/ReqDate')[0].value
	
	d1Orders = shared.Lodestar.R3.Scheduling.getReqDateOrdersByMaterial(prodLine, d1)
	d2Orders = shared.Lodestar.R3.Scheduling.getReqDateOrdersByMaterial(prodLine, d2)

	orders = shared.Lodestar.R3.Scheduling.joinOrdersByMaterial(d1Orders, d2Orders)
	
	#stragglers = shared.Lodestar.R3.Tag.getStragglersList(linePath)

	serial = startSerial
	if serial is not None:
		for material in orders:
			if material == model:
				matOrders = orders[material]
				for order in matOrders:
					orderQuantity = shared.Lodestar.R3.Scheduling.getScheduleQuantity(order)
					straggler = shared.Lodestar.R3.Util.isStragglerByModel(linePath, material, orderQuantity)

					if not straggler:
						seqNr = shared.Lodestar.R3.Scheduling.getOrderSeqNr(order)
						seqNr = int(seqNr)
						if seqNr > alteredSeqNr: # only adjust subsequent orders
							reqDate = shared.Lodestar.R3.Scheduling.getOrderReqDate(order)
							if not isSerialValid(serial, reqDate):
								serial = generateSerial(prodLine, reqDate)	
							serial = populateSerialNumbers(prodLine, order, serial)

def rePopulateReqDateModelSerials(prodLine, model, startSerial, alteredSeqNr, reqDate):
	print 'rePopulate', model, startSerial
	linePath = shared.Lodestar.R3.Config.getLinePath(prodLine)

	alteredSeqNr = int(alteredSeqNr) # the seqNr where 1x1 changed
	orders = shared.Lodestar.R3.Scheduling.getReqDateOrdersByMaterial(prodLine, reqDate)
	
	#stragglers = shared.Lodestar.R3.Tag.getStragglersList(linePath)

	serial = startSerial
	if serial is not None:
		for material in orders:
			if material == model:
				matOrders = orders[material]
				for order in matOrders:
					orderQuantity = shared.Lodestar.R3.Scheduling.getScheduleQuantity(order)
					straggler = shared.Lodestar.R3.Util.isStragglerByModel(linePath, material, orderQuantity)
					if not straggler:
						seqNr = shared.Lodestar.R3.Scheduling.getOrderSeqNr(order)
						seqNr = int(seqNr)
						if seqNr > alteredSeqNr: # only adjust subsequent orders			
							serial = populateSerialNumbers(prodLine, order, serial)
																	
def continueCarryOverSerialNumbers(prodLine, orderNumber, startSerial, startUnit):
	logger = system.util.getLogger('black out')
	logger.info('continueSerialNumbers ' + orderNumber)
	print 'continueSerialNubmers', orderNumber
	existing1x1 = shared.Lodestar.R3.OneByOne.get1x1(orderNumber)
	print ' startSerial', startSerial
	print ' startUnit', startUnit
	lastSerial = startSerial
	for row in range(existing1x1.rowCount):
		serial = existing1x1.getValueAt(row, 'SerialNumber')
		unit = existing1x1.getValueAt(row, 'Name')
		
		if int(unit) >= startUnit:
			# no scanner time found, serial already exists
			unitId = existing1x1.getValueAt(row, 'id')
			shared.Lodestar.R3.OneByOne.set1x1Serial(unitId, lastSerial)
			lastSerial = incrementSerial(lastSerial)

	return lastSerial
	
def continueOverProdSerialNumbers(prodLine, orderNumber, startSerial, startUnit):
	logger = system.util.getLogger('black out')
	logger.info('continueSerialNumbers ' + orderNumber)
	print 'continueSerialNubmers', orderNumber
	existing1x1 = shared.Lodestar.R3.OneByOne.get1x1(orderNumber)
	print ' startSerial', startSerial
	print ' startUnit', startUnit
	lastSerial = startSerial
	for row in range(existing1x1.rowCount):
		unit = existing1x1.getValueAt(row, 'Name')
		
		if int(unit) >= startUnit:
			# no scanner time found, serial already exists
			unitId = existing1x1.getValueAt(row, 'id')
			shared.Lodestar.R3.OneByOne.set1x1Serial(unitId, lastSerial)
			lastSerial = incrementSerial(lastSerial)

	return lastSerial
	
def continueSerialNumbers(prodLine, orderNumber):
	existing1x1 = shared.Lodestar.R3.OneByOne.get1x1(orderNumber)
	
	lastSerial = None
	for row in range(existing1x1.rowCount):
		serial = existing1x1.getValueAt(row, 'SerialNumber')
		if serial is not None:
			# serial already populated
			lastSerial = serial
		else:
			# generate new serial and update
			unitId = existing1x1.getValueAt(row, 'id')
			lastSerial = incrementSerial(lastSerial)
			shared.Lodestar.R3.OneByOne.set1x1Serial(unitId, lastSerial)
	return lastSerial
			
def incrementSerial(serialNumber):
	plantID, yearID, weekID, uniqueNum = parseSerial(serialNumber)
	uniqueNum += 1
	if uniqueNum > 99999:
		uniqueNum = 0
		weekID = int(weekID) + 1
		weekID = str(weekID).zfill(2)
	uniqueNum = str(uniqueNum).zfill(5)
	return plantID+yearID+weekID+uniqueNum
	
def parseSerial(serialNumber):
	# CX0747686
	plantID = serialNumber[0] # C
	yearID = serialNumber[1] # X
	weekID = serialNumber[2:4] # 07
#	lineID = serialNumber[4] # 4
	uniqueNum = int(serialNumber[4:]) # 7686
#	return plantID, yearID, weekID, lineID, uniqueNum
	return plantID, yearID, weekID, uniqueNum
	
def getLastSerial(prodLine, model):	# from zspace
	if isKenmore(model):
		# first two chars are not stored in zspace for Kenmore
		model = model[2:]
	
	lineID = getLineCode(prodLine) # line identifier for OnlinePrinting system, not Sepasoft
	plantCode = shared.Lodestar.R3.Config.getPlantSerialCode()
	params = {'lineID': lineID, 'model': model, 'plantCode': plantCode}
	queryName = 'Scheduling/getLastSerialForModel'
	if clientScope():
		serial = system.db.runNamedQuery(queryName, params)
	else:
		serial = system.db.runNamedQuery(projectName, queryName, params)
	return serial
	
def getLastIgnitionSerial(prodLine, material, reqDate, sapSource):
	queryName = 'Scheduling/getLastIgnSerial'
	params = {'prodLine': prodLine, 'material': material, 'reqDate': reqDate, 'sapSource': sapSource}
	if clientScope():
		lastSerial = system.db.runNamedQuery(queryName, params)
	else:
		lastSerial = system.db.runNamedQuery(projectName, queryName, params)
	return lastSerial
	
def generateSerial(prodLine, reqDate):
	plantCode = shared.Lodestar.R3.Config.getPlantSerialCode()
	yearCode = getYearCode()
	weekCode = getWeekCode(reqDate)
#	lineCode = getLineCode(prodLine)
	uniqueNum = '00001'
	
	return plantCode+yearCode+weekCode+uniqueNum

def isSerialValid(serialNumber, reqDate):
	if serialNumber is not None:
		plantID, yearID, weekID, uniqueNum = parseSerial(serialNumber)
		actPlantID = shared.Lodestar.R3.Config.getPlantSerialCode()
		actYearID = getYearCode()
		actWeekID = getWeekCode(reqDate)
		
		return  (plantID == actPlantID) and (yearID == actYearID) and (weekID == actWeekID)
	else:
		return False

def isEnteredSerialValid(serialNumber, prodLine, reqDate):
	plantCode = shared.Lodestar.R3.Config.getPlantSerialCode()
	yearCode = getYearCode()
	weekCode = getWeekCode(reqDate)
#	lineCode = getLineCode(prodLine)
	
	message = ''

	if len(serialNumber) != 9:
		message = "Serial must be exactly 9 characters"	
	elif not serialNumber.isalnum():
		message = "Serial must not contain spaces or special characters"
	elif serialNumber[0].upper() != plantCode:
		message = "Serial must start with " + plantCode
	elif serialNumber[1].upper() != yearCode:
		message = "Year code must be " + yearCode
	elif serialNumber[2:4] != weekCode and int(serialNumber[2:4]) != int(weekCode) + 1 and int(serialNumber[2:4]) != int(weekCode) - 1:
		message = "Week code is invalid"
#	elif serialNumber[4] != lineCode:
#		message = "Line code must be " + lineCode
	elif not serialNumber[4:].isdigit():
		message = "Last 5 characters must be numbers"
	return message == "", message
			
def isKenmore(model):
	prefix = model[:2]
	return prefix[0].isdigit() and prefix[1].isdigit()
					
def getWeekCode(reqDate):
	yearStart = getYearStart()
	#today = system.date.now()
	
	weeksBetween = system.date.weeksBetween(yearStart, reqDate)
	weekCode = weeksBetween + 1
	return str(weekCode).zfill(2)
		
def getYearCode():
	year = system.date.getYear(system.date.now())
	queryName = 'Scheduling/getYearCode'
	params = {'year': year}
	if clientScope():
		yearCode = system.db.runNamedQuery(queryName, params)
	else:
		yearCode = system.db.runNamedQuery(projectName, queryName, params)
	return yearCode
	
def getLineCode(prodLine):
	return prodLine[-1]
	
def getYearStart():
	year = system.date.getYear(system.date.now())
	queryName = 'Scheduling/getYearStart'
	params = {'year': year}
	if clientScope():
		yearStart = system.db.runNamedQuery(queryName, params)
	else:
		yearStart = system.db.runNamedQuery(projectName, queryName, params)
	return yearStart