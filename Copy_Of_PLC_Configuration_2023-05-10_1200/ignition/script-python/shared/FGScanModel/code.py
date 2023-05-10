def modelEngToSales(model):
	readTable = system.tag.readBlocking("[default]FG_Scanning/ConfigScreen/ReadFromTable")[0].value
	query = "Select * from " + readTable + " WHERE engineering_model_number=?"
	DB = system.tag.readBlocking("[default]FG_Scanning/ConfigScreen/DBConnection")[0].value
	search = model.replace(" ","")
	results=system.db.runPrepQuery(query, [search], DB)
	if results.getRowCount() > 0:
		salesModel = results.getValueAt(0,0)
		return salesModel
	else:
		return
		
def sendToSAP(serial, model, timestamp, salesModel, scanner, line):
	writeTable = system.tag.readBlocking("[default]FG_Scanning/ConfigScreen/WriteToTable")[0].value
	scanSourceID = scanner  #How do I get ScanSourceID and LineID to be properly put into FG_Scanning/Config folder??
	lineID = line
	DB = system.tag.readBlocking("[default]FG_Scanning/ConfigScreen/DBConnection")[0].value
	shiftID = "1" #hardcoded because there is no expectation of this changing for this event
	obsoleteFlag = 0  #hardcoded because there is no expectation of this changing for this event
	query = "INSERT INTO " + writeTable + " (Serial_Number, Model_Number, TimeStamp, Scan_Source_ID, Line_ID, Shift_ID, Sales_Number, Obsolete_Flag)" + " VALUES (?,?,?,?,?,?,?,?)"
	args = [serial, model, timestamp, scanSourceID, lineID, shiftID, salesModel, obsoleteFlag]
	results = system.db.runPrepUpdate(query, args, DB)
	if results > 0:
		return results
	else:
		return
		
def networkDeviceHealth():
	lineSettings = system.tag.readBlocking('[default]FG_Scanning/ConfigScreen/LineSettings')[0].value
	pyLineSettings = system.dataset.toPyDataSet(lineSettings)
	colIndex = pyLineSettings.getColumnIndex('Line ID')
	dbName = system.tag.readBlocking('[default]FG_Scanning/ConfigScreen/DBConnection')[0].value
	dbStatus = system.tag.readBlocking('[System]Gateway/Database/'+dbName+'/Available')[0].value
	system.tag.writeBlocking('[default]FG_Scanning/ConfigScreen/DBConnectionStatus', dbStatus)
	import os
	lineIDList = []
	x = 0 #use to walk through the lineIDList
	
	#Create LineIDList
	for row in pyLineSettings:
		lineID = row[colIndex]
		lineIDList.append(lineID)
		

	
	#Get IP for scanner and terminal 
	for line in lineIDList:
		scannerIP = system.tag.readBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/ScannerIP')[0].value
		terminalIP = system.tag.readBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/TerminalIP')[0].value
		pingInterval = system.tag.readBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/PingInterval')[0].value
		notify = system.tag.readBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/Notifications')[0].value
		opcServer = system.tag.readBlocking('[default]FG_Scanning/' + str(lineIDList[x]) +'/Config/OPC Server')[0].value
		opcStatus = system.tag.readBlocking('[System]Gateway/OPC/Connections/'+ opcServer +'/Connected')[0].value
		system.tag.writeBlocking('[default]FG_Scanning/' + str(lineIDList[x]) +'/Config/OPCServerStatus',opcStatus)
		
		#scanner
		scannerStatus = os.system('ping -n 1 -w 1000 ' + scannerIP)
		if scannerStatus == 0:
			scannerTime = system.date.now()
			system.tag.writeBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/LastPingScanner',scannerTime)
			system.tag.writeBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/LastPingScannerStatus', 1)
		elif scannerStatus == 1:
			scannerTime = system.date.now()
			lastScannerTime = system.tag.readBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/LastPingScanner')[0].value
			diff = system.date.minutesBetween(lastScannerTime,scannerTime)
			if diff >= pingInterval:
				#Alarm notification triggers
				if notify:
					system.tag.writeBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/LastPingScannerStatus', 1)
					system.tag.writeBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/LastPingScannerStatus', 0)
		
		#terminal		
		terminalStatus = os.system('ping -n 1 -w 1000 ' + terminalIP)
		if terminalStatus == 0:	
			terminalTime = system.date.now()
			system.tag.writeBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/LastPingTerminal',terminalTime)
			system.tag.writeBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/LastPingTerminalStatus', 1)	
		elif terminalStatus == 1:
			terminalTime = system.date.now()
			lastTerminalTime = system.tag.readBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/LastPingTerminal')[0].value
			minDiff = system.date.minutesBetween(lastTerminalTime,terminalTime)
			if minDiff >= pingInterval:
				#Alarm notification triggers
				if notify:
					system.tag.writeBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/LastPingTerminalStatus', 1)
					system.tag.writeBlocking('[default]FG_Scanning/' + str(lineIDList[x]) + '/Config/LastPingTerminalStatus', 0)
			
		x+=1
	return
