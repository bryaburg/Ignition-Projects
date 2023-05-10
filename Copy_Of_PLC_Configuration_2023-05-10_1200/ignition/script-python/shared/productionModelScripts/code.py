def testScript():
	return 1
	
def getAreaID(UUID):
	#Get Local ID from UUID	
	#print UUID
	query = "SELECT AreaLocalID FROM AreaLocal WHERE AreaID = '" + UUID + "'"
	ID = system.db.runQuery(query, 'MasterData_R2')
	#print "Local Area ID: " + ID
	return ID
	
def getLineID(UUID):	
	#Get Local ID from UUID
	ID = system.db.runQuery("SELECT LineLocalID FROM LineLocal WHERE LineID = " + mesObject.getMESObjectUUID(), 'MasterDate_R2')
	#print UUID + " = " + ID	
	return ID
	
	
def getAreaNameFromID(ID):
	#Get Area name from Local ID
	dataSet = system.db.runQuery("SELECT AreaID FROM AreaLocal WHERE AreaLocalID = '" + str(ID) + "'", 'MasterData_R2')
	UUID = dataSet.getValueAt(0,0)
	if UUID is not None:
		link = system.mes.getMESObjectLink(UUID)
		if link is not None:
			Name = link.getName()
			return Name
		else:
			return None
	else:
		return None
		
def getLineNameFromID(ID):	
	#Get Line name from Local ID
	dataSet = system.db.runQuery("SELECT LineID FROM LineLocal WHERE LineLocalID = '" + str(ID) + "'", 'MasterData_R2')
	if dataSet.rowCount > 0:
		UUID = dataSet.getValueAt(0,0)
		#print UUID
		Name = system.mes.getMESObjectLink(UUID).getName()
		#print Name
		return Name
	else:
		return ""
		
		
def getLineNamesFromIDs(lineIDsString):
	#Get Line names from string of Line Local IDs
	lineNames = []
	lineIDs = str(lineIDsString).split(',')
	for ID in lineIDs:
		#print ID
		data = system.db.runQuery("SELECT LineID FROM LineLocal WHERE LineLocalID = '" + str(ID) + "'", 'MasterData_R2')
		if data.rowCount > 0:
			UUID = data.getValueAt(0,0)
			Name = system.mes.getMESObjectLink(UUID).getName()
			if "{" not in Name:
				lineNames.append([Name])
				
	lineNamesDataSet = system.dataset.toDataSet(['Line Name'], lineNames)
	return lineNamesDataSet

def getAreaIDFromName(Name):
	#Get Area Local ID from Area Name
	UUID = system.mes.getMESObjectLinkByName('Area', Name).getMESObjectUUID()
	dataSet = system.db.runQuery("SELECT AreaLocalID FROM AreaLocal WHERE AreaID = '" + UUID + "'", 'MasterData_R2')
	LocalID = dataSet.getValueAt(0,0)
	#print LocalID
	return LocalID
	
def getLineIDFromName(Name):
	#Get Line Local ID from Area Name
	UUID = system.mes.getMESObjectLinkByName('Line', Name).getMESObjectUUID()
	dataSet = system.db.runQuery("SELECT LineLocalID FROM LineLocal WHERE LineID = '" + UUID + "'", 'MasterData_R2')
	LocalID = dataSet.getValueAt(0,0)
	#print LocalID
	return LocalID
	
def getLinesFromLocalAreaID(ID):
	dataSet = system.db.runQuery("SELECT AreaID FROM AreaLocal WHERE AreaLocalID = '" + str(ID) + "'", 'MasterData_R2')
	UUID = dataSet.getValueAt(0,0)
	area = system.mes.getMESObjectLink(UUID).getMESObject()
	Lines = area.getChildCollection()
	#print Lines
	lineDataSet = system.db.runQuery("SELECT LineLocalID AS LineID, Description AS LineName FROM LineLocal WHERE LineID IN (" + " ".join("'" + str(x) + "'," for x in Lines) + "'')", 'MasterData_R2')
	return lineDataSet
	
def getLineNamesFromLocalAreaID(ID):
	lineNames = []
	dataSet = system.db.runQuery("SELECT AreaID FROM AreaLocal WHERE AreaLocalID = '" + str(ID) + "'", 'MasterData_R2')
	UUID = dataSet.getValueAt(0,0)
	#print UUID
	area = system.mes.getMESObjectLink(UUID).getMESObject()
	Lines = area.getChildCollection()
	#print Lines
	lineList = Lines.getList()
	for line in lineList:
		lineName = line.getName()
		#this check is because Sepasoft returns the entire equipment path instead of just the name as it should
		#this strips the name from the path if needed
		if "Whirlpool MES\\M003 Clyde" in lineName:
			fields = lineName.split('\\')
			lineName = fields[len(fields)-1]
		lineNames.append([lineName])
	#print lineNames
	return system.dataset.toDataSet(["Line Name"], lineNames)
	
def getPartNumberNameFromUUID(UUID):
	#Get part number name from UUID
	partNoLink = system.mes.getMESObjectLink(UUID)
	if partNoLink is not None:
		if partNoLink.hasMESObject():
			#print partNoLink.getMESObject().name
			return partNoLink.getMESObject().name
		else:
			return ""
	else:
		return ""
		
def getPartNumberNameFromLocalPartID(ID):
	#Get part number name from Local Part ID
	dataSet = system.db.runQuery("SELECT PartID FROM PartLocal WHERE PartLocalID = '" + str(ID) + "'", 'MasterData_R2')
	if dataSet is not None:
		UUID = dataSet.getValueAt(0,0)
		partNoLink = system.mes.getMESObjectLink(UUID)
		if partNoLink is not None:
			if partNoLink.hasMESObject():
				return partNoLink.getMESObject().name
			else:
				return ""
		else:
			return ""
				
def getPartNumberUUIDFromLocalPartID(ID):
	#Get part number name from UUID
	dataSet = system.db.runQuery("SELECT PartID FROM PartLocal WHERE PartLocalID = '" + str(ID) + "'", 'MasterData_R2')
	if dataSet is not None:
		UUID = dataSet.getValueAt(0,0)
		return UUID
	else:
		return ""
	
																																
#def checkForUpcomingChangeover(Area, Line, minutes):
#	#returns the name and scheduled start time of a work order
#	#based on Area, Line, and how many hours to check into the future for an upcoming work order
#	#specify the equipment path
#	eqPath = '[global]\\Whirlpool MES\\M003 Clyde\\' + Area + '\\' + Line
#	
#	#define the start and end dates
#	#we want to return changeovers starting X minutes from now only
#	begin = system.date.now()
#	end = system.date.addMinutes(system.date.now(), minutes)
#	
#	#let category be 'Active'
#	category = 'Active'
#	  
#	#Gets the equipment schedule entries
#	try:
#		list = system.mes.getEquipmentScheduleEntries(eqPath, begin, end, category, False)
#	except:
#		return ''
#	
#	#we only need to look at the next workorder, not the entire list
#	if(len(list) > 0):
#		woLink = list[0].getWorkOrderLink()
#		woScheduledStart = list[0].getScheduledStartDate()
#		if (woScheduledStart < end) and (woScheduledStart > begin):
#			return 'Work Order: ' + str(woLink) + ' scheduled to start at: ' + str(woScheduledStart)
#	else:
#		return ''
		
		
def checkForUpcomingChangeover(Area, Line, minutes):
	#returns the name and scheduled start time of a work order
	#based on Area, Line, and how many hours to check into the future for an upcoming work order
	#specify the equipment path
	eqPath = '[global]\\Whirlpool MES\\M003 Clyde\\' + Area + '\\' + Line
	
	#define the start and end dates to search for
	begin = system.date.addDays(system.date.now(), -7)
	end = system.date.addMinutes(system.date.now(), minutes)
	
	#let category be 'Active'
	category = 'Active'
	  
	#Gets the equipment schedule entries
	try:
		scheduleEntries = system.mes.getEquipmentScheduleEntries(eqPath, begin, end, category, False)
		#for item in list:
			#print item
	except:
		return ''
		
	if(len(scheduleEntries) > 0):
		for row in range(len(scheduleEntries)):
			woLink = scheduleEntries[row].getWorkOrderLink()
			woActualEndDate = scheduleEntries[row].getActualEndDate()
			if(woActualEndDate == None):
				woScheduledEnd = scheduleEntries[row].getScheduledEndDate()
				if (woScheduledEnd > begin) and (woScheduledEnd < end):
					try:
						nextWOLink = scheduleEntries[row + 1].getWorkOrderLink()
						nextWOScheduledStart = scheduleEntries[row+1].getScheduledStartDate()
						returnText = 'Part Order: ' + str(woLink) + ' scheduled to end at: ' + str(woScheduledEnd) + '. Part Order: ' + str(nextWOLink) + ' scheduled to start at: ' + str(nextWOScheduledStart) + '.'
					except:
						returnText = 'Part Order: ' + str(woLink) + ' scheduled to end at: ' + str(woScheduledEnd) + '. No Part Order is scheduled to follow.'
					return returnText					
		return ''
	
	else:
		return ''