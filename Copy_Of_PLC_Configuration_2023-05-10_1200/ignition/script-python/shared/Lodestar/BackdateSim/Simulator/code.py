from project.Utils import callLater



def removeCounts(eqPath,startTime, endTime,counterName,reportBack = None):
	"""Removes the counts from a counter by setting deltas to 0

	This function removes the counts from the specified sepasoft counter between the 
		two specified date times. 

	Args:
		eqPath (str): The equipment path for the line in sepasoft format 
		startTime (java.util.date): The start time of when to clear the counts
		endTime (java.util.date): The end time of when to clear the counts
		counterName (str): The counter name as it appears on the sepasoft equipment manager
		reportBack (func): callback function that will be passed string updates as they happen

	Returns:
		None
	
	TODO: WRM - This function should use removeTagCollectorValues, but a bug (SD10438) makes it
		impossible.  Once the bug is fixed the function should be retooled to use remove instead 
		of update

	"""

	#Update will automatically recalculate the following values

	#Get the count value before the start time
	justBeforeStart = system.date.addSeconds(startTime,-1)
	previousValue = system.mes.getTagCollectorPreviousValue(eqPath,'Equipment Count',counterName,justBeforeStart)
	if previousValue == None:
		previousValue = 0

	#Get the list of events for each count between start and end
	data = system.mes.getTagCollectorValues(eqPath, 'Equipment Count', counterName, startTime, endTime)
	#Loop through list of count events and set the value to the previous value
	if data != None:
		for row in range(data.rowCount):
			timeStamp = data.getValueAt(row,'TimeStamp')
			system.mes.updateTagCollectorValue(eqPath, 'Equipment Count', counterName, timeStamp, previousValue)
	else:
		report = 'No data found between %s and %s and previous count = %s' % (startTime,endTime,previousValue)
		if reportBack != None:
			reportBack(report)
		

def createCountEvents(eqPath, startTime, endTime, counterName, rate, reportBack = None):
	"""Creats a set of counts for a line based on rate, adding at a resolution of 1 unit

	Creates counts on a sepasoft counter for a sepasoft line between the start and end time at the rate
		specified as seconds/unit

	Args:
		eqPath (str): The equipment path for the line in sepasoft format 
		startTime (java.util.date): The start time of when to clear the counts
		endTime (java.util.date): The end time of when to clear the counts
		counterName (str): The counter name as it appears on the sepasoft equipment manager
		rate (float): The rate in seconds/unit
		reportBack (func): callback function that will be passed string updates

	Returns:
		None

	"""

	
	# Counts need to be purely additive for the OEE module to function correctly
	
	#Build the data
	currentTime = system.date.addSeconds(startTime, rate)
	
	count = system.mes.getTagCollectorPreviousValue(eqPath,'Equipment Count',counterName,currentTime)
	#print count
	if count == None:
		count = 0
	#loop through the time period and enter values
	while currentTime <= endTime:
		count += 1
		try:
			system.mes.addTagCollectorValue(eqPath,'Equipment Count', counterName, currentTime, count,True)
			if reportBack != None:
				reportBack( 'Add %s %s %s' % (currentTime, count, counterName))
		except:
			system.mes.updateTagCollectorValue(eqPath,'Equipment Count', counterName, currentTime, count)
			if reportBack != None:
				reportBack( 'Update %s %s %s' % (currentTime, count, counterName))
				
		currentTime = system.date.addSeconds(currentTime, rate)
	
	#The intersticial previous values don't effect count but the final one may cause issues for the next sim
	#  Shift.  Place the current count at the end of the shift to make it correct to pick up previous value
	#  at the start of the next shift
#	try:
#		system.mes.addTagCollectorValue(eqPath,'Equipment Count', counterName, endTime, count,True)
#	except:
#		system.mes.updateTagCollectorValue(eqPath,'Equipment Count', counterName, endTime, count)
	if reportBack != None:
		reportBack('Finished adding %s' % counterName)

def addEvent(eqPath,state,endState,start,duration,removeCount, countEqPath = ''):
	"""Add an event to the historical record at a workstation

	This function adds a state event to a workstation at the start date for a duration measured in
		seconds.  The function has the option to remove counts for the same period of time.

	Args:
		eqPath (str): The equipment path for the workstation in sepasoft format 
		state (int): The integer representation of the state as it appears in the Sepasoft State Class
			assigned to the workstation
		endState (int): The integer representation of the state for the equipment to go back to at the
			end of the event.
		start (java.util.date): The start of the event
		duration (int): The time in seconds that the event will last
		removeCount (bool): Command to remove the count for the same time period
		countEqPath (str): The equipment path for the line or equipment in sepasoft format for the
			counters to be removed

	Returns:
		None

	Note: The counters specified are for R2 lines.
	"""


	end = system.date.addSeconds(start,duration)

	#Set the start
	try:
		system.mes.addTagCollectorValue(eqPath,'Equipment State','',start,state)
	except:
		system.mes.updateTagCollectorValue(eqPath,'Equipment State','',start,state)
	
	#Set the end
	try:
		system.mes.addTagCollectorValue(eqPath,'Equipment State','',end,endState)
	except:
		system.mes.updateTagCollectorValue(eqPath,'Equipment State','',end,endState)
		#pass #Don't overwrite a good bit of data with just running but update is needed to 
		#account for not being able to remove.
		#TODO: WRM Remove and replace with pass once sepasoft May2021 release is out.

	# Remove the counts 
	if removeCount and countEqPath != '':
		#counts are on the line level and events are on the leaf.  Need to find the line count
		removeCounts(countEqPath,start, end,'Material In')
		removeCounts(countEqPath,start, end,'Material Out')


### Here for the process

## Need more abstraction  Put non screen specific stuff into a code block

#- define Entry Point from Screen to Pure background code

# Grab the name of the db for the release
def getDbName(isR1):
	"""Get the ignition db connection name for the extension.

	Return the propper db conection name for the extension database for the release

	Args:
		isR1 (bool): True returns R1 database name, False returns R2

	Returns:
		str: The ignition db connection name

	"""
	# find the db name
	if isR1:
		db = system.tag.readBlocking("[default]Site/Configuration/IgnitionMES_Extension")[0].value
	else:
		db = system.tag.readBlocking("[default]Site/Configuration/IgnitionMES_Extension_R2")[0].value

	#return db name
	return db

# Grab all of the stuff in a line
def getEqInLine(isR1,lineEQPath):
	"""Function gets a python dataset listing of the equipment assigned to the line

	This function returns a listing of all equipment assigned to the line from the cache table 
		located in the extension database for the release

	Args:
		isR1 (bool): True means the line being passed is an R1 line, False means it is R2
		lineEQPath (str): The equipment path for the line or equipment in sepasoft format

	Returns:
		com.inductiveautomation.ignition.common.script.builtin.DatasetUtilities$PyDataSet: The listing of all
			columns for the equipment assigned to the line as they appear in the model cache database.
	
	TODO: WRM - Need to get the headers for the dataset for the documentation.
	"""
	lineName = lineEQPath.split('\\' )[-1]
	db = getDbName(isR1)
	if isR1:
		query = "SELECT * FROM ModelReplication where LINE = '%s' and DELETED = 0" % lineName
	else:
		query = "SELECT * FROM ModelReplication where LINE = '%s' AND DELETED = 0 order by SORT_ORDER" % lineName


	eqListPDS = system.db.runQuery(query,db) 

	return eqListPDS


def getDefaults(isR1):
	"""Get the default values used for building a shifts worth of data

	This function returns default values to be used for states modes and tag collector values for the requested
		release

	Args:
		isR1 (bool): True means the line being passed is an R1 line, False means it is R2

	Returns:
		OPERATIONUUIDNONE (str): The default value for operation uuid when an operation is not running
		OPERATIONUUID (str): The default value for the operation uuid when an operation is running
		PRODUCTCODE (str): The default value for the product code for a run
		WORKORDER (str): The default value for the work order for a run
		MODERUNNING (int): The default mode value for normal running operation
		MODECHANGEOVER (int): The default mode value for changeover operation
		MODEIDLE (int): The default mode value for idle
		STATERUNNING (int): The default state value for running
		STATEDOWN (int): The default state value for downtime
	
	"""
	if isR1:
		OPERATIONUUIDNONE = ''
		OPERATIONUUID = 'OperationR1Simulated'
		PRODUCTCODE = 'Washer'
		WORKORDER = ''
		MODERUNNING = 1
		MODECHANGEOVER = 2
		MODEIDLE = 3
		STATERUNNING = 0
		STATEDOWN = 2
	else:
		OPERATIONUUIDNONE = ''
		OPERATIONUUID = 'OperationR2Simulated'
		PRODUCTCODE = ''
		WORKORDER = 'SimulatedR2Workorder'
		MODERUNNING = 1
		MODECHANGEOVER = 2
		MODEIDLE = 0
		STATERUNNING = 1
		STATEDOWN = 2

	return (OPERATIONUUIDNONE,OPERATIONUUID,PRODUCTCODE,WORKORDER,MODERUNNING,MODECHANGEOVER,MODEIDLE,STATERUNNING,STATEDOWN)



def cleanShift(lineEQPath,startDT,endDT,lineRate,isR1,reportBack = None,idleMinutes=5,changeOverSeconds=1):
	"""Remove all shift related values for the time of the shift for the line

	The function removes existing state, mode, and tag collector values and places in a clean run for a shift 
		defined by the inputs.  The values are cleared out idleMinutes before the startDT to the endDT.
		Idle values are placed from idleMinutes before startDT until startDT.  Running values are place in from
		startDT until endDT except for line mode which includes a changeover mode for changeOverSeconds starting
		at startDT

	Args:
		lineEQPath (str): The equipment path for the line in sepasoft format.
		startDT (java.util.date): The start time of when to clear the counts.
		endDT (java.util.date): The end time of when to clear the counts.
		lineRate (float): The rate in seconds/unit.
		isR1 (bool): Boolean for if the line is an R1 line.
		reportBack (boolean): callback function that will be passed string updates from the sim
		idleMinutes (int): The amount of idle buffer time before the startDT to clear out and set to idle
		changOverSeconds (int): The amount of seconds the line will be in changeover for the clean shift

	Returns:
		None

	"""

	(OPERATIONUUIDNONE,OPERATIONUUID,PRODUCTCODE,WORKORDER,MODERUNNING,MODECHANGEOVER,MODEIDLE,STATERUNNING,STATEDOWN) = getDefaults(isR1)

	eqListPDS = getEqInLine(isR1,lineEQPath)
	#Move through the time period in cronological order just to help keep things straight.
	#Clear the existing information in the model for the time period
	zeroDate = system.date.addMinutes(startDT,-1*idleMinutes)
	changOverEnd = system.date.addSeconds(startDT,changeOverSeconds)
	# loop through each piece of equipment
	fixList = ["Equipment Operation UUID", "Equipment Product Code", "Equipment Work Order", "Equipment Mode","Equipment State"]
	numEq = len(eqListPDS)
	count = 0

	for eq in eqListPDS:
		#break #placed for testing second half of code
		count += 1
		eqPath = eq['PATH']
		eqLevel = eq['OBJ_LEVEL']
		if isR1:
			if eqLevel == 4:
				isLeaf = True
			else:
				isLeaf = False
		else:
			if eqLevel == 2:
				isLeaf = True
			else:
				isLeaf = False
		#remove the cal and repair workstations as they are not sepasoft
		if 'Whirlpool MES' in eqPath:
			#try:
				
			#clear out all of the existing data
			if reportBack != None:
				report = eqPath + '   %s / %s' % (count,numEq)
				reportBack(report)	
				report = 'Clearing out data'
				reportBack(report)
			for x in fixList:
				system.mes.removeTagCollectorValues(eqPath,x,'',zeroDate,endDT)

			#Clear out counts seperately since it requires a different format
			if eqLevel == 1:
				removeCounts(eqPath,zeroDate, endDT,'Material Out')
				removeCounts(eqPath,zeroDate, endDT,'Material In')
				#system.mes.removeTagCollectorValues(eqPath,'Equipment Count', 'Material Out',zeroDate,end)
				#system.mes.removeTagCollectorValues(eqPath,'Equipment Count', 'Material In',zeroDate,end)
				
				
			#set values for clean run by looping through the fix list and setting each for the equipment
			if reportBack != None:
				report = 'Default Values Add Start'
				reportBack(report)
			for x in fixList:
				if x == "Equipment Mode":
					#Put in modes for clean run Idle-changeover-running-idle to simulate modes for shift
					system.mes.addTagCollectorValue(eqPath,x,'',zeroDate,MODEIDLE)
					system.mes.addTagCollectorValue(eqPath,x,'',startDT,MODECHANGEOVER)
					system.mes.addTagCollectorValue(eqPath,x,'',changOverEnd,MODERUNNING)
					system.mes.addTagCollectorValue(eqPath,x,'',endDT,MODEIDLE)
				elif x == 'Equipment State':
					#Set the state if its a leaf to be running the entire time
					#print eqPath,eqLevel
					if isLeaf:
						system.mes.addTagCollectorValue(eqPath,x,'',zeroDate,STATERUNNING)
						
				else:
					#set the rest of the shift values
					#Set the value at the begining of the  zero date to blank
					system.mes.addTagCollectorValue(eqPath,x,'',zeroDate,'')

					#Set the start of shift values
					if x == "Equipment Operation UUID":
						system.mes.addTagCollectorValue(eqPath,x,'',startDT,OPERATIONUUID)
						system.mes.addTagCollectorValue(eqPath,x,'',endDT,'')
					if x == "Equipment Product Code":
						system.mes.addTagCollectorValue(eqPath,x,'',startDT,PRODUCTCODE)
						system.mes.addTagCollectorValue(eqPath,x,'',endDT,'')
					if x == "Equipment Work Order":
						system.mes.addTagCollectorValue(eqPath,x,'',startDT,WORKORDER)
						system.mes.addTagCollectorValue(eqPath,x,'',endDT,'')
			
			#Counts need to be added on the line
			if eqLevel == 1 :
				# R2 counts added directly to Sepasoft
				if reportBack != None:
					report = 'Starting Default Count Addition'
					reportBack(report)
				if not isR1:
					createCountEvents(eqPath, startDT, endDT,'Material In', lineRate)
					createCountEvents(eqPath, startDT, endDT,'Material Out', lineRate)
				if reportBack != None:
					report = 'Added Default Count Values'
					reportBack(report)

				shiftSeconds = system.date.secondsBetween(startDT,endDT)
				r1count = (shiftSeconds * 1.0) / lineRate
			
			if reportBack != None:
				report = 'Finished Default Shift Values'
				reportBack(report)

			
		if reportBack != None:
			report = 'Finished Ideal Shift'
			reportBack(report)	
	
	#At this point the line should have an ideal run over the time period
	
	#End of Code, report to user.
	if reportBack != None:
		report = 'Finished  Clean Run For all EQ in line'
		reportBack(report)	
	
	# get the ideal count for a shift
	shiftSeconds = system.date.secondsBetween(startDT,endDT)
	idealCount = shiftSeconds/lineRate
	return idealCount
#end of def cleanShift

def getEventsFromFile(fileLoc):
	"""Reads events from a formatted CSV

	It reads events from a formatted CSV file and returns them in a two dimensional array. Each row of the array is a
		separate event.

	Args:
		fileLoc (str): File path to the formatted CSV file

	Returns:
		array[][]:(ID,State,Start,Duration,MLC,Repeat Timer) for R1 (ID,State,Start,Duration,Position,Repeat Timer) for R2


	"""
	import csv

	#grab the file
	csvfile = open(fileLoc)
	events = []
	try:
		#The first 3 lines are headers
		csvread = csv.reader(csvfile)
		head1 = csvread.next()
		head2 = csvread.next()
		head3 = csvread.next()
		
		#append the data
		for line in csvread:
			events.append(line)
	finally:
		#Be good and make sure the file gets closed
		csvfile.close()
	
	return events


def getR1EquipmentList(lineEQPath,db):
	"""Gets lists for both MLC and non MLC Equipment assigned to the line

	This function pulls both the MLC and non MLC equipment lists from the model cache table in the Extension database

	Args:
		lineEQPath (str): The equipment path for the line or equipment in sepasoft format
		db (str): The ignition connection name for the db

	Returns:
		Python Dataset,Python Dataset: Main Line Eq list, Non Main Line Eq List

	"""
	
	lineName = lineEQPath.split('\\' )[-1]
	
	#get key cell
	query = "SELECT * FROM ModelReplication where LINE = '%s' and DELETED = 0 and OBJ_LEVEL = 4 and PATH like '%%Main Line%%'" % lineName
	eqMainLinePDS = system.db.runQuery(query,db) 
	
	query = "SELECT * FROM ModelReplication where LINE = '%s' and DELETED = 0 and OBJ_LEVEL = 4 and PATH not like '%%Main Line%%'" % lineName
	eqNonMLSPDS = system.db.runQuery(query,db)

	return (eqMainLinePDS,eqNonMLSPDS)


def placeR1Events(events,runningState,shiftStartDT,shiftEndDT,lineEQPath,db,reportBack = None):
	"""Places the events into the line between start and end

	This function takes the each event defined by a row in events and adds it to the line. The events
		are all in seconds offset from the shift start.

	Args:
		events (array[][]): 2D Array of events to be added to the line
		startDT (java.util.date): The start time of when to clear the counts.
		endDT (java.util.date): The end time of when to clear the counts.
		lineEQPath (str): The equipment path for the line or equipment in sepasoft format
		db (str): The ignition connection name for the db that has model cache table
		reportBack (func): callback function that will be passed string updates

	Returns:
		long : Downtime in seconds that was placed into the shift

	Note: All events are added at their offset, there is no check for offsets that would be added to the
		line after the shift end date.
	"""
	shiftSeconds = system.date.secondsBetween(shiftStartDT,shiftEndDT)
	
	#Get the R1 Equipment lists
	(eqMainLinePDS,eqNonMLSPDS) = getR1EquipmentList(lineEQPath,db)
	r1Downtime = 0.0

	if reportBack != None:
		report = '%s MLS Workstation and %s NON MLS Workstations Detected' % (len(eqMainLinePDS),len(eqNonMLSPDS))
		reportBack(report)
	
	for evt in events:
		id = int(evt[0])
		state = int(evt[1])
		startDelay = int(evt[2])
		duration = int(evt[3])
		mlc = evt[4] == '1'
		repeat = int(evt[5])
		
		if mlc:
			eqPath = eqMainLinePDS[id]['PATH']
			r1Downtime = r1Downtime + duration
		else:
			eqPath = eqNonMLSPDS[id]['PATH']
			
		eventStart = system.date.addSeconds(shiftStartDT,startDelay)
		#print eqPath,state,eventStart,duration,mlc
		if reportBack != None:
			report = '%s added state %s from %s for %s seconds' % (eqPath,state,eventStart,duration)
			reportBack(report)
		
		addEvent(eqPath,state,runningState,eventStart,duration,False)
		
		
		#Handle repeate
		#TODO
		if repeat >0:
			secondsLeftInShift = shiftSeconds - startDelay - duration - repeat
			eventTime = duration +repeat
			
			nextStartTime = system.date.addSeconds(shiftStartDT,startDelay + eventTime)   #start time + startDelay+duration+repeat
			
			while nextStartTime < shiftEndDT:
				secondsLeftInShift = system.date.secondsBetween(nextStartTime,shiftEndDT)
				if secondsLeftInShift < duration:
					#If the event is long then whats left pass the time left to ensure it doesn't go further than the shift
					if reportBack != None:
						report = 'Adding repeat event at %s for %s seconds' % (nextStartTime,secondsLeftInShift)
						reportBack(report)
					addEvent(eqPath,state,runningState,nextStartTime,secondsLeftInShift,False)
					if mlc:
						r1Downtime = r1Downtime + secondsLeftInShift
				else:
					#pass the new event to be written
					if reportBack != None:
						report = 'Adding repeat event at %s for %s seconds' % (nextStartTime,duration)
						reportBack(report)
					addEvent(eqPath,state,runningState,nextStartTime,duration,False)
					if mlc:
						r1Downtime = r1Downtime + duration
				nextStartTime = system.date.addSeconds(nextStartTime, duration+repeat)
	return r1Downtime


def getR2EquipmentList(lineEQPath,db):
	"""Gets lists for workstations at before, at, and after the key cell

	Gets lists for workstations at before, at, and after the key cell

	Args:
		lineEQPath (str): The equipment path for the line or equipment in sepasoft format
		db (str): The ignition connection name for the db

	Returns:
		Python Dataset,Python Dataset,Python Dataset: Key Cell Eq list, Pre Key Cell Eq List, Post Key Cell Eq List

	"""

	#R2 events
	db = system.tag.readBlocking("[default]Site/Configuration/IgnitionMES_Extension_R2")[0].value
	#get key cell
	query = "SELECT * FROM ModelReplication where PARENT_PATH = '%s' AND DELETED = 0 AND SORT_ORDER = 0" % lineEQPath
	eqKeyPDS = system.db.runQuery(query,db) 
	
	#get starved cell
	query = "SELECT * FROM ModelReplication where PARENT_PATH = '%s' AND DELETED = 0 AND SORT_ORDER = -1" % lineEQPath
	eqPrePDS = system.db.runQuery(query,db) 
	
	#get blocked cell
	query = "SELECT * FROM ModelReplication where PARENT_PATH = '%s' AND DELETED = 0 AND SORT_ORDER = 1" % lineEQPath
	eqPostPDS = system.db.runQuery(query,db) 

	return (eqKeyPDS,eqPrePDS,eqPostPDS)


def placeR2Events(events,runningState,shiftStartDT,shiftEndDT,lineEQPath,db,reportBack = None):
	"""Places the events into the line between start and end

	This function takes the each event defined by a row in events and adds it to the line. The events
		are all in seconds offset from the shift start.

	Args:
		events (array[][]): 2D Array of events to be added to the line
		startDT (java.util.date): The start time of when to clear the counts.
		endDT (java.util.date): The end time of when to clear the counts.
		lineEQPath (str): The equipment path for the line or equipment in sepasoft format
		db (str): The ignition connection name for the db that has model cache table
		reportBack (func): callback function that will be passed string updates

	Returns:
		None

	Note: All events are added at their offset, there is no check for offsets that would be added to the
		line after the shift end date.
	"""

	(eqKeyPDS,eqPrePDS,eqPostPDS) = getR2EquipmentList(lineEQPath,db)

	shiftSeconds = system.date.secondsBetween(shiftStartDT,shiftEndDT)
	
	for evt in events:
		id = int(evt[0])
		state = int(evt[1])
		startDelay = int(evt[2])
		duration = int(evt[3])
		pos = int(evt[4]) 
		repeat = int(evt[5])
		
		totalDowntime = 0.0

		#determine the eqpath for the event and add duration if key cell
		if pos ==0:
			eqPath = eqKeyPDS[id]['PATH']
			
		elif pos == -1:
			eqPath = eqPrePDS[id]['PATH']
		elif pos == 1:
			eqPath = eqPostPDS[id]['PATH']
		else:
			#Not implemented yet
			if reportBack != None:
				report = 'R2 Pos not usable'
				reportBack(report)
			continue
		
		eventStart = system.date.addSeconds(shiftStartDT,startDelay)
		#print eqPath,state,eventStart,duration,mlc
		if reportBack != None:
			report = '%s added state %s from %s for %s seconds' % (eqPath,state,eventStart,duration)
			reportBack(report)
		
		addEvent(eqPath,state,runningState,eventStart,duration,True,lineEQPath)
		
		#Handle repeate
		#TODO
		if repeat >0:
			secondsLeftInShift = shiftSeconds - startDelay - duration - repeat
			eventTime = duration +repeat
			
			nextStartTime = system.date.addSeconds(shiftStartDT,startDelay + eventTime)   #start time + startDelay+duration+repeat
			
			while nextStartTime < shiftEndDT:
				secondsLeftInShift = system.date.secondsBetween(nextStartTime,shiftEndDT)
				if secondsLeftInShift < duration:
					#If the event is long then whats left pass the time left to ensure it doesn't go further than the shift
					if reportBack != None:
						report = 'Adding repeat event at %s for %s seconds' % (nextStartTime,secondsLeftInShift)
						reportBack(report)
					addEvent(eqPath,state,runningState,nextStartTime,secondsLeftInShift,True,lineEQPath)
					
				else:
					#pass the new event to be written
					if reportBack != None:
						report = 'Adding repeat event at %s for %s seconds' % (nextStartTime,duration)
						reportBack(report)
					addEvent(eqPath,state,runningState,nextStartTime,duration,True,lineEQPath)
					
				nextStartTime = system.date.addSeconds(nextStartTime, duration+repeat)
	if reportBack != None:
		report = 'R2 downtime added'
		reportBack(report)


def placeEvents(events,shiftStartDT,shiftEndDT,lineEQPath,isR1,reportBack = None):
	"""Places the events into the line between start and end

	This function takes the each event defined by a row in events and adds it to the line. The events
		are all in seconds offset from the shift start.

	Args:
		events (array[][]): 2D Array of events to be added to the line
		startDT (java.util.date): The start time of when to clear the counts.
		endDT (java.util.date): The end time of when to clear the counts.
		lineEQPath (str): The equipment path for the line or equipment in sepasoft format
		isR1 (bool): Is the line specified an R1 line or R2 line
		reportBack (func): callback function that will be passed string updates

	Returns:
		long : R1 Downtime in seconds that was placed into the shift.  If R2 then 0.0 is returned

	Note: All events are added at their offset, there is no check for offsets that would be added to the
		line after the shift end date.
	"""
	(OPERATIONUUIDNONE,OPERATIONUUID,PRODUCTCODE,WORKORDER,MODERUNNING,MODECHANGEOVER,MODEIDLE,STATERUNNING,STATEDOWN) = getDefaults(isR1)


	db = getDbName(isR1)

	if len(events) < 1:
		if reportBack != None:
			report = 'No Events Found'
			reportBack(report)
		return
	if reportBack != None:
		report = '%s events read in' % len(events)
		reportBack(report)
	
	# Get the amount of time the shift is
	shiftSeconds = system.date.secondsBetween(shiftStartDT,shiftEndDT)
	
	r1count = 0.0
	downtime = 0.0

	if isR1:
		downtime = placeR1Events(events,STATERUNNING,shiftStartDT,shiftEndDT,lineEQPath,db,reportBack)
				
	else:
		placeR2Events(events,STATERUNNING,shiftStartDT,shiftEndDT,lineEQPath,db,reportBack)
	if reportBack != None:	
		report = 'Events Placed into History'
		reportBack(report)
	
	return downtime	
		
		
		


def writeR1KPIs(lineName,shiftStartDT,shiftEndDT,shift,r1Downtime,r1Count,rate,reportBack = None):
	"""Places the events into the line between start and end

	This function takes the each event defined by a row in events and adds it to the line. The events
		are all in seconds offset from the shift start.

	Args:
		lineName (str): The name of the line as it appears in the sepasoft Equipment Manager
		shiftStartDT (java.util.date): The datetime start of the shift
		shiftEndDT (java.util.date): The datetime end of the shift
		shift (int): 1,2,or 3 that specifies the shift to write the KPI's to
		r1Downtime (long): Downtime in seconds for the shift
		r1Count (long): The number of produced units for the shift
		rate (long): The rate of the line in seconds per unit
		reportBack (func): callback function that will be passed string updates

	Returns:
		None

	"""

	import math
	#TODO Deal with the MES Extension Values saved to the db
	#The shift end KPIs need to be added to the MES Extension DB to make everything report wise works.
	if reportBack != None:

		report = 'Starting KPI add'
		reportBack(report)
	
	dbDate = system.date.format(shiftStartDT, "yyyy/MM/dd")
	businessDate = dbDate
	lineCode = lineName
	#get and set goals for schedule
	project.Lodestar.Scheduling.Schedule.recordShiftParameters(lineCode, dbDate, shift)
	
	lostUnits = r1Downtime * rate
	produced = math.floor(r1Count)

	data = {}
	data["LOST_UNITS"] = int(lostUnits)
	data["AY"] = produced / (produced + lostUnits)
	data["UNITS_PRODUCED"] = int(produced) 
	data["FPY"] = 1 
	data["SCHEDULED_UNITS"] = int(produced + lostUnits) 
	data["THEORETICAL_UNITS"] = int(produced + lostUnits) 
	data["SHIFT_END_TIME"] = system.date.format(shiftEndDT, "yyyy-MM-dd HH:mm")
	data["SHIFT_START_TIME"] = system.date.format(shiftStartDT, "yyyy-MM-dd HH:mm")
	data["SHIFT_GROUP"] = ''
	 
	#the rest of the parameters are handled by the recordShiftParameters  uncomment to use 
	#data["TOTAL_LABOR_GOAL"] = ''
	#data["HPU_GOAL"] = ''
	#data["CAL_LINE_GOAL"] = ''
	#data["CAL_LINE_GOAL"] = ''
	#data["AY_LINE_GOAL"] = ''
	#data["FPY_LINE_GOAL"] = ''
	if reportBack != None:
		report = "Adding the KPI's for the script"
		reportBack(report)
	for key in data:
		project.Lodestar.General._setProductionParameter(lineCode, shift, businessDate, key, data[key])	
	
	if reportBack != None:
		report = "Finished adding KPIs"
		reportBack(report)	


def simulateWithEvents(lineEQPath,startDT,endDT,events,shift,rate,isR1,reportBack = None,idleMinutes=5,changeOverSeconds=1):
	"""Simulator point of entrance to allow for simulation of a shift

	This function will take an existing line and clear out all downtime for a shift starting at startDT.  It will
		clear out from (idleMinutes) minutes before the shift the endDT, set the line to idle, then changover at 
		startDT for (changeOverSeconds) seconds, before changing the line to running.  It will chang the line to 
		idle at the end of the shift.  KPI's will be placed for any R1 lines supplied.  Downtime events passed in
		the event parameter will be added to the line and counts removed for that time.

	Args:
		lineEQPath (str): The name of the line as it appears in the sepasoft Equipment Manager
		startDT (java.util.date): The datetime start of the shift
		endDT (java.util.date): The datetime end of the shift
		events (list [][]): (ID,State,Start,Duration,MLC,Repeat Timer) for R1 (ID,State,Start,Duration,Position,Repeat Timer) for R2
		shift (int): 1,2,or 3 that specifies the shift to write the KPI's to
		rate (long): The rate of the line in seconds per unit
		isR1 (bool): Boolean flag for if the equipment is R1 or R2
		reportBack (func): callback function that will be passed string updates
		idleMinutes (int): The amount of time prior to the startDT that info will be cleared out.
		changeOverSeconds (int): The amount of seconds that the line should be in changeover before shifting to running

	Returns:
		Bool: Returns if there was a problem with the simulation.  False = Good, True = problem

	Notes:
		Events Breakdown
			2 dimentional list. Each line defines an event for the simulation.  The simulation grabs equipment lists from the 
			Model Cache.

			R1
			(ID,State,Start,Duration,MLC,Repeat Timer) = (Int,Int,int,int,int,int)
			ID (int): 0 based index for the workstation. Combined with MLC it specifies a specific workstation for the downtime.
			State (int): The downtime state to be placed into the historical record
			Start (int): The time in seconds after the start of the shift for the event to start
			Duration(int): The time in seconds of how long the event should last
			MLC (int): Casted to bool for R1, Switches between the MLC and non MLC equipment list when determining which equipment to place downtime in
			Repeate Timer (int): Seconds after the event ends before starting again. Set to -1 for no repeat 

			R2
			(ID,State,Start,Duration,Position,Repeat Timer) = (Int,Int,int,int,bool,int)
			ID (int): 0 based index for the workstation. Combined with MLC it specifies a specific workstation for the downtime.
			State (int): The downtime state to be placed into the historical record
			Start (int): The time in seconds after the start of the shift for the event to start
			Duration(int): The time in seconds of how long the event should last
			Position (int): Position of the downtime compared to the key cell.  -1 = before key cell,0 = key cell, 1 = after key cell
			Repeate Timer (int): Seconds after the event ends before starting again. Set to -1 for no repeat 



	"""

	if reportBack != None: 
		reportBack('Starting Simulation on %s' % lineEQPath)

	#Get the line name from the path
	lineName = lineEQPath.split('\\' )[-1]
	
	#Set downtime in case write from file is not enabled
	r1Downtime = 0.0
	
	
	#Write an ideal shift
	idealCount = cleanShift(lineEQPath,startDT,endDT,rate,isR1,reportBack,idleMinutes,changeOverSeconds)
	
	if reportBack != None:
		reportBack('Starting Events')

	#write specified downtime events
	r1Downtime = placeEvents(events,startDT,endDT,lineEQPath,isR1,reportBack)
	
	
	
	if isR1:
		r1Count = (system.date.secondsBetween(startDT,endDT) - r1Downtime)/rate
		writeR1KPIs(lineName,startDT,endDT,shift,r1Downtime,r1Count,rate,reportBack)

	if reportBack != None:
		reportBack('END SIMULATION')

	return False
	
def simulateWithOutEvents(lineEQPath,startDT,endDT,shift,rate,isR1,reportBack = None,idleMinutes = 5,changeOverSeconds=1):
	"""Simulator point of entrance to allow for simulation of a shift

	This function will take an existing line and clear out all downtime for a shift starting at startDT.  It will
		clear out from (idleMinutes) minutes before the shift the endDT, set the line to idle, then changover at 
		startDT for (changeOverSeconds) seconds, before changing the line to running.  It will chang the line to 
		idle at the end of the shift.  KPI's will be placed for any R1 lines supplied.

	Args:
		lineEQPath (str): The name of the line as it appears in the sepasoft Equipment Manager
		startDT (java.util.date): The datetime start of the shift
		endDT (java.util.date): The datetime end of the shift
		shift (int): 1,2,or 3 that specifies the shift to write the KPI's to
		rate (long): The rate of the line in seconds per unit
		isR1 (bool): Boolean flag for if the equipment is R1 or R2
		reportBack (func): callback function that will be passed string updates
		idleMinutes (int): The amount of time prior to the startDT that info will be cleared out.
		changeOverSeconds (int): The amount of seconds that the line should be in changeover before shifting to running

	Returns:
		Bool: Returns if there was a problem with the simulation.  False = Good, True = problem

	Notes:
		

	"""
	if reportBack != None:
		reportBack('Starting Simulation on %s' % lineEQPath)


	#Get the line name from the path
	lineName = lineEQPath.split('\\' )[-1]

	#Set downtime in case write from file is not enabled
	r1Downtime = 0.0

	
	#Write an ideal shift
	idealCount = cleanShift(lineEQPath,startDT,endDT,rate,isR1,reportBack,idleMinutes,changeOverSeconds)
	reportBack('%s %s' % (type(idealCount), idealCount))
	
	if isR1:
		writeR1KPIs(lineName,startDT,endDT,shift,r1Downtime,idealCount,rate,reportBack)

	if reportBack != None:
		reportBack('END SIMULATION')


def logToInternal(report):
	"""Sample reportback function that loggs messages to the ignition log at debug level

	This function is a sample function for the reportback functionality of the simulator. This function
		will pass any string updates to the internal ignition logger at a debug reporting level.

	Args:
		report (str): String to pass to the simulator.

	Returns:
		None

	Notes:
		

	"""
	logger = system.util.getLogger('BackdateSim')
	if logger.idDebugEnabled():
		logger.debug(report)
