from com.inductiveautomation.ignition.common import BasicDataset

def toObjectList(dataset):
	ls = []
	pyDataSet = system.dataset.toPyDataSet(dataset)
	columns = pyDataSet.getColumnNames()
	
	# Convert the dataset to an object list
	for row in pyDataSet:	
		obj = {}
		for column in columns:
			obj[column] = row[column]
		
		ls.append(obj)
	
	return ls
	
def tagPathBuilder(base, tags):
	returnArray = []
	for tag in tags:
		returnArray.append(base + tag)

	return returnArray
	
def cleanValues(tagNames, readResult):
	#Also for a util
	#Maybe in the future, this can also help understand bad quality tags.
	#but for now, this will create an object(kvp) to be used as the returnd JSON
	logger = system.util.getLogger("ValueCleaner")
	returnObject = {}
	for i in range(len(tagNames)):
		if isinstance(readResult[i].value, BasicDataset):
			returnObject[tagNames[i]] = toObjectList(readResult[i].value)
		else:
			returnObject[tagNames[i]] = readResult[i].value

	return returnObject

def getDowntimeReasons(data):
	reasons = []
	if data != None:
		for row in range(data.rowCount):
			reasonPath = data.getValueAt(row, "Line Downtime Reason Path")
			cellName = data.getValueAt(row, "Line Downtime Equipment Name")
			lst = reasonPath.split("/")
			
			# print "%s - %s " % (cellName, reasonPath)
			if len(lst) > 2:
				group = lst[2]
			else:
				group = "Auto"
			
			reasons.append(cellName + " - " + group)
		
	return list(set(reasons))
	
def getGroupedDowntime(data, reasons):
	hdr = ["label", "number", "type", "EqName", "Grp", "Path"]
	width = len(hdr)
	height = len(reasons)
	myData = [['0',0, '0', '0','0', '0'] for y in range(height)]
	 
	for row in range(data.rowCount):
		reasonPath = data.getValueAt(row, "Line Downtime Reason Path")
		cellName = data.getValueAt(row, "Line Downtime Equipment Name")
		dtMins = data.getValueAt(row, "Line State Duration")
		dtEnd = data.getValueAt(row, "Line State Event End")
		path = data.getValueAt(row, "Line Downtime Equipment Path")
		type = data.getValueAt(row, "Line Downtime Reason")
		
		# LS-73 Fix: Exception: Usupported operand type(s) for +: 'float' and 'NoneType'
		if dtMins == None:
			dtMins = 0.0
			
		lst = reasonPath.split("/")
		if len(lst) > 2:
			group = lst[2]
			groupPath = lst[0] + "/" + lst[1] + "/" + lst[2]
		else:
			group = "Auto"
			groupPath = lst[0] + "/" + lst[1]
		
		groupName = cellName + " - " + group
		idx = reasons.index(groupName)
		val = myData[idx][1]
		
		myData[idx][0] = groupName
		myData[idx][1] = val + dtMins
		myData[idx][2] = type
		myData[idx][3] = cellName
		myData[idx][4] = groupPath
		myData[idx][5] = path
		
	# Only return downtime reasons that accured downtime (greater than 0)
	newData = []
	for row in myData:
		if row[1] != 0:
			newData.append(row)
			
	# Sort the data in reverse order by the downtime amount
	myData = newData
	myData = sorted(myData, key=lambda item: item[1], reverse=True)
	
	# Convert to data to a DataSet
	return system.dataset.toDataSet(hdr, myData)
	
def getUncodedDowntime(analysis, microstopThreshold):
	pyDataSet = system.dataset.toPyDataSet(analysis)
	headers = ['Line Downtime Equipment Name','Line State Name','Line State Duration','Line State Event Begin','Line State Event End']
	count = 0
	data = []
	microstopThreshold = microstopThreshold / (60*1.0)
	
	for row in pyDataSet:
		state = row['Line State Value']
		if (state in [1,2,4,8] or (state > 999 and row['Equipment Note'] == None)) and row['Line State Duration'] >= microstopThreshold:
			count = count + 1
			data.append([row['Line Downtime Equipment Name'], row['Line Downtime Reason'], row['Line State Duration'], row['Line State Event Begin'], row['Line State Event End']])
	
	return {"count": count, "downtime_records": system.dataset.toDataSet(headers, data)}
	
def getTopDowntime(downtime, topAmount):
	returnedDowntime = []
	headers = ["label", "number", "type"]
	pyDataSet = system.dataset.toPyDataSet(downtime)
	
	for i in range(downtime.rowCount):
		if i < topAmount:
			returnedDowntime.append([pyDataSet[i][0], pyDataSet[i][1], pyDataSet[i][2]])
		
	return system.dataset.toDataSet(headers, returnedDowntime)
	
def getLineAnalysis(line, start, end, onlyUnplanned=False, onlyMainline=False):
	filter = "Equipment Path LIKE 'Whirlpool MES\M003 Clyde\Assembly\<<LINE>><<MAIN_LINE>>'".replace("<<LINE>>", line)
	if onlyMainline:
		filter = filter.replace("<<MAIN_LINE>>", "\Main Line")
	else:
		filter = filter.replace("<<MAIN_LINE>>", "")
		
	if onlyUnplanned:
		filter += " AND Line State Type = 'Unplanned Downtime'"
	
	print filter
	groupby = "Line Downtime Equipment Name,Line State Name,Equipment Path,Line Downtime State Time Stamp"
	orderby = "Line Downtime State Time Stamp"
	datapoints = [
		"Line Downtime Equipment Name",
		"Equipment Path",
		"Line State Name",
		"Line State Duration",
		"Line Downtime Reason Path",
		"Line Downtime State Time Stamp",
		"Line Downtime Equipment Path",
		"Equipment Path",
		"Line State Event Begin",
		"Line State Event End",
		"Line State Type",
		"Line State Value",
		"Line Downtime Reason"
	]
	
	settings = system.mes.analysis.createMESAnalysisSettings("getLineAnalysis")
	settings.setDataPoints(datapoints)
	settings.setFilterExpression(filter)
	settings.setGroupBy(groupby)
	settings.setOrderBy(orderby)
	
	return system.mes.analysis.executeAnalysis(start, end, settings).getDataset()
	
def getLineStateTags(line):
	tagPath = "[default]Scheduling/{{LINE}}/Schedule/".replace("{{LINE}}", line)
	tags = [
		'State',
		'Shift',
		'Running',
		'Flex_Active',
		'FlexTimer_HoursDisplay',
		'FlexTimer_MinutesDisplay',
		'FlexTimer_SecondsDisplay',
		'FlexTo_Line/DATA',
		'_1st_Break_Active',
		'_1st_Break_MinRemain_Display',
		'_1st_Break_SecRemain_Display',
		'_2nd_Break_Active',
		'_2nd_Break_MinRemain_Display',
		'_2nd_Break_SecRemain_Display',
		'Lunch_Active',
		'Lunch_MinRemain_Display',
		'Lunch_SecRemain_Display'
	]
	
	#print tagPath
	
	tagPaths = tagPathBuilder(tagPath, tags)
	tagValues = system.tag.readAll(tagPaths)
	
	return cleanValues(tags, tagValues)
	
def getLineDownState(line):
	start = system.date.addSeconds(system.date.now(),-1)
	end = system.date.now()
	out = getLineAnalysis(line, start, end, onlyUnplanned=True, onlyMainline=True)
	
	downState = {'is_down': False, 'equipment_name': '', 'equipment_state_value': -1, 'equipment_reason': '', 'down_start_timestamp': ''}
	if out.hasData():
		downState['is_down'] = True
		downState['equipment_name'] = out.getValueAt(0, 'Line Downtime Equipment Name')
		downState['equipment_state_value'] = out.getValueAt(0, 'Line State Value')
		downState['equipment_reason'] = out.getValueAt(0, 'Line State Name')
		downState['down_start_timestamp'] = out.getValueAt(0, 'Line Downtime State Time Stamp')
		
	return downState
	
def getLineState(line):
	stateTagValues = getLineStateTags(line)
	state = {"state_value": -1, "state_name": 'DEFAULT', "state_text": []}
	
	if stateTagValues['State'] == 0:
		state = {'state_value': 0, 'state_name': 'NOT_SCHEDULED', 'state_text': ['NOT SCHEDULED']}
		
	elif stateTagValues['State'] == 1:
		remaining = "TIME REMAINING "
		if stateTagValues['_1st_Break_Active']:
			remaining = "TIME REMAINING %s:%s" % (stateTagValues['_1st_Break_MinRemain_Display'], stateTagValues['_1st_Break_SecRemain_Display'])
		elif stateTagValues['_2nd_Break_Active']:
			remaining = "TIME REMAINING %s:%s" % (stateTagValues['_2nd_Break_MinRemain_Display'], stateTagValues['_2nd_Break_SecRemain_Display'])
		elif stateTagValues['Lunch_Active']:
			remaining = "TIME REMAINING %s:%s" % (stateTagValues['Lunch_MinRemain_Display'], stateTagValues['Lunch_SecRemain_Display'])
			
		state = {'state_value': 1, 'state_name': 'BREAK', 'state_text': ['BREAK', remaining]}
		
	elif stateTagValues['State'] == 2:
		downState = getLineDownState(line)
		if downState['is_down']:
			state = {'state_value': 2, 'state_name': 'DOWN', 'state_text': ['DOWN', downState['equipment_name'], downState['equipment_reason'], downState['down_start_timestamp']]}
		else:
			state = {'state_value': 2, 'state_name': 'DOWN', 'state_text': ['DOWN']}
	elif stateTagValues['State'] == 3:
		state = {'state_value': 3, 'state_name': 'RUNNING', 'state_text': ['RUNNING']}
		
	elif stateTagValues['State'] == 4:
		flexLine = stateTagValues['FlexTo_Line/DATA']
		flexRemaining = "TIME REMAINING %s:%s" % (stateTagValues['FlexTimer_HoursDisplay'], stateTagValues['FlexTimer_MinutesDisplay'])
		
		state = {'state_value': 4, 'state_name': 'FLEX', 'state_text': ['FLEXING', flexLine, flexRemaining]}
		
	elif stateTagValues['State'] == 5:
		lunchRemaining = "TIME REMAINING %s:%s" % (stateTagValues['Lunch_MinRemain_Display'], stateTagValues['Lunch_SecRemainDisplay'])
		state = {'state_value': 5, 'state_name': 'BREAK', 'state_text': ['BREAK', lunchRemaining]}
	
	return state
	
def writeLineState(line, lineState):
	base = "[default]Dashboard/{{LINE}}/LineStatus/".replace("{{LINE}}", line)
	tags = ['state_value', 'state_name', 'state_text']
	paths = tagPathBuilder(base, tags)
	values = [lineState['state_value'], lineState['state_name'], lineState['state_text']]
	
	system.tag.writeAll(paths, values)
	
def getLines():
	tag_path = '[default]Downtime/Config/ActiveLines'
	return system.tag.readBlocking(tag_path)[0].value
	
def getLineCurrentShiftRange(line):
	# Tag Paths
	start_tag_path = "[default]Scheduling/{{LINE}}/Schedule/Start_Time/DATA".replace("{{LINE}}", line)
	end_tag_path = "[default]Scheduling/{{LINE}}/Schedule/End_Time/DATA".replace("{{LINE}}", line)
	current_shift_tag_path = "[default]Scheduling/{{LINE}}/Schedule/Shift".replace("{{LINE}}", line)
	
	# Obtain the tag values
	start_value = system.tag.readBlocking(start_tag_path)[0].value
	end_value = system.tag.readBlocking(end_tag_path)[0].value
	current_shift_value = system.tag.readBlocking(current_shift_tag_path)[0].value
	
	# Get the current time and whether we are currently in am/pm
	now = system.date.now()
	amPm = system.date.getAMorPM(now)
	
	start = ""
	end = ""
	
	# If we are in third shift and we are 'am' then the start time is yesterday and the end time is today
	# If we are in third shift and we are 'pm' then that means the end time is tomorrow and the start time is today
	# Else first and second shift have start and end times of today
	if current_shift_value == 3 and amPm == 1:
		start = now
		end = system.date.addDays(now, 1)
	elif current_shift_value == 3 and amPm == 0:
		start = system.date.addDays(now, -1)
		end = now
	else:
		start = now
		end = now
		
	# The start and end tag values only house the hour and minute, so we need to split that to utilize system.date.setTime() method
	start_split = start_value.split(':')
	end_split = end_value.split(':')
	
	# system.date.setTime() requires a date object, plus hour, minute, second for the time
	start = system.date.setTime(start, int(start_split[0]), int(start_split[1]), 0)
	end = system.date.setTime(end, int(end_split[0]), int(end_split[1]), 0)
	
	# Return a dict of the range
	return {'start': start, 'end': end}
	
def getLineTopRepairs(line, start, end, topAmount):
	params = {"line": line, "start": start, "end": end, "crated": True}
	results = system.db.runNamedQuery("clymesappq3", "LineRepairs", params)
	pyDataSet = system.dataset.toPyDataSet(results)
	
	returnedRepairs = []
	headers = ['component', 'count', 'line']
	for i in range(pyDataSet.rowCount):
		if i < topAmount:
			returnedRepairs.append([pyDataSet[i][0], pyDataSet[i][1], pyDataSet[i][2]])
		
	return system.dataset.toDataSet(headers, returnedRepairs)
	
def getLineCachedDowntime(line, start, end, onlyUnplanned=False, onlyMainline=False):
	line = line.replace("N", "n")
	tag_path = "[default]Downtime/{{LINE}}Downtime".replace("{{LINE}}", line)
	downtime = system.tag.readBlocking(tag_path)[0].value
	
	pyDataSet = system.dataset.toPyDataSet(downtime) 
	returnedDataSet = system.dataset.clearDataset(downtime)
	for row in pyDataSet:
		event_begin = row['Line State Event Begin']
		event_end = row['Line State Event End']
		type = row['Line Downtime Reason']
		
		if system.date.isBetween(event_begin, start, end):
			if onlyUnplanned and type == "Planned Downtime":
				continue
				
			# LS-77 Fix: Remove planned downtime that is classified as 'Other' if classified as Unplanned only
			reason_split = row['Line Downtime Reason Path'].split("/")
			if onlyUnplanned and len(reason_split) > 2 and reason_split[2] == "Planned":
				continue
				
			# LS-77 Fix: Remove any subline downtime if flagged for Mainline only
			equipment_split = row['Line Downtime Equipment Name'].split("-")
			if onlyMainline and len(equipment_split) > 1 and equipment_split[0][-1] == 'S':
				continue
		
			to_append = [
				row['Line State Event Begin'],
				row['Line State Event End'],
				row['Line State Duration'],
				row['Line Downtime Equipment Path'],
				row['Line Downtime Original Equipment Path'],
				row['Line Downtime Equipment Name'],
				row['Line Downtime Reason'],
				row['Line Downtime Reason Path'],
				row['Equipment Note'],
				row['Line State Value'],
				row['Line Downtime State Time Stamp'],
				row['Line Downtime End State Time Stamp'],
				row['Line Downtime Reason Split'],
				row['Line Downtime Can Revert Split'],
				row['Line Downtime Event Sequence'],
				row['Line Downtime Occurrence Count'],
				row['Line State Override Type'],
				row['Line State Override Scope'],
				row['Line State Overridden'],
				row['State Begin Time']
			]
			
			returnedDataSet = system.dataset.addRow(returnedDataSet, to_append)

	return returnedDataSet
			
def getLineCALs(line):
	tag_path = "[default]Dashboard/<<LINE>>/CurrentShift/CALDataset".replace("<<LINE>>", line)
	return system.tag.readBlocking(tag_path)[0].value
	
def getLineMessage(line):
	base = '[default]Dashboard/{{LINE}}/LineStatus/'.replace("{{LINE}}", line)
	tags = ['message_timestamp', 'message_user', 'message_text']
	paths = tagPathBuilder(base, tags)
	
	values = system.tag.readAll(paths)
	return cleanValues(tags, values)