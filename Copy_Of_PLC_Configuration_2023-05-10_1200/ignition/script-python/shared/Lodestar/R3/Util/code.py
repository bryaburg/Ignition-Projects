from java.util import Date

def clientScope():
	if hasattr(system.util,'invokeLater'):
		return True
	else:
		return False
				
def updateSAPTags(tagName,start,error=False):
	#define paths to all tags
	parentPath = 'SAP/%s' % tagName
	inProgPath = '%s/In Progress' % parentPath
	execStartPath = '%s/Performance/Current Execution Start' % parentPath
	lastExecStartPath = '%s/Performance/Last Execution Start' % parentPath
	lastExecEndPath = '%s/Performance/Last Execution End' % parentPath
	lastExecPath = '%s/Execution/Last Execution' % parentPath
	errorPath = '%s/SAP Request Error' % parentPath
	#get dates
	now = system.date.now()
	curExecStart = system.tag.readBlocking(execStartPath)[0].value
	#update based on whether it is the start or end of an SAP update request
	if start: #this is the beginning of an SAP request, Set In Progress tag to true and update current execution start tag
		paths = [inProgPath,execStartPath,errorPath]
		vals = [True,now,False]
	else:
		paths = [inProgPath,execStartPath,lastExecStartPath,lastExecEndPath,lastExecPath,errorPath]
		vals = [False,None,curExecStart,now,now,error]
	#update all tags
	system.tag.writeAll(paths,vals) 
			
def sap2mesDateFormat(dateStr):
	if dateStr is None or dateStr == '0000-00-00':
		return None
	else:
		year,month,day = dateStr.replace(' 00:00:00','').split('-')
		date = system.date.getDate(int(year),int(month)-1,int(day))
		date = system.date.setTime(date,0,0,0)
		return date

def sap2mesDateFormat2(dateStr):
	if dateStr is None or dateStr == '0000-00-00' or dateStr == '-  -':
		return None
	else:
		return dateStr

def sap2mesDateFormat3(dateStr):
	if dateStr is None or dateStr == '000000':
		return None
	else:
		return dateStr
		
def sapdateformat(dateStr):
	try:
		return system.date.parse(dateStr, YYYY-MM-DD)
	except:
		return None

def setTimeOnDate(timePart, datePart):
	return system.date.setTime(
		datePart,
		system.date.getHour24(timePart), 
		system.date.getMinute(timePart), 
		system.date.getSecond(timePart)
	)
	
def timestampToDate(timestamp):
	# converts java.sql.Timestamp to java.util.Date
	return Date(timestamp.getTime())
		
def stripMillis(date):
	formatted = system.date.format(date, 'yyyy-MM-dd HH:mm:ss')
	return system.date.parse(formatted)
	
def filterDataSet(ds, filters):
	"""
		Overview:
			Takes a dataset and returns a new dataset containing only rows that satisfy the filters
		Arguments:
			ds - The original dataset to operate on
			filters - A string that can be converted to a Python dictionary. Keys are column names,
				values are what is checked for equivalency in the column specified by the key
	"""
	filters = eval(filters)
	rowsToDelete = []
	for row in range(ds.getRowCount()):
		for key in filters:
			filterVal = filters[key]
			if filterVal != None and filterVal != '':
				dsVal = str(ds.getValueAt(row, key))
				if dsVal != filterVal:
					rowsToDelete.append(row)
					break
	return system.dataset.deleteRows(ds, rowsToDelete)
	
def filterDataSetWildcard(ds, filters):
	"""
	Overview:
		Takes a dataset and returns a new dataset containing only rows that satisfy the filters
		Allows the use of a wildcard (*)
	Arguments:
		ds - The original dataset to operate on
		filters - A string that can be converted to a Python dictionary. Keys are column names,
			values are what is checked for equivalency in the column specified by the key
	"""
	filters = eval(filters)
	rowsToDelete = []
	for row in range(ds.getRowCount()):
		for key in filters:
			filterVal = filters[key]
			if '*' in filterVal:
				filterVal = filterVal.replace('*','')
				filterVal = filterVal.strip()
				dsVal = str(ds.getValueAt(row, key))
				if filterVal.lower() in dsVal.lower():
					continue
				else:
					rowsToDelete.append(row)
			if filterVal != None and filterVal != '':
				dsVal = str(ds.getValueAt(row, key))
				if dsVal != filterVal:
					rowsToDelete.append(row)
					break
	return system.dataset.deleteRows(ds, rowsToDelete)
	
def transformComp(component,direction):
	"""
	Disclaimer:
		Do not stop preview mode in the designer when this script is running, wait for the 'transform complete' 
		print statement before stopping preview mode, else the component may stay at its hidden coordinates. 
	Overview:
		If component is invisible:
			Takes the component and moves it into view in the direction of the direction argument.
		If component is visible:
			Takes the component and moves it in the opposite direction from the direction argument given, 
			then makes the component invisible and puts it back in its original location.
			
		Use this script for components within 10 pixels of the edge of a group or container and set the direction
		to that edge. 
		
		If the argument 'd' is passed, the component will appear from above moving in a downward fashion.
		The distance the component will be moved will always be 10 pixels + the height (or width) from the 
		original coordinate. For instance, if a component is 100x100 (w,h) pixels and sits at 0,0 (x,y) 
		it will be moved to 0,-110 where the bottom of the component is now 10 pixels above the original 
		top of the component. If the same component was passed the 'l' (left) direction, it would move to 
		-110,0 where the right of the component is 10 pixels away from the original left side of the component.
		Upon completion of moving to the hidden coordinates, the component will then set its visibility property
		to false and return itself to its original coordinates for design and debug.
		
		
	Arguments:
		component = The component you wish to move [component]
		direction = The direction you want the component to come into view up,down,left,right ('u','d','l','r') [string]
	Return datatype:
		None
	"""
	def resetPosition():	
		component.visible = 0
		system.gui.transform(
			component,
			x, y,
			callback = notify
			)
		
	def slideOut():
		component.visible = 1
		system.gui.transform(
				component,
				x, y,
				duration = 1000,
				acceleration=system.gui.ACCL_FAST_TO_SLOW,
				callback = notify
				)
		
	def hidePosition(coordinate,size,direction):
		positions = {
			'u' : (size + 10 + coordinate),
			'd' : (coordinate - size - 10),
			'l' : (coordinate - size - 10),
			'r' : (size + 10 + coordinate)
			}
		return positions[direction]
	def notify():
		print 'transform complete (%s,%s)' % (x,y)
		
	x = component.getX()
	y = component.getY()
	width = component.getWidth()
	height = component.getHeight()
	direction = direction.lower()
	
	if direction in ('u','d'):
		size = height
		hidePositionX = x
		hidePositionY = hidePosition(y,size,direction)
	elif direction in ('l','r'):
		size = width
		hidePositionX = hidePosition(x,size,direction)
		hidePositionY = y
	
	if component.visible:
		system.gui.transform(
		component,
		hidePositionX, hidePositionY,
		duration = 1000,
		acceleration = system.gui.ACCL_FAST_TO_SLOW,
		callback = resetPosition
		)
	else:
		system.gui.transform(
			component,
			hidePositionX, hidePositionY,
			duration = 0,
			callback = slideOut
			)
			
def dsColToList(ds, colName):
	vals = []
	for row in range(ds.getRowCount()):
		val = ds.getValueAt(row, colName)
		vals.append(val)
	return vals
	
def isStraggler(stragglers, orders, row):
	orderQty = orders.getValueAt(row, 'REMQTY')
	orderModel = orders.getValueAt(row, 'MATERIAL')
	for i in range(stragglers.rowCount):
		stragDiff = stragglers.getValueAt(i, 'QTYDIFF')
		stragModel = stragglers.getValueAt(i, 'MATERIAL')
		if stragDiff == orderQty and stragModel == orderModel:
			return True
	return False
		
def isStragglerByModel(linePath, orderModel, orderQty):
	stragglerPath = '%s/PLC SCHED/Stragglers' %linePath
	stragglers = system.tag.readBlocking(stragglerPath)[0].value
	print stragglers
	for i in range(stragglers.rowCount):
		stragDiff = stragglers.getValueAt(i, 'QTYDIFF')
		stragModel = stragglers.getValueAt(i, 'MATERIAL')
		print stragDiff, stragModel

		if stragDiff == orderQty and stragModel == orderModel:
			return True
	return False
	
def isCurrentDay(reqDate):
	reqDate = system.date.midnight(reqDate)
	today = system.date.midnight(system.date.now())
	return reqDate == today
	
def getOpRequestUUID(scheduleEntryUUID):
	schedItemList = system.mes.loadSchedule(scheduleEntryUUID)
	for item in schedItemList:
		if item.getMESObjectTypeName() == 'OperationsRequest':
			return item.getUUID()
			
def getWorkOrderUUID(workOrderName):
	workOrder = system.mes.workorder.getMESWorkOrder(workOrderName)
	return workOrder.getUUID()