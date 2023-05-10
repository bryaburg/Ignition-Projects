from java.awt import Color
from shared.Lodestar.R3.Config import projectName
from shared.Lodestar.R3.Util import clientScope, setTimeOnDate

def getScheduleMeta(prodLine, reqDate, scheduledTotal):
	schedule = getTodayLineSchedule(prodLine, reqDate)
	if schedule is not None and schedule.rowCount > 0 and scheduledTotal > 0:
		shifts = ['S1', 'S2', 'S3']
		shiftsActive = (
			schedule.getValueAt(0,'S1_ACTIVE'),
			schedule.getValueAt(0,'S2_ACTIVE'),
			schedule.getValueAt(0,'S3_ACTIVE')
		)

		productionStart = getProductionStartTime(reqDate, schedule, shiftsActive)
		productionEnd = getProductionEndTime(reqDate, schedule, shiftsActive)

		if productionStart is not None and productionEnd is not None:
			breakLen = schedule.getValueAt(0, 'BRK_LEN') * 60
			lunchLen = schedule.getValueAt(0, 'LUNCH_LEN') * 60
			
			nonProdSecs = 0
			breakTimes = getShiftGaps(reqDate, schedule, shiftsActive)
			for shiftGap in breakTimes:
				start = shiftGap['start']
				end = shiftGap['end']
				gapLength = system.date.secondsBetween(start, end)
				nonProdSecs += gapLength
			for i in range(len(shifts)):
				if shiftsActive[i]:
					nonProdSecs += (breakLen * 2) + lunchLen
					shiftBreaks = getShiftBreaks(reqDate, schedule, shifts[i], breakLen, lunchLen)
					breakTimes.extend(shiftBreaks)

			secondsAvailable = system.date.secondsBetween(productionStart, productionEnd)
			secondsPerUnit = float(secondsAvailable) / scheduledTotal
			
			prodSecs = secondsAvailable - nonProdSecs
			prodSecsPerUnit = float(prodSecs) / scheduledTotal
			return {'startDate':productionStart,
					'endDate':productionEnd,
					'secondsAvailable':secondsAvailable,
					'secondsPerUnit':secondsPerUnit,
					'nonProdSecs': nonProdSecs,
					'prodSecsPerUnit': prodSecsPerUnit,
					'breakTimes': breakTimes
			}
	return None
	
def getTodayLineSchedule(prodLine,reqDate):
	dow = system.date.getDayOfWeek(reqDate) - 1 #subtract one to account for difference in week start between schedule and getDayOfWeekFunction
	if dow == 0: #sunday, round robin to end of the week
		dow = 7
	params = {'LINE':prodLine,
			  'DOW_INT':dow
			  }
	if clientScope():
		schedule = system.db.runNamedQuery('Scheduling/getActiveDaySchedule',params)
	else:	
		schedule = system.db.runNamedQuery(projectName,'Scheduling/getActiveDaySchedule',params)
	
	return schedule
		
def getProductionStartTime(reqDate, schedule, shiftsActive):
	# assuming S3 is the start of the production day, find the earliest active shift
	datePart = reqDate
	s1Active, s2Active, s3Active = shiftsActive
	if s3Active:
		timePart = schedule.getValueAt(0,'S3_START')
		if system.date.getHour24(timePart) > 12:
			# if shift 3 starts after 12pm, it is safe to assume it starts the day before
			datePart = system.date.addDays(datePart, -1)
	elif s1Active:
		timePart = schedule.getValueAt(0,'S1_START')
	elif s2Active:
		timePart = schedule.getValueAt(0,'S2_START')
	else:
		return None
	return setTimeOnDate(timePart, datePart)
			
def getProductionEndTime(reqDate, schedule, shiftsActive):
	datePart = reqDate
	s1Active, s2Active, s3Active = shiftsActive
	if s2Active:
		timePart = schedule.getValueAt(0,'S2_END')
		if system.date.getHour24(timePart) < 12:
			 #if shift 2 end after midnight, it is safe to assume it ends the day after
			 datePart = system.date.addDays(datePart, 1)
	elif s1Active:
		timePart = schedule.getValueAt(0,'S1_END')
	elif s3Active:
		timePart = schedule.getValueAt(0,'S3_END')
	else:
		return None
	return setTimeOnDate(timePart, datePart)
			
def getShiftGaps(reqDate, schedule, shiftsActive):
	datePart = reqDate
	s1Active, s2Active, s3Active = shiftsActive
	shiftGaps = []
	if s3Active and s1Active:
		# gap between S3 and S1
		gapItem = {}
		
		startTimePart = schedule.getValueAt(0,'S3_END')
		startDateTime = setTimeOnDate(startTimePart, datePart)
		
		endTimePart = schedule.getValueAt(0, 'S1_START')
		endDateTime = setTimeOnDate(endTimePart, datePart)
		
		if system.date.secondsBetween(startDateTime, endDateTime) > 0:
			gapItem['start'] = startDateTime
			gapItem['end'] = endDateTime
			#print 's3-s1 gap', gapItem
			shiftGaps.append(gapItem)
	if s1Active and s2Active:
		# gap between S1 and S2
		gapItem = {}
		
		startTimePart = schedule.getValueAt(0,'S1_END')
		startDateTime = setTimeOnDate(startTimePart, datePart)
		
		endTimePart = schedule.getValueAt(0, 'S2_START')
		endDateTime = setTimeOnDate(endTimePart, datePart)
		if system.date.secondsBetween(startDateTime, endDateTime) > 0:
			gapItem['start'] = startDateTime
			gapItem['end'] = endDateTime
			#print 's1-s2 gap', gapItem
			shiftGaps.append(gapItem)
	return shiftGaps
		
def getShiftBreaks(reqDate, schedule, shift, breakLen, lunchLen):
	breakNames = ['BRK1', 'BRK2', 'LUNCH']
	shiftBreaks = []
	for breakName in breakNames:
		timePart = schedule.getValueAt(0, shift+'_'+breakName)
		breakStart = setTimeOnDate(timePart, reqDate)
		if breakName == 'LUNCH':
			breakEnd = system.date.addSeconds(breakStart, lunchLen)
		else:
			breakEnd = system.date.addSeconds(breakStart, breakLen)
		
		shiftBreaks.append({'start': breakStart, 'end': breakEnd})
	return shiftBreaks
	
def getNextProductionDays(startDate, prodLine):
	#startDate = system.date.setTime(system.date.now(),0,0,0)

	shiftSchedule = getShiftSchedule(prodLine)
	daysToSchedule = system.tag.readBlocking('R3/Config/Days To Schedule')[0].value
	
	dates = []
	paths = []
	
	for i in range(daysToSchedule):
		date = getNextProductionDay(startDate, shiftSchedule)
		dates.append(date)
		
		path = 'D%s' %str(i + 1)
		paths.append(path)
		
		startDate = system.date.addDays(date, 1)
		
	return dates, paths
	
def getNextProductionDay(startDate, shiftSchedule):
	dowInt = system.date.getDayOfWeek(startDate)
	# convert to SchedActive format where monday = 1
	dowInt = ((dowInt + 5) % 7) + 1
	
	row = dowInt - 1 # shiftSchedule ordered by dowInt but starting at 1
	
	s1Active = shiftSchedule.getValueAt(row, 'S1_Active')
	s2Active = shiftSchedule.getValueAt(row, 'S2_Active')
	s3Active = shiftSchedule.getValueAt(row, 'S3_Active')
	
	if s1Active or s2Active or s3Active:
		return startDate
	else:
		startDate = system.date.addDays(startDate, 1)
		return getNextProductionDay(startDate, shiftSchedule)
		
def getNextReqDate(prodLine):
	siteName = shared.Lodestar.R3.Config.getSiteName()
	endPath = 'Whirlpool MES/%s/Assembly/%s/PLC SCHED/ProductionEnd' %(siteName, prodLine)
	curEnd = system.tag.readBlocking(endPath)[0].value
	
	startDate = system.date.midnight(curEnd)
	
	nextProdDays, paths = shared.Lodestar.R3.Production.getNextProductionDays(startDate, prodLine)
	for date in nextProdDays:
		if system.date.isAfter(date, curEnd):
			return date
			
def setProdTimes(prodLine):
	siteName = shared.Lodestar.R3.Config.getSiteName()
	nextReqDate = getNextReqDate(prodLine)
	
	schedTotal = 1 # not the actual scheduled but irrelevant here
	meta = shared.Lodestar.R3.Production.getScheduleMeta(prodLine, nextReqDate, schedTotal)
	
	prodStart = meta['startDate']
	prodEnd = meta['endDate']
	
	startPath = 'Whirlpool MES/%s/Assembly/%s/PLC SCHED/ProductionStart' %(siteName, prodLine)
	endPath = 'Whirlpool MES/%s/Assembly/%s/PLC SCHED/ProductionEnd' %(siteName, prodLine)

	paths = [startPath, endPath]
	vals = [prodStart, prodEnd]
	
	system.tag.writeAll(paths, vals)
			
def getShiftSchedule(line):
	params = {'prodLine': line}
	if clientScope():
		shiftSchedule = system.db.runNamedQuery('Scheduling/getActiveWeekSchedule', params)
	else:
		shiftSchedule = system.db.runNamedQuery(projectName, 'Scheduling/getActiveWeekSchedule', params)
	return shiftSchedule
	
def getProdDayScheduleEntries(linePath, prodStart, prodEnd):
	# wrapper for getEquipmentScheduleEntires to further filter based on time part
	mesSchedule = system.mes.getEquipmentScheduleEntries(linePath,prodStart,prodEnd,'Active',False)
	prodSchedule = []
	for item in mesSchedule:
		schedStart = item.getUserScheduledStartDate()
		if system.date.isBetween(schedStart, prodStart, prodEnd):
			prodSchedule.append(item)
	return prodSchedule
	
def getBreakEvents(prodLine, reqDate):
	schedule = getTodayLineSchedule(prodLine, reqDate)
	
	breakLen = schedule.getValueAt(0, 'BRK_LEN') * 60
	lunchLen = schedule.getValueAt(0, 'LUNCH_LEN') * 60
	
	shifts = ['S1', 'S2', 'S3']
	shiftsActive = (
		schedule.getValueAt(0,'S1_ACTIVE'),
		schedule.getValueAt(0,'S2_ACTIVE'),
		schedule.getValueAt(0,'S3_ACTIVE')
	)
	color = Color(255, 255, 0, 125)
	breakEventHeaders = ['StartDate', 'EndDate', 'Color']
	breakEventVals = []
	for i in range(len(shifts)):
		if shiftsActive[i]:
			shiftBreaks = getShiftBreaks(reqDate, schedule, shifts[i], breakLen, lunchLen)
			for breakEvent in shiftBreaks:
				startDate = breakEvent['start']
				endDate = breakEvent['end']
				breakEventVals.append([startDate, endDate, color])
	
#	shiftGaps = getShiftGaps(reqDate, schedule, shiftsActive)
#	for breakEvent in shiftGaps:
#		startDate = breakEvent['start']
#		endDate = breakEvent['end']
#		breakEventVals.append([startDate, endDate, color])
		
	return system.dataset.toDataSet(breakEventHeaders, breakEventVals)