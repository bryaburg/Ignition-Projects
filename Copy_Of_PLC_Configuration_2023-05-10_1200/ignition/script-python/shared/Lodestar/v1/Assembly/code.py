###MAIN CALLS
from shared.Lodestar.DecoupleHelpers import *
from com.inductiveautomation.ignition.common import BasicDataset

def CurrentStatus(line):

	##These two sections can be broken into their own calls to be individually mapped
	##For simplicity, sprint one combined them.
	
	returnValues =[]
	
	#Set tagpaths
	schedulePath = "[default]Scheduling/{{LINE}}/Schedule/".replace("{{LINE}}",line)
	dashboardPath = "[default]Dashboard/{{LINE}}/CurrentShift/".replace("{{LINE}}",line)
	livePath = "[MES]Whirlpool MES/M003 Clyde/Assembly/{{LINE}}/Main Line/Live Analysis/Downtime/".replace("{{LINE}}", line)
	dailyTotalsPath = "[default]Dashboard/{{LINE}}/".replace("{{LINE}}", line)
	
	##Data from Schedule folder
	#Units_Produced
	#Units_Lossed
	#AY_Percent
	#Shift
	#State
	#Brk_LEN
	#Scheduled_Units
	
	scheduleTagList =["Units_Produced", "Units_Lossed","AY_Percent", "Shift", "PreviousShift", "State", "Brk_LEN","Scheduled_Units","Theoretical_Units","Shift_Active"]
	scheduleTagPaths = tagPathBuilder(schedulePath, scheduleTagList)
	
	#Read all values
	scheduleReadValues = system.tag.readAll(scheduleTagPaths)
	
	#Convert
	scheduleObject = cleanValues(scheduleTagList, scheduleReadValues)

	##Data from Dashboard folder
	#TargetAY
	#TargetFPY
	#RepairUnits
	#Repair
	#CAL
	#CALTarget
	#CALDataset 
	
	dashboardTagList = ["TargetAY", "TargetFPY", "FPY", "RepairUnits", "Repair", "CAL", "CALTarget","CALDataset"]
	dashboardTagPaths = tagPathBuilder(dashboardPath, dashboardTagList)
	
	#Read all values
	dashboardReadValues = system.tag.readAll(dashboardTagPaths)
	
	#Convert
	dashboardObject = cleanValues(dashboardTagList, dashboardReadValues)
	
	##Data from Live Analysis
	#Unplanned Downtime
	
	liveTagList = ["Unplanned Downtime"]
	liveTagPaths = tagPathBuilder(livePath, liveTagList)
	
	#Read all values
	liveReadValues = system.tag.readAll(liveTagPaths)
	
	#Convert
	liveObject = cleanValues(liveTagList, liveReadValues)
	
	##Data from Dailys Dashboard folder
	#DayAYTarget
	#DayCALTarget
	#DayFPYTarget
	
	dailyTotalsTagList = ["DayAYTarget", "DayCALTarget", "DayFPYTarget"]
	dailyTotalsTagPaths = tagPathBuilder(dailyTotalsPath, dailyTotalsTagList)
	dailyTotalsValues = system.tag.readAll(dailyTotalsTagPaths)
	dailyTotalsObject = cleanValues(dailyTotalsTagList, dailyTotalsValues)
	
	
	return {"schedule": scheduleObject, "dashboard": dashboardObject, "live":liveObject, "daily_targets": dailyTotalsObject}

def PreviousStatus(line):
	returnValues = {}
	
	#Set top level tag paths
	bottomPath = "[default]Dashboard/{{LINE}}/PrevShiftBot/".replace("{{LINE}}",line)
	topPath = "[default]Dashboard/{{LINE}}/PrevShiftTop/".replace("{{LINE}}",line)
	
	#Data from top and bottom folders are the same.
	#AccumulatedDowntime
	#CAL
	#CALDataset
	#CALTarget
	#DateString
	#Repair
	#RepairUnits
	#TargetAY
	#TargetFPY
	
	tagNames = ["AY", "AYTarget", "CAL", "CALTarget", "DateString", "Downtime", "FPY", "FPYTarget","Lost","Produced","Repair","RepairUnits","Scheduled","Shift"]
	
	#Create full tagpaths
	bottomTags = tagPathBuilder(bottomPath, tagNames)
	topTags = tagPathBuilder(topPath, tagNames)
	
	#Read values
	bottomValues = system.tag.readAll(bottomTags)
	topValues = system.tag.readAll(topTags)
	
	#Convert
	bottomObject = cleanValues(tagNames, bottomValues)
	topObject = cleanValues(tagNames, topValues)
	
	#Return
	return {"PreviousBottom" : bottomObject, "PreviousTop": topObject}

def LineState(line):
	base = "[default]Dashboard/{{LINE}}/LineStatus/".replace("{{LINE}}", line)
	tags = ['state_name', 'state_value', 'state_text']
	paths = tagPathBuilder(base, tags)
	values = system.tag.readAll(paths)
	
	state = cleanValues(tags, values)
	return state
	
def LineCurrentData(line):
	shift_range = getLineCurrentShiftRange(line)
	repairs_top = getLineTopRepairs(line, shift_range['start'], shift_range['end'], 5)
	cals = getLineCALs(line)
	
	downtime = getLineCachedDowntime(line, shift_range['start'], shift_range['end'], True, True)
	downtime_reasons = getDowntimeReasons(downtime)
	downtime_grouped = getGroupedDowntime(downtime, downtime_reasons)
	downtime_top = getTopDowntime(downtime_grouped, 5)
	
	return {
		'top_downtime': toObjectList(downtime_top), 
		'top_repairs': toObjectList(repairs_top),
		'cals': toObjectList(cals)
	}
	
def LineMessage(line):
	return getLineMessage(line)
	
def gatewayHandle_updateLineStatus():
	lines = toObjectList(getLines())
	for line in lines:
		state = getLineState(line['LineName'])
		writeLineState(line['LineName'], state)
