#returns alert count for Process Check and Quality Alert
def getAlertCounts():
	#-----get process check alarms count-----
	failedCheckResults = system.alarm.queryStatus(state=['ClearUnacked'], source = ["*Alerts_Monitoring/Process Checks/Failed Check/AlertTag*"], includeSystem=False)
	outOfTimeResults = system.alarm.queryJournal(state=['ClearUnacked'], source = ["*Alerts_Monitoring/Process Checks/Out of Time/AlertTag*"], includeSystem=False)
	scheduledRunResults = system.alarm.queryJournal(state=['ClearUnacked'], source = ["*Alerts_Monitoring/Process Checks/Scheduled Run/AlertTag*"], includeSystem=False)
		
	totalAlertSet = []
	
	#print"Process Check Failed Check Results:"
	for item in failedCheckResults:
		itemID = item.getId()
		#print "\tUUID:", itemID
		totalAlertSet.append(itemID)
	
	#print "Process Check Out of Time Results:"		
	for item in outOfTimeResults:
		itemID = item.getId()
		#print "\tUUID:", itemID
		totalAlertSet.append(itemID)

	#print "Process Check Scheduled Run Results:"		
	for item in scheduledRunResults:
		itemID = item.getId()
		#print "\tUUID:", itemID
		totalAlertSet.append(itemID)
					
		
	#-----get quality alarms count-----
	qualityResults = system.alarm.queryStatus(state=['ActiveUnacked', 'ClearUnacked'], source = ["*AlertsMonitoring/OEE Defect Count*"], includeSystem=False)

	#print "Quality Alert Results:"		
	for item in qualityResults:
		itemID = item.getId()
		#print "\tUUID:", itemID
		totalAlertSet.append(itemID)


	#-----get Changeover alarms count-----
	changeoverResults = system.alarm.queryStatus(state=['ActiveUnacked', 'ClearUnacked'], source = ["*/AlertsMonitoring/Upcoming Changeover*"], includeSystem=False)
		
	#print "Changeover Alert Results:"		
	for item in changeoverResults:
		itemID = item.getId()
		#print "\tUUID:", itemID
		totalAlertSet.append(itemID)
		
	
	#-----get total alarm count-----
	#convert to a set to only get unique alarms
	totalAlertSet = set(totalAlertSet)
	#print "\nTrimmed Alert Set:" 
	#for item in totalAlertSet:
		#print "\tUUID", item
		
	totalCount = len(totalAlertSet)
	
	return totalCount
