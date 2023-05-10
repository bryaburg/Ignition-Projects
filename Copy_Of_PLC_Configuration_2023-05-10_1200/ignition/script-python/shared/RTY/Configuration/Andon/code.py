from shared.RTY.General import getShift, sendEmail, getRTYDb
from shared.Common.Util import getUser

def clearAlarmByName(alarmName):
	# This is called to clear an alarm state
	print 'clearAlarmByName Update Alarm', alarmName,
	tagPath = '[State_RTY]Alarms'
	system.tag.writeAll([tagPath + "/" + alarmName + "/NextRefreshTime", 
						 tagPath + "/" + alarmName + "/CurrentDefectCount"],
						[system.date.getDate(2099, 12, 31), 0])  # Note that the defect count may be > 0, but it's less than threshold, so it's not in alarm.  If it needs to be accurate, calculate it as above.

def getAndonDistributionsByShift(AndonConfigId=None, AlarmDescription='', FirstShift=0, SecondShift=0, ThirdShift=0):
	spCall = system.db.createSProcCall('usp_GetAndonDistributionsByShift', getRTYDb())
	spCall.registerInParam(1, system.db.INTEGER, AndonConfigId)
	spCall.registerInParam(2, system.db.NVARCHAR, AlarmDescription)
	spCall.registerInParam(3, system.db.BIT, FirstShift)
	spCall.registerInParam(4, system.db.BIT, SecondShift)
	spCall.registerInParam(5, system.db.BIT, ThirdShift)
	system.db.execSProcCall(spCall)
	
	res = spCall.getResultSet()
	return res

def getAndonDistributionsByShiftGroup(AndonConfigId=None, AlarmDescription='', Shift=None, Group=''):
	'''Executes stored proc that retrieves andon distributions given a group, shift and AndonConfigId or AlarmDescription

    Args:
        AndonConfigId (int):
        AlarmDescription (str): 
        Shift: (int)
        Group: (str)
          

    Returns:
        Dataset of AndonDistribution data
    '''
	spCall = system.db.createSProcCall('usp_GetAndonDistributionsByShiftGroup', getRTYDb())
	spCall.registerInParam(1, system.db.INTEGER, AndonConfigId)
	spCall.registerInParam(2, system.db.NVARCHAR, AlarmDescription)	
	spCall.registerInParam(3, system.db.NVARCHAR, Group)
	if Shift:
		spCall.registerInParam(4, system.db.INTEGER, Shift)
	
	system.db.execSProcCall(spCall)
	
	res = spCall.getResultSet()
	return res

def addAndonDistribution(AndonId, RTYUserId, Group, Shift, CreatedBy=system.security.getUsername()):
	spCall = system.db.createSProcCall('usp_AddAndonDistribution', getRTYDb())	
	spCall.registerInParam(1, system.db.INTEGER, AndonId)
	spCall.registerInParam(2, system.db.INTEGER, RTYUserId)
	spCall.registerInParam(3, system.db.NVARCHAR, CreatedBy),
	spCall.registerInParam(4, system.db.NVARCHAR, Group),
	spCall.registerInParam(5, system.db.INTEGER, Shift)
	spCall.registerReturnParam(system.db.INTEGER)
		
	system.db.execSProcCall(spCall)
		
	return spCall.getReturnValue() or 0

def getShiftByAndon(AndonConfigId=None, AlarmDescription=''):
	spCall = system.db.createSProcCall('usp_GetShiftByAndon', getRTYDb())
	spCall.registerInParam(1, system.db.INTEGER, AndonConfigId or 0)
	spCall.registerInParam(2, system.db.NVARCHAR, AlarmDescription)
	spCall.registerReturnParam(system.db.INTEGER) 
	system.db.execSProcCall(spCall)
			
	res = spCall.getReturnValue() or None
	return res

def getShiftGroupByAndon(AndonConfigId=None, AlarmDescription=''):
	'''Executes stored proc that retrieves group & shift given AndonConfigId or AlarmDescription

    Executes upon clicking Submit on AddRTYUser Window

    Args:
        event (event): event object from button click

    Returns:
        Nothing
    '''
	spCall = system.db.createSProcCall('usp_GetShiftGroupByAndon', getRTYDb())
	spCall.registerInParam(1, system.db.INTEGER, AndonConfigId or 0)
	spCall.registerInParam(2, system.db.NVARCHAR, AlarmDescription)
	#spCall.registerReturnParam(system.db.INTEGER) 
	system.db.execSProcCall(spCall)
			
	res = spCall.getResultSet()
	return res

def	getAndonId(AlarmDescription):
	spCall = system.db.createSProcCall('usp_GetAndonId', getRTYDb())
	spCall.registerInParam(1, system.db.NVARCHAR, AlarmDescription)
	spCall.registerReturnParam(system.db.INTEGER)
	system.db.execSProcCall(spCall)
	
	res = spCall.getReturnValue() or None
	return res

def getAllRTYGroups():	
	'''Retrieve all possible Group options from the ShiftHistory table
		
		Args:
			None
	'''
	spCall = system.db.createSProcCall('usp_GetAllRTYGroups', getRTYDb())	
	system.db.execSProcCall(spCall)	
	res = spCall.getResultSet()	
	return res

def getRTYUserId(UserName = system.security.getUsername()):
	spCall = system.db.createSProcCall('usp_GetRTYUserId', getRTYDb())
	spCall.registerInParam(1, system.db.NVARCHAR, UserName) 
	spCall.registerReturnParam(system.db.INTEGER)
	system.db.execSProcCall(spCall)
		
	RTYUserId = spCall.getReturnValue() or None
	return RTYUserId
	
def getRTYUserDetails(RTYUserId=None, UserName = system.security.getUsername()):
	spCall = system.db.createSProcCall('usp_GetRTYUserDetails', getRTYDb())
	spCall.registerInParam(1, system.db.INTEGER, RTYUserId or 0)
	spCall.registerInParam(2, system.db.NVARCHAR, UserName) 

	system.db.execSProcCall(spCall)
		
	RTYUserDetails = spCall.getResultSet()
	return RTYUserDetails
	
def addRTYUser(UserName, Email, CellProviderId, CellNumber, SMSAgreementConfirmed, SMSAgreementDate):
	'''Adds a new RTYUser record to the Lodestar_RTY db
		
		Args:
			UserName, Email, CellProviderId, CellNumber, SMSAgreementConfirmed, SMSAgreementDate
	'''
	spCall = system.db.createSProcCall('usp_AddRTYUser', getRTYDb())
	spCall.registerInParam(1, system.db.NVARCHAR, UserName or system.security.getUsername())
	spCall.registerInParam(2, system.db.NVARCHAR, Email)
	spCall.registerInParam(3, system.db.INTEGER, CellProviderId)
	spCall.registerInParam(4, system.db.NVARCHAR, CellNumber)
	spCall.registerInParam(5, system.db.BIT, SMSAgreementConfirmed)
	spCall.registerInParam(6, system.db.TIMESTAMP, SMSAgreementDate)
	spCall.registerInParam(7, system.db.NVARCHAR, UserName)
	spCall.registerReturnParam(system.db.INTEGER)
	
	system.db.execSProcCall(spCall)
	
	return spCall.getReturnValue() or 0

def getAndonNotificationDetails(AlarmDescription):
	spCall = system.db.createSProcCall('usp_GetAndonNotificationDetails', getRTYDb())
	spCall.registerInParam(1, system.db.NVARCHAR, AlarmDescription)
	system.db.execSProcCall(spCall)
	
	res = spCall.getResultSet()
	
	return res

def getAndonDistributions(AndonConfigId=None, AlarmDescription=''):
	spCall = system.db.createSProcCall('usp_GetAndonDistributions', getRTYDb())
	spCall.registerInParam(1, system.db.INTEGER, AndonConfigId)
	spCall.registerInParam(2, system.db.NVARCHAR, AlarmDescription)
	system.db.execSProcCall(spCall)
	
	res = spCall.getResultSet()
	return res

def deleteAndonDistribution(AndonDistributionId, UserName=system.security.getUsername()):
	spCall = system.db.createSProcCall('usp_DeleteAndonDistribution', getRTYDb())
	spCall.registerInParam(1, system.db.INTEGER, AndonDistributionId)
	spCall.registerInParam(2, system.db.NVARCHAR, UserName)
	
	res = system.db.execSProcCall(spCall)
	return res
	
def leaveAndonDistribution(AndonId, RTYUserId, UserName=system.security.getUsername()):
	spCall = system.db.createSProcCall('usp_LeaveAndonDistribution', getRTYDb())
	spCall.registerInParam(1, system.db.INTEGER, AndonId)
	spCall.registerInParam(2, system.db.INTEGER, RTYUserId)
	spCall.registerInParam(3, system.db.NVARCHAR, UserName)
	
	res = system.db.execSProcCall(spCall)
	
	return res
	
def sendAndonNotification(tagPath, prev, cur, init, limit, timeWin):	
	if cur>=limit:
		tagSplit = tagPath.split('/')
		AlarmDescription = tagSplit[-2]
		groupShift = getShiftGroupByAndon(None, AlarmDescription)
		err=False
		
		shift = groupShift.getValueAt(0,0)
		group = groupShift.getValueAt(0,1)
		firstShift = 1 if shift==1 else 0
		secondShift = 1 if shift==2 else 0
		thirdShift = 1 if shift==3 else 0
		
		try:
			AndonDistributions = getAndonDistributionsByShiftGroup(None, AlarmDescription, shift, group or '')
			textTo = []
			emailTo = []
			textEmail = ''
			details = getAndonNotificationDetails(AlarmDescription)
			sta = AndonDistributions.getValueAt(0, "StationName")
			pyDetails = system.dataset.toPyDataSet(details)
			subject = 'RTY Andon Alarm ' + AlarmDescription +  ' triggered: Limit-' + str(limit) + ' Current-' + str(cur) + '(in a ' + str(timeWin) + ' minute period)'
			
			body='''
			<html>
			  <body>
			  	<p>Team,<br>Defect Andon alarm threshold has been met, here are the details:<br><br></p>
			  	<table border="1px solid black" border-collapse="collapse" cellspacing="5" cellpadding="5" border-spacing="5" style="margin:-15 0 0 60;padding:10;">    
			  		<caption><strong>Andon Alarm Defect Details Table</strong></caption>
			  		<thead><tr>			  					  
			    '''						
			
			colNames = details.getColumnNames()
			for col in range(len(colNames)):
				colName = str(colNames[col])
				if colName is not None and colName<>'':
					body = body + '<th padding=”10” text-align="center">' + colName + '</th>'
				
			body = body + '</tr></thead>'
			
			for row in range(0, details.getRowCount()):
				body = body + '<tr padding=”10”>'
				for col in range(len(colNames)):
					#print(details.getValueAt(row, col))
					body = body + '<td padding=”10” text-align="center">' + str(details.getValueAt(row, col)) + '</td>'
				body = body + '</tr>'
				
			body = body + '''</table><br><br>
			<b>Alarm Name: </b>%s<br>
			  <b>Defect Count Limit: </b>%s<br>
			  <b>Alarm Time Window: </b>%s<br>
			  <strong>Current Defect Count: </strong>%s<br>
			  <strong>Station: </strong>%s			  
			  </body>
			</html>
				'''
			
			body = body % (AlarmDescription, limit, timeWin, cur, sta)
			
			for row in range(AndonDistributions.getRowCount()):
				emailTo.append(AndonDistributions.getValueAt(row, 'Email'))
				if AndonDistributions.getValueAt(row, 'SMSAgreementConfirmed'):
					cellNumber = AndonDistributions.getValueAt(row, 'CellNumber')
					textEmail = cellNumber + '@' + AndonDistributions.getValueAt(row, 'EmailDomain')
					textTo.append(textEmail)
			#finalHtml = body.format
			if len(emailTo)>0:
				sendEmail(emailTo, subject, body, html=1)
			
			if len(textTo)>0:
				sendEmail(textTo, None, subject, html=0)
		except:			
			system.tag.write(tagPath, prev)
