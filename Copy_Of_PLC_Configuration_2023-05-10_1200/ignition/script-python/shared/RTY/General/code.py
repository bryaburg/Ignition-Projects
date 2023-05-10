'''
	Created by/on:  WJF, 2020-02-25
	
	Placeholder for Generally useful scripts for the RTY project.
	
	Updated by: WJF - 2020-04-20 - Created getLodestarDbName
				WJF - 2021-05-21 - Created 
'''


from shared.Common.Util import getUser


def showSpinner(timeout=10000):
	'''<short description>
		
		<long description>
		
		Args:
			<arg name> (<arg type>): description
		
		Returns:
			<return type>: description
	'''
	try:
		window = system.nav.openWindow('Spinner', {'timeout' : timeout})
		updateSpinner(window)
		system.nav.centerWindow(window)
		return window
	except:
		print('spinner failed to open')


def updateSpinner(window, text=''):
	'''<short description>
		
		<long description>
		
		Args:
			<arg name> (<arg type>): description
		
		Returns:
			<return type>: description
	'''
	try:
		window.getRootContainer().updateText = text
	except:
		print('spinner failed to update')


def closeSpinner(window):
	'''<short description>
		
		<long description>
		
		Args:
			<arg name> (<arg type>): description
		
		Returns:
			<return type>: description
	'''
	try:
		updateSpinner(window)
		system.nav.closeWindow(window)
	except:
		windows = system.gui.findWindows('Spinner')
		for win in window:
			updateSpinner(win)
			system.nav.closeWindow(win)


def getRTYDb():
	'''
		Returns the name of the Ignition RTY database connection in Ignition.
	'''
	return system.tag.read('[Configuration]RTY/Database_Name').value


def	RTYImageDuplicationCheck(filePath):
	'''<short description>
		
		<long description>
		
		Args:
			<arg name> (<arg type>): description
		
		Returns:
			<return type>: description
	'''
	projectName = system.tag.read('[System]Client/System/ProjectName').value 
						
	res = system.util.sendRequest(project = projectName,messageHandler = "RTYImageDuplicationCheck",payload = {"filePath":filePath})
	return res


def getLodestarDb():
	'''
		Returns the name of the Ignition Lodestar R-one database connection in Ignition.
	'''
	return system.tag.read('[Configuration]Assembly/Database_Name').value


def getLodestarDbName():
	'''
		Returns the name of the Ignition Lodestar R-one database.
	'''
	return system.tag.read('[Configuration]Assembly/Database_Name').value


def insertLog(functionName, logType, log, user):
	'''
		Inserts RTY Log.
	'''
	logQuery = """
				INSERT INTO dbo.Logger(CodeCreatingLog, LogType, LogDescription, CreatedBy,
										CreatedOn)
				VALUES (?, ?, ?, ?,
						GETDATE())
				"""
	logParams = [functionName, logType, log, user]
	system.db.runPrepUpdate(logQuery, logParams, getRTYDb())
	
	
def getShift(lineCode):
	'''
		Returns current Shift number from given Whirlpool line code.
	'''
	query = """
			SELECT dbo.fn_GetCurrentShift(?)
			"""
	return system.db.runScalarPrepQuery(query, [lineCode], getRTYDb())
	
	
def getCurrentShiftStartTime(lineCode):
	'''
		Returns current Shift start time from given Whirlpool line code.
	'''
	query = """
			SELECT dbo.fn_GetCurrentShiftStartTime(?)
			"""
	return system.db.runScalarPrepQuery(query, [lineCode], getRTYDb())	


def sendEmail(emailTo, subject, body, html=0, cc=[], bcc=[]):
	'''<short description>
		
		<long description>
		
		Args:
			<arg name> (<arg type>): description
		
		Returns:
			<return type>: description
	'''
	mailServer = system.tag.read("[Configuration]Site/Mail_Lists/MailServer").value # "mailhost.whirlpool.com:25:tls"
	smtpProf = system.tag.read("[Configuration]Site/Mail_Lists/SMTP Profile").value
	myuser = system.tag.read("[Configuration]Site/Mail_Lists/MailServerUser").value
	mypass = system.tag.read("[Configuration]Site/Mail_Lists/MailServerPassword").value
	mypass = system.tag.read("[Configuration]Site/Mail_Lists/MailServerPassword").value
	fromAddr = system.tag.read("[Configuration]Site/Mail_Lists/MailSender").value
	system.net.sendEmail(smtp=mailServer, fromAddr=fromAddr,
								subject=subject, body=body, html=html, to=emailTo, username=myuser, password=mypass,
								smtpProfile = smtpProf, cc=cc, bcc=bcc)


def datesSelectedAndInRange(timeDimension, dates, range=62):
	'''Checks to see if any date in dates is before (today - range days).
		
		Args:
			timeDimension (int): Index of selected date range value.
			dates (list of java.util.date): List of dates.
			range (int):  Number of days in the past a date can be valid.
		
		Returns:
			bool: True if date is on or equal to (today - range days).
	'''
	todayMidnight = system.date.midnight(system.date.now())
	rangeDate = system.date.addDays(todayMidnight, -range)
	
	if timeDimension < 0:
		return False
	
	for date in dates: 
		if date < rangeDate:
			warnMsg = 'Must select dates within the past 62 days. To query older dates, refer to the Data Studio dashboard.'
			system.gui.warningBox(warnMsg, 'Invalid Date Selected')
			return False
	
	return True
