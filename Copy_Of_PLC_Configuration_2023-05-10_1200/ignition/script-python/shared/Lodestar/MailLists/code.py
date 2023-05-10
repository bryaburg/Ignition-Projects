from shared.Common.Util import * #log
from shared.Common.Db import *


################################################################################################################
# stored procedure handlers
	

def readMailLists():
	connection = system.tag.readBlocking("[Configuration]Site/Configuration/MES_Assembly_DB")[0].value
	storedProc = system.db.createSProcCall("get_mail_list", connection)
	results = getSPResults(storedProc, {}, {}, {})
	return results
	

def refreshMailListsMasterData():
	system.tag.writeBlocking("[Configuration]Site/Mail_Lists/MailLists", readMailLists())

	
###########################################################################################################################
# Send Email


def sendEmail(selectedFeature, selectedLine, body, subject, replyTo, attachmentNames=None, attachmentData=None, html=1, onSuccess=None, onError=None, selectedShift=None):
	dataSet = system.tag.readBlocking("[Configuration]Site/Mail_Lists/MailLists")[0].value
	mailServer = system.tag.readBlocking("[Configuration]Site/Mail_Lists/Mail_Server")[0].value # "mailhost.whirlpool.com:25:tls"
	smtpProf = system.tag.readBlocking("[Configuration]Site/Mail_Lists/SMTP_Profile")[0].value
	recipients = ""
	myuser = system.tag.readBlocking("[Configuration]Site/Mail_Lists/User")[0].value
	mypass = system.tag.readBlocking("[Configuration]Site/Mail_Lists/Password")[0].value
	found = False
	replyList = [replyTo]

	# find the intended mail list	
	for row in range(dataSet.getRowCount()):
		feature = dataSet.getValueAt(row, 'FEATURE_NAME')
		line = dataSet.getValueAt(row, 'LINE_NAME') 
		enabled = dataSet.getValueAt(row, 'EMAIL_ENABLED')
		shift = dataSet.getValueAt(row, 'SHIFT')
		# only include rows that match the requested filter
		if selectedFeature == feature:
			log("debug", "MailList.sendMail", "Feature match ", feature, " ", selectedFeature)
		if selectedLine == line:
			log("debug", "MailList.sendMail",  "Lines match ", line, " ", selectedLine)
		if selectedShift == shift:
			log("debug", "MailList.sendMail",  "Shifts match ", shift, " ", selectedShift)
		if enabled:
			log("debug", "MailList.sendMail",  "Feature enabled ", enabled)
		if (selectedFeature == feature
			and ((selectedLine == line) or (not selectedLine is None and line is None))
			and ((selectedShift == shift) or (not selectedShift is None and shift is None))
			and ((enabled or enabled is None))
			):	# send line specific messages to both the line specific list and the plant-wide list
				found = True
				log("debug", "MailList.sendMail", "Found is ", found)
				log("debug", "MailList.sendMail", row, " ", line)
				recipients = dataSet.getValueAt(row, 'EMAIL_ADDRESS_LIST')
				
				# Now send the email			
				def sendEmailAndBlock():
					log("debug", "MailList.sendMail", "Inside sendEmailAndBlock")
					try:
						debugPrint("Sending email: ", selectedFeature, selectedLine, replyList)
						debugPrint("             : ", recipients, subject, body)
						log("debug", "MailList.sendMail", " Sending email")
						system.net.sendEmail(smtp=mailServer, fromAddr=replyTo, #"CLY_LODESTAR_ALERT@Whirlpool.com",
								subject=subject, body=body, html=html, to=recipients.split(','), 
								attachmentNames = attachmentNames,
								attachmentData = attachmentData,
								replyTo=replyList, username=myuser, password=mypass, 
								smtpProfile = smtpProf)
						if not onSuccess is None and callable(onSuccess):
							onSuccess()
					except Exception, err:
						if onError is None and callable(onError):
							onError(getExceptionCauseString(err))
						debugPrint(getExceptionCauseString(err))
				log("debug", "MailList.sendMail",  recipients, " about to send email")		
				system.util.invokeAsynchronous(sendEmailAndBlock)

	
	if not found:
		log('debug', 'MailList.sendMail', "Mail feature:", selectedFeature, "not found for line: ", selectedLine)
	