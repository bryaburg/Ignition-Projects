import java
from urllib import quote_plus,quote

def gatewayLogErrors(func):
	'''
	Utility function to make sure tag errors enter the log each time the error happens intead of just 
	the first instance of the error
	'''
	def wrapper(*args, **kwargs):
		log = system.util.getLogger('Stratum.SideCarComms')
		try:
			return func(*args, **kwargs)
		except (Exception, java.lang.Exception) as err:
			log.warn('Error on Gateway Script: %s'% str(err))
	return wrapper

def getPort():
	'''
	Helper function to get the port from the server configuration
	'''
	return system.tag.read('[Configuration]Stratum/SideCarPortNumber').value #81
	
def getSideCarPreamble():
	'''
	Helper function to get the server address from the configuration
	'''
	return system.tag.read('[Configuration]Stratum/SideCarPreamble').value % getPort()

def getTageChangeTemplateURL():
	'''
	Helper function to get the tag change end point from the config
	'''
	return system.tag.read('[Configuration]Stratum/StateChangeEndpoint').value

def getSaveDBEndpoint():
	'''
	Helper function to get the save db endpoint
	'''
	return system.tag.read('[Configuration]Stratum/SaveDBEndpoint').value
	
def getDBSaveURL():
	'''
	Helper function to get the full save db url
	'''
	return getSideCarPreamble() + getSaveDBEndpoint() #"http://localhost:81/db/all"

def getTagChangeURL():
	'''
	Helper function to get the full tag change endpoint URL
	'''
	return getSideCarPreamble() + getTageChangeTemplateURL()

def getTagChangePostURL(location,code, timestamp=None):
	'''
	Helper function to get the full tag change url for a specific location and code
	'''
	if timestamp:
		return getTagChangeURL()+'/'+quote(location+'/'+str(code)+'/'+system.date.format(timestamp, 'yyyy-MM-dd HH:mm:ss.SSSXXX'))
	else:
		return getTagChangeURL()+'/'+location+'/'+str(code)
	
def getObjectiveDowntimeStartEndpoint():
	'''
	Helper function to get the Downtime Start Endpoint
	'''
	return system.tag.read('[Configuration]Stratum/ObjectiveDowntimeStartEndpoint').value
	
def getObjectiveDowntimeStartURL():
	'''
	Helper function to get the url to trigger objective downtime creation
	'''
	return getSideCarPreamble() + getObjectiveDowntimeStartEndpoint()

def getSaveStateEndpoint():
	return system.tag.read('[Configuration]Stratum/SideCarStateSaveEndpoint').value
	
def getSaveStateURL():
	return getSideCarPreamble() + getSaveStateEndpoint()

def getRefreshStratumSideCarURL():
	'''
	Returns url that tells SideCar to refresh from db
	'''
	return getSideCarPreamble() + system.tag.read('[Configuration]Stratum/SideCarRefreshYourself').value

def getEquipmentURL(eqType, instanceName):
	'''
	Returns url that tells SideCar to refresh from db
	'''
	if instanceName:
		url = getSideCarPreamble() + "/type/%s/%s" % (eqType, instanceName)
	else:
		url = getSideCarPreamble() + "/type/%s" % eqType
		
	return url.replace(" ", "%20")

def updateEquipmentURL():
	'''
	Returns url that tells SideCar to update equipment
	'''
	return getSideCarPreamble() + "/equipment/update"

@gatewayLogErrors
def BuildObjectiveDowntimes():
	log = system.util.getLogger('Stratum.SideCarComms.ObjectiveStartCall')
	log.info('Sending Objective Start Call')
	log.info(getObjectiveDowntimeStartURL())
	result = system.net.httpClient().post(getObjectiveDowntimeStartURL())
	log.info('Code:' + str(result.statusCode) + ": " + result.text)
	return result

@gatewayLogErrors
def tagChange(location, code):
	'''
	Function to pass a tag change event to the stratum sidecar
	'''
	# log the change on debug for troubleshooting
	#TODO WRM Change to debug
	log = system.util.getLogger('Stratum.SideCarComms.TagChange')
	log.info('%s, %s'% (location, code))
	
	#result = system.net.httpClient().get("http://localhost:81/")
	result = system.net.httpClient().post(getTagChangePostURL(location, code))
	#TODO WRM Change to debug
	log.info('Code:' + str(result.statusCode) + ": " + result.text)
	
	return result
	
@gatewayLogErrors
def tagChangeWithTime(location, code, timestamp):
	'''
	Function to pass a tag change event to the stratum sidecar
	'''
	# log the change on debug for troubleshooting
	#TODO WRM Change to debug
	log = system.util.getLogger('Stratum.SideCarComms.TagChange')
	log.info('%s,%s,%s' % (location, code, timestamp))
	
	#result = system.net.httpClient().get("http://localhost:81/")
	result = system.net.httpClient().post(getTagChangePostURL(location,code,timestamp))
	#TODO WRM Change to debug
	log.info('Code:' + str(result.statusCode) + ": " + result.text)
	
	return result

@gatewayLogErrors
def saveToDB():
	log = system.util.getLogger('Stratum.SideCarComms.StratumSaveDB')
	#TODO WRM Change to debug
	log.info('Starting DB Save')
	log.info(getDBSaveURL())
	result = system.net.httpClient().post(getDBSaveURL())
	#TODO WRM Change to debug
	log.info('Code:' + str(result.statusCode) + ": " + result.text)
	
@gatewayLogErrors
def saveStates():
	log = system.util.getLogger('Stratum.SideCarComms.StratumSaveStates')
	#TODO WRM Change to debug
	log.info('Starting DB Save')
	log.info(getDBSaveURL())
	result = system.net.httpClient().post(getSaveStateURL())
	#TODO WRM Change to debug
	log.info('Code:' + str(result.statusCode) + ": " + result.text)

@gatewayLogErrors
def tellSideCarToRefresh():
	log = system.util.getLogger('Stratum.SideCarComms.StratumTellSideCarToRefresh')
	log.info('Telling SideCar to refresh')
	endpointURL = getRefreshStratumSideCarURL()
	log.info(endpointURL)
	result = system.net.httpClient().post(endpointURL)
	log.info('Code:' + str(result.statusCode) + ": " + result.text)
	return result
	
@gatewayLogErrors
def getEquipment(eqType, instanceName=""):
	log = system.util.getLogger('Stratum.SideCarComms.StratumGetEquipment')
	log.info('Getting equipment')
	endpointURL = getEquipmentURL(eqType, instanceName)
	log.info(endpointURL)
	result = system.net.httpClient().get(endpointURL)
	log.info('Code:' + str(result.statusCode) + ": " + result.text)
	try:
		return result.json
	except:
		return None

@gatewayLogErrors
def updateEquipment(body):
	log = system.util.getLogger('Stratum.SideCarComms.StratumUpdateEquipment')
	log.info('Updating equipment')
	endpointURL = updateEquipmentURL()
	log.info(endpointURL)
	result = system.net.httpClient().post(endpointURL, data=body)
	log.info('Code:' + str(result.statusCode) + ": " + result.text)
	return result.statusCode
