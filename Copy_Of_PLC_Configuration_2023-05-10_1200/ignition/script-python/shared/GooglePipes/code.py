'''
	GooglePipes - Used to export Lodestar database information to a Google Cloud Platform - BigQuery - dataStudioTest.
	
	Written by:	Bryan Mohr 		- 1250 BC
	Updated by:	Joe (Polaris)	- 2021-01-08 - Added Docstrings
								- 2021-01-11 - Moved re Global variables inside to_snake_case
								- 2021-01-12 - Rewrote/worked print statements into Log entries.
											 - Commented out FilenameFormatter.  It's a duplicate of filename_formatter.
								- 2021-01-13 - Attached functions to GooglePipes/FireExtract and GooglePipes/FireLoader tags.
								- 2021-01-14 - Tested GooglePipes/FireExtract and GooglePipes/FireLoader tags.
											 - Only extracting CAL and Repair.  Others require line.  Could fake it?
											 - Many docstrings still need to be improved/written.
								- 2021-02-24 - Added get_rty, bq_RTY_extract, and bq_RTY_BQ_RTY_Upload.
								- 2021-02-25 - Rewrote filename_formatter to be more readable.
								- 2021-03-11 - Retired get_csv_files.  Replaced with newest_csv_file.
											 - Updated bq_upload to comply with this change.
				Bryan Mohr		- 2021-04-20 - Added new call to create dataset of all KPI data that currently exists
								- 2021-04-26 - Updated a check to to see if the file exists before trying to access.
								- 2021-04-27 - Added two functions to seek what lines are configured.
				Joe (Polaris)	- 2021-05-07 - Added get_historical_kpi
											 - Changed bq_kpi_extract to combine get_all_line_kpi and get_historical_kpi
				Joe (Polaris)	- 2021-05-17 - get_line_kpi converts None to float(0.0) before row creation.
								- 2021-05-19 - bq_kpi_extract can now select if current information is extracted
				Bryan Mohr
								- 2022-09-20 - Adding this as a refrence that this file has the 'new' versions
											 - of the exporters. They were updated to remove the 'default' tag provider.
'''


##TODO:  Find the actual external functions we're using and replace these *'s with actual names.
from shared.Common.Db import getSPResults
from shared.Lodestar.DecoupleHelpers import *
from shared.Lodestar.v1.Assembly import *
from subprocess import call, Popen, PIPE
from java.util import Date

import os
import re


##TODO: Change the project name direction for named query exec to read from the project tag.
##TODO: It is irrelevant after the project name change, but is a best practice.


def to_snake_case(str):
	'''
		Places a '_' before each non-first capital letter, and lowercases all capital letters.
		ex. 'thisIsCamelCase' => 'this_is_camel_case'
	'''
	first_re = re.compile('(.)([A-Z][a-z]+)')
	s1 = first_re.sub(r'\1_\2', str)
	
	all_re = re.compile('([a-z0-9])([A-Z])')
	return all_re.sub(r'\1_\2', s1).lower()

	
def ds2sc(dataset):
	'''
		The incoming dataset's headers are replaced by equivalent headers in snake_case.
	'''
	pyds = system.dataset.toPyDataSet(dataset)
	oldHeaders = pyds.getColumnNames()
	newHeaders = []
	data = []
	
	for header in oldHeaders: 	
		newHeaders.append(to_snake_case(header))
		
	for row in pyds:
		row_data = []
		for header in oldHeaders:
			row_data.append(row[header])
			
		data.append(row_data)
		
	return system.dataset.toDataSet(newHeaders, data)


def save_csv(filepath, dataset):
	'''
		If the file already exists, append only the data in the dataset to the file.
		Otherwise, create the file and include the headers.
	'''
	if system.file.fileExists(filepath):
		data = system.dataset.toCSV(dataset, showHeaders=False)
		system.file.writeFile(filepath, data, True)
	else:
		data = system.dataset.toCSV(dataset, showHeaders=True)
		system.file.writeFile(filepath, data, False)


#def get_csv_files(path):
#	'''
#		Path MUST be a folder path that exists in the OS, not a file path.
#		Return a list of strings that are file paths of CSVs in that folder (non-recursive).
#		
#		Depricated.  Replaced by newest_csv_filepath
#	'''
#	log = system.util.getLogger('GooglePipes.get_csv_files')
#	log.trace('path = ' + str(path))
#	
#	files = [x for x in os.listdir(path) if x.endswith(".csv")]
#	log.trace('path = ' + str(files))
#	
#	fullpaths = []
#	for filename in files:
#		fullpath = os.path.join(path, filename)
#		fullpaths.append(fullpath)
#		
#	return fullpaths


def newest_csv_filepath(path):
	'''
		Path MUST be a folder path that exists in the OS, not a file path.
		Return the file path of the newest CSV in that folder (non-recursive).
		Delete all other .csv files in the folder.
	'''

	log = system.util.getLogger('GooglePipes.newest_csv_filepath')
	log.trace('path = ' + str(path))
	
	
	files = [x for x in os.listdir(path) if x.endswith(".csv")]
	log.trace('path = ' + str(files))
	
	if files:
		#creating a list of tuples of (UnixTimeStamp, filePath)
		folderData = []
		for filename in files:
			filePath = os.path.join(path, filename)
			fileData = (os.path.getmtime(filePath), filePath)
			folderData.append(fileData)
		
		#sorts oldest to newest on the UnixTime
		sortedFolderData = sorted(folderData, key=lambda x: (x))
		
		#remove the newest fileData tuple from the list
		newestFilePath = sortedFolderData.pop()
		
		#delete files in the rest of the list
		for csv in sortedFolderData:
			os.remove(csv[1])
		
		return newestFilePath[1]
		
	else:
		return None


def filename_formatter(start, end):
	'''Filenames are the time they are created.
	'''
	now = system.date.now()
	return system.date.format(now, 'DHHmm') + '.csv'
	
#	'''	Takes java dates and returns a string in the form 
#		'startYearDayOfYearHourOfDay-endYearDayOfYearHourOfDay.csv'
#	'''
#	startString = system.date.format(start, 'YYYYDHH')
#	endString = system.date.format(end, 'YYYYDHH')
#	return startString + '-' + endString + '.csv'

	#return "%s%s%s-%s%s%s.csv" % (system.date.getYear(start), system.date.getDayOfYear(start), system.date.getHour24(start), system.date.getYear(end), system.date.getDayOfYear(end), system.date.getHour24(end))


#def exec_script(script_path, args):
#	'''
#		**NOT USED IN THIS LIBRARY**
#		Executes given script in a shell subprocess.
#		script_path is a string.  args must be iterable.
#	'''
#	full_script = script_path + " " + " ".join(args)
#	return Popen(full_script, shell=True, stderr=PIPE, stdout=PIPE).communicate()


def get_all_line_kpi():
	'''
	TODO: Understand why function overloading isn't a thing in python, I mean operator overlaod can get pretty hairy. I always enjoyed me a clean function overload.
	I wouldn't have to write this function essentialy twice and call the child function. Though at the same time, in a functional programming world
	"Doing one thing well" is a good rule to live by, but so is DRY. The real issue here is that in nineteen ninety eight the undertaker threw mankind 
	off hеll in a cell, and plummeted sixteen feet through an announcer’s table.
	
	TODO: Need a standard way to extract the currently configured lines.	
	
	Returns:
		A list of datasets with the current KPI values for each configured line	
	'''
	
	#Get list of all configured lines
	target_site = system.tag.read('[default]Site\Full Site').value
	print target_site
	site_path = get_configured_site(target_site)
	print site_path
	configured_lines = get_configured_lines(site_path,'Assembly')

	#Configure the return object/dataset thingy
	nows_list = []
	
	print configured_lines
	for line in configured_lines:
		#Call get_line_kpi(line)
		line_kpi = get_line_kpi(line)
		
		#Extract bits from the existing list to send to the shift start function
		shift = line_kpi[2]
		line = line_kpi[1]
		business_day = line_kpi[3]
		
		#get shift start
		shift_start = get_shift_start(line, system.date.now(), shift)
		if not shift_start:
			return None
	
		#add new data to list
		line_kpi.append(shift_start)

		#Add data to list
		nows_list.append(line_kpi)

	#Return dataset with all current 'nows'
	return nows_list
	

def get_line_kpi(line):
	'''---Write summary here--
	
		---Write longer explaination here---
		
		Args:
			line (?): Stuff
			
		Returns:
			(list): Contains an entire row of BigQuery KPI data.
	'''
	log = system.util.getLogger('GooglePipes.get_line_kpi')
	data = CurrentStatus(line) #shared.Lodestar.v1.Assembly.CurrentStatus(line)
	plant = system.tag.read('[default]Site/Site').value
	businessDay = system.date.now()
	shiftMode = '3'
	# get shift mode defaulted to 3  , 3 = 3rd shift starts the bussines day and 1 = 1st Shift Starts the bussines day
	try:
		#result = system.db.runNamedQuery(project_name, 'GetDayShiftMode', {})
		call = system.db.createSProcCall("GetSysParamBusinessDayShiftMode", "IgnitionMES_Extension")
		system.db.execSProcCall(call)
		result = call.getResultSet()
		if result.rowCount == 1:
			shiftMode = result.getValueAt(0,0)
	except Exception, err:
		log.error('StoredProcedure GetSysParamBusinessDayShiftMode in Lodestar_R1 failed:\n' + str(err))

	# if ShiftMode == 1 then the bussines day that starts the shift is the one to take as reference for the KPIs 
	if shiftMode == 1:
		try:
			#result = system.db.runNamedQuery(project_name, 'GetBusinessDay', {'line': line, 'shift': data['schedule']['PreviousShift']})
			call = system.db.createSProcCall("GetProdParamBusinessDay", "IgnitionMES_Extension")
			call.registerInParam('line', system.db.NVARCHAR, line)
			call.registerInParam('shift', system.db.INTEGER, data['schedule']['PreviousShift'])
			
			system.db.execSProcCall(call)
			result = call.getResultSet()
			if result.rowCount == 1:
				businessDay = result.getValueAt(0,0)
		except Exception, err:
			log.error('StoredProcedure GetProdParamBusinessDay in Lodestar_R1 failed:\n' + str(err))
	
	#data null value check
	for item in data:
		log.trace('data( ' + item + ' )')
		for component in data[item]:
			log.trace('item( '  + component + ' ) = ' + str(data[item][component]))
			if data[item][component] == None:
				data[item][component] = 0.0
				log.trace('item( '  + component + ' ) = ' + str(data[item][component]))
	
	headers = ['plant', 'line', 'shift', 'business_day', 'ay', 'ay_goal', 'units_produced', 'scheduled_units', 'theoretical_units', 'downtime', 'repairs', 'cals']
	row = [
		plant,
		line,
		int(data['schedule']['PreviousShift']),
		system.date.format(businessDay, 'YYYY-MM-dd'),
		float(data['schedule']['AY_Percent']),
		float(data['dashboard']['TargetAY']),
		int(data['schedule']['Units_Produced']),
		int(data['schedule']['Scheduled_Units']),
		int(data['schedule']['Theoretical_Units']),
		float(data['live']['Unplanned Downtime']),
		int(data['dashboard']['Repair']),
		int(data['dashboard']['CAL'])
	]
	
	return row


def get_historical_kpi(plant, start_date):
	'''Gets all line's ended shift's kpi data for plant from date to today.
	
		Args:
			plant (str):
			start_date (str, java.util.Date): 
		
		Returns
			dataset: plant, line, shift, business_day, ay, ay_goal, units_produced, scheduled_units, theoretical_units, downtime, repairs, cals
	
	'''
	logger = system.util.getLogger("GooglePipes.get_historical_kpi")
	
	call = system.db.createSProcCall("BQEGetKPI", "IgnitionMES_Extension")
	
	call.registerInParam('plant', system.db.NVARCHAR, plant)
	call.registerInParam('startDate', system.db.DATE, start_date)
	
	try:  
		system.db.execSProcCall(call)
		return call.getResultSet()
	except Exception, error:
		logger.error("Can't database right now, try again later.  " + str(error))
		return None

	
def get_line_downtime(line, start, end):
	'''
		Needs to be written.  [Summary here.]
		Used by bq_downtime_extract.
	'''
	pass

	
def get_cals(start, end):
	'''
		Called from bq_cal_extract.
	'''
	# Need to adjust this to provide project name when it goes on to the gateway
				 
	data = system.db.runNamedQuery('Lodestar_R1', 'QualityDataExport_CALs', {'StartDate': start, 'EndDate': end})
	return ds2sc(data)

	
def get_repairs(start, end):
	'''
		Called from bq_repair_extract.
	'''
	# Need to adjust this to provide project name when it goes on to the gateway
	data = system.db.runNamedQuery('Lodestar_R1', 'QualityDataExport_Repairs', {'StartDate': start, 'EndDate': end})
	return ds2sc(data)
	
	
def get_rty(start, end):
	'''
		Called from bq_RTY_extract.
	'''
	# Need to adjust this to provide project name when it goes on to the gateway
	rtyDatabaseConnection = system.tag.read('Site/Configuration/RTYDatabase').value
	storedProc = system.db.createSProcCall('usp_GoogleExportGet', rtyDatabaseConnection)
	inParameters = {'plant': system.tag.read('Site/Site').value,
					'startTime': start, 
					'endTime': end}
	data = shared.Common.Db.getSPResults(storedProc, inParameters, {}, {})
	return ds2sc(data)

	
def bq_downtime_extract(line, path, start, end):
	'''
		TODO: Explain this is used in a tag change script. [yet to be created]
	'''
	logger = system.util.getLogger('GooglePipes.BQExtract')
	logger.info("DOWNTIME EXTRACT SUCCESS")


def bq_kpi_extract(getCurrentKPI, start, end):
	'''Grabs the historical (and possibly current) kpi data and saves them to a file.
				
		Args:
			getCurrentKPI (bool): True means grab the current KPI data and
									append to historical before saving.
			start (java.util.Date): Start of the historical grab.
			end (java.util.Date): End of the historical grab.
	'''
	logger = system.util.getLogger('GooglePipes.BQExtract')
	logger.info("KPI EXTRACT STARTED")
	
	path = system.tag.read('[default]GooglePipes/BQ_KPI/FilePickupPath').value
	plant = system.tag.read('[default]Site/Site').value
	
	historicalData = get_historical_kpi(plant, start)
	
	logger.info("KPI EXTRACT HISTORICAL DATA COMPLETED")
	if getCurrentKPI:
		currentData = get_all_line_kpi()
		for lineKPI in currentData:
			historicalData = system.dataset.addRow(historicalData, 0, lineKPI)
	
	filepath = path + '\\' + filename_formatter(start, end)
	save_csv(filepath, historicalData)
	
	logger.info("KPI EXTRACT SUCCESS")

	
def get_downtime_new(start, end, plant):
	logger = system.util.getLogger('GooglePipes.BQExtract')
	logger.info("NEW DOWNTIME EXTRACT STARTED")
	
	path = system.tag.read('[Configuration]Google_Pipes/BQ_Downtime/FilePickupPath').value
	
	call = system.db.createSProcCall("[MES_Assembly].[dbo].[get_downtime_export]", "Lodestar_api_db")
	call.registerInParam("MINDATE", system.db.DATE, start)
	call.registerInParam("PLANT", system.db.NVARCHAR, plant)
	call.registerInParam("LINE_UUID", system.db.NVARCHAR, None) #I HATE EVERYTHING
	
	system.db.execSProcCall(call)
	data = call.getResultSet()
	
	filepath = path + '\\' + filename_formatter(start, end)
	save_csv(filepath, data)


def get_kpi_new(start, end, plant):
	logger = system.util.getLogger('GooglePipes.BQExtract')
	logger.info("NEW KPI EXTRACT STARTED")
	
	path = system.tag.read('[Configuration]Google_Pipes/BQ_KPI/FilePickupPath').value
	
	call = system.db.createSProcCall("[MES_Assembly].[dbo].[get_kpi_export]", "Lodestar_api_db")
	call.registerInParam("MINDATE", system.db.DATE, start)
	call.registerInParam("PLANT", system.db.NVARCHAR, plant)
	
	system.db.execSProcCall(call)
	data = call.getResultSet()
	
	filepath = path + '\\' + filename_formatter(start, end)
	save_csv(filepath, data)
	

def bq_cal_extract(start, end):
	'''
		TODO: Explain this is used in a tag change script: BQExtractTester
	'''
	logger = system.util.getLogger('GooglePipes.BQExtract')
	logger.info("CAL EXTRACT STARTED")
	
	path = system.tag.readBlocking('[Configuration]Google_Pipes/BQ_CAL/FilePickupPath')[0].value
	data = get_cals(start, end)
	filepath = path + '\\' + filename_formatter(start, end)
	save_csv(filepath, data)
	
	logger.info("CAL EXTRACT SUCCESS")

	
def bq_repair_extract(start, end):
	'''
		TODO: Explain this is used in a tag change script: BQExtractTester
	'''
	logger = system.util.getLogger('GooglePipes.BQExtract')	
	logger.info("REPAIR EXTRACT STARTED")
	
	path = system.tag.read('[Configuration]Google_Pipes/BQ_Repair/FilePickupPath').value
	data = get_repairs(start, end)
	filepath = path + '\\' + filename_formatter(start, end)
	save_csv(filepath, data)
	
	logger.info("REPAIR EXTRACT SUCCESS")
	
	
def bq_RTY_extract(start, end):
	'''
		TODO: Explain this is used in a tag change script: BQExtractTester
	'''
	logger = system.util.getLogger('GooglePipes.BQExtract')
	
	path = system.tag.read('[default]GooglePipes/BQ_RTY/FilePickupPath').value
	data = get_rty(start, end)
	filepath = path + '\\' + filename_formatter(start, end)
	save_csv(filepath, data)
	
	logger.info("RTY EXTRACT SUCCESS")


def bq_upload(job_name, csv_path, schema_path, key_path, script_path, bq_dataset, bq_table, bq_plant, bq_jobtype, bq_jobcolumn):
	'''
		TODO: Write docstring.
	'''
	logger = system.util.getLogger('GooglePipes.BQUploader')
	logger.info("Starting BQ Upload Job: " + job_name)
	
	bq_plant = "\"" + bq_plant + "\""
	
	#commented out code below was related to get_csv_files (depricated)
	#filepaths = get_csv_files(csv_path)
	
	if os.path.exists(csv_path):
	
		path = newest_csv_filepath(csv_path)
		#files_processed = 0
		#if len(filepaths) > 0:
		if path:
			#for path in filepaths:
				if system.file.fileExists(path):
					args = [script_path, key_path, schema_path, path, bq_dataset, bq_table, bq_plant, bq_jobtype, bq_jobcolumn]
					full_script =  "python " + " ".join(args)
					
					logger.trace(full_script)
					
					(data, err) = Popen(full_script, shell=True, stderr=PIPE, stdout=PIPE).communicate()
					
					if err == '':
						json_result = system.util.jsonDecode(data)
						if json_result["success"]:
							message = "BQ Upload Job: " + job_name + " File Processed: " + path 
							if "delete_job" in json_result:
								message += " Delete Job: " + str(json_result["delete_job"]) + " \n"
							if "append_job" in json_result:
								message += " Append Job: " + str(json_result["append_job"]) + " \n"
								
							logger.info(message)
						else:
							logger.error('jsonDecode error - [' + path + ']: ' + json_result["error_message"])
					else:
						logger.error('Popen error: ' + err)
						
					#files_processed = files_processed + 1
				else:
					logger.info("BQ Upload Job: " + job_name + " Error.  File missing from filePath: " + path)	
		#logger.info("BQ Upload Job: " + job_name + " Files processed: " + str(files_processed))
		else:
			logger.info("BQ Upload Job: " + job_name + " Complete.  No File to Upload.")
	else:
		logger.info("BQ Upload Job: " + job_name + " Path :" + str(csv_path) + " does not exist.")


def BQ_Downtime(start, end):
	'''
		Calls base exporter function and writes CSV to specified location for time window.
		Calls external python script and returns job details.
	'''
	log = system.util.getLogger('GooglePipes.BQ_Downtime')
	
	#Get values from gateway
	outputDirectory = system.tag.read("GooglePipes/BQ_Downtime/FilePickupPath").value
	keyFile = system.tag.read("GooglePipes/BQ_Downtime/KeyfilePath").value
	scriptPath = system.tag.read("GooglePipes/BQ_Downtime/ScriptPath").value
	
	#OutputFile
	outputFilePath = os.path.join(outputDirectory, filename_formatter(start,end))
	
	#Call Base exporter function.
	project.DwnTime.Export.pullLineDowntimeOver(start, end, outputFilePath)
	
	outputFilePath = "D:\\GooglePipes\\BQ_Downtime\\TESTFILE.CSV"
	
	'''
		Call external script to push to BigQuery
		args (also run script -h for halp)
		  	keyfile     Path to the GCP keyfile
			schemafile  Table schema file .csv -placeholder
			sourcefile  Source file from Lodestar exporter
			dataset     BigQuery destination dataset
			table       BigQuery destination table
	'''
	
	log.trace('scriptPath = ' + str(scriptPath)
			+ '\nkeyFile = ' + str(keyFile)
			+ '\noutputFilePath = ' + str(outputFilePath))
	
	proc = Popen(["‪D:\\GooglePipes\\Scripts\\filetobq.py","D:\\GooglePipes\\Keys\\Ignition-BigQueryKey.json","nofile",outputFilePath,"lodestar","ign_downtimeEvents_dev"], shell=True, stdout=PIPE)
	
	#Read values back in
	result = proc.stdout.readline()
	log.trace('result = ' + str(result))

	#Convert to JSON
	jsonResult = system.util.jsonDecode(result)
	
	if jsonResult.success:
		log.trace('jsonResult.job_id = ' + str(jsonResult.job_id)
				+ 'jsonResult.rows_loaded = ' + str(jsonResult.rows_loaded))
	else:
		log.error('jsonResult was not able to be jsonDecoded')


#TODO:  Consolidate BQ_x_Upload into a single call.  A lot of copy/pasta code here... Can pass type (KPI, CAL, etc.) as a variable.

#Upload functions runs from the gateway event timer script every 20 minutes.  Fires the upload to Google BigQuery.
def BQ_Downtime_Backshop_Upload(envs):
	log = system.util.getLogger('GooglePipes.BQ_Downtime_Backshop_Upload')
	for env in envs:
		if system.tag.exists("GooglePipes/BQ_Backshop_Downtime_%s" % env):
			log.trace("In Backshop Uploader")
			csv_file_path = system.tag.read("GooglePipes/BQ_Backshop_Downtime_%s/FilePickupPath" % env).value
			bq_schema_path = system.tag.read("GooglePipes/BQ_Backshop_Downtime_%s/SchemaPath" % env).value
			bq_key_file = system.tag.read("GooglePipes/BQ_Backshop_Downtime_%s/KeyfilePath" % env).value
			bq_upload_script = system.tag.read("GooglePipes/BQ_Backshop_Downtime_%s/ScriptPath" % env).value
			bq_dataset = system.tag.read("GooglePipes/BQ_Backshop_Downtime_%s/BigQueryDataset" % env).value
			bq_table = system.tag.read("GooglePipes/BQ_Backshop_Downtime_%s/BigqueryTable" % env).value
			bq_plant = system.tag.read("GooglePipes/BQ_Backshop_Downtime_%s/Plant" % env).value
			bq_jobtype = system.tag.read("GooglePipes/BQ_Backshop_Downtime_%s/JobType" % env).value
			bq_jobcolumn = system.tag.read("GooglePipes/BQ_Backshop_Downtime_%s/JobColumn" % env).value
			
			bq_upload('Downtime_backshop_%s Upload' % env, csv_file_path, bq_schema_path, bq_key_file, bq_upload_script, bq_dataset, bq_table, bq_plant, bq_jobtype, bq_jobcolumn)


def BQ_Downtime_Upload(envs):
	log = system.util.getLogger('GooglePipes.BQ_Downtime_Upload')
	for env in envs:
		if system.tag.exists("GooglePipes/BQ_Downtime_%s" % env):
			log.trace("In Downtime Uploader")
			csv_file_path = system.tag.read("GooglePipes/BQ_Downtime_%s/FilePickupPath" % env).value
			bq_schema_path = system.tag.read("GooglePipes/BQ_Downtime_%s/SchemaPath" % env).value
			bq_key_file = system.tag.read("GooglePipes/BQ_Downtime_%s/KeyfilePath" % env).value
			bq_upload_script = system.tag.read("GooglePipes/BQ_Downtime_%s/ScriptPath" % env).value
			bq_dataset = system.tag.read("GooglePipes/BQ_Downtime_%s/BigQueryDataset" % env).value
			bq_table = system.tag.read("GooglePipes/BQ_Downtime_%s/BigqueryTable" % env).value
			bq_plant = system.tag.read("GooglePipes/BQ_Downtime_%s/Plant" % env).value
			bq_jobtype = system.tag.read("GooglePipes/BQ_Downtime_%s/JobType" % env).value
			bq_jobcolumn = system.tag.read("GooglePipes/BQ_Downtime_%s/JobColumn" % env).value
			
			bq_upload('Downtime_%s Upload' % env, csv_file_path, bq_schema_path, bq_key_file, bq_upload_script, bq_dataset, bq_table, bq_plant, bq_jobtype, bq_jobcolumn)

				
def BQ_KPI_Upload(envs):
	log = system.util.getLogger('GooglePipes.BQ_KPI_Upload')
	for env in envs:
		if system.tag.exists("GooglePipes/BQ_KPI_%s" % env):
			log.trace("In KPI Uploader")
			csv_file_path = system.tag.read("GooglePipes/BQ_KPI_%s/FilePickupPath" % env).value
			bq_schema_path = system.tag.read("GooglePipes/BQ_KPI_%s/SchemaPath" % env).value
			bq_key_file = system.tag.read("GooglePipes/BQ_KPI_%s/KeyfilePath" % env).value
			bq_upload_script = system.tag.read("GooglePipes/BQ_KPI_%s/ScriptPath" % env).value
			bq_dataset = system.tag.read("GooglePipes/BQ_KPI_%s/BigQueryDataset" % env).value
			bq_table = system.tag.read("GooglePipes/BQ_KPI_%s/BigqueryTable" % env).value
			bq_plant = system.tag.read("GooglePipes/BQ_KPI_%s/Plant" % env).value
			bq_jobtype = system.tag.read("GooglePipes/BQ_KPI_%s/JobType" % env).value
			bq_jobcolumn = system.tag.read("GooglePipes/BQ_KPI_%s/JobColumn" % env).value
			log.trace(env + " " + str(bq_plant))
			bq_upload('KPI_%s Upload' % env, csv_file_path, bq_schema_path, bq_key_file, bq_upload_script, bq_dataset, bq_table, bq_plant, bq_jobtype, bq_jobcolumn)

		
def BQ_CAL_Upload(envs):
	log = system.util.getLogger('GooglePipes.BQ_CAL_Upload')
	for env in envs:
		if system.tag.exists("GooglePipes/BQ_CAL_%s" % env):
			log.trace("In CAL Uploader")
			csv_file_path = system.tag.read("GooglePipes/BQ_CAL_%s/FilePickupPath" % env).value
			bq_schema_path = system.tag.read("GooglePipes/BQ_CAL_%s/SchemaPath" % env).value
			bq_key_file = system.tag.read("GooglePipes/BQ_CAL_%s/KeyfilePath" % env).value
			bq_upload_script = system.tag.read("GooglePipes/BQ_CAL_%s/ScriptPath" % env).value		
			bq_dataset = system.tag.read("GooglePipes/BQ_CAL_%s/BigQueryDataset" % env).value
			bq_table = system.tag.read("GooglePipes/BQ_CAL_%s/BigqueryTable" % env).value
			bq_plant = system.tag.read("GooglePipes/BQ_CAL_%s/Plant" % env).value
			bq_jobtype = system.tag.read("GooglePipes/BQ_CAL_%s/JobType" % env).value
			bq_jobcolumn = system.tag.read("GooglePipes/BQ_CAL_%s/JobColumn" % env).value
			
			
			
			bq_upload('CAL_%s Upload' % env, csv_file_path, bq_schema_path, bq_key_file, bq_upload_script, bq_dataset, bq_table, bq_plant, bq_jobtype, bq_jobcolumn)

	
def BQ_Repair_Upload(envs):
	log = system.util.getLogger('GooglePipes.BQ_Repair_Upload')
	for env in envs:
		if system.tag.exists("GooglePipes/BQ_Repair_%s" % env):
			log.trace("In Repair Uploader")
			csv_file_path = system.tag.read("GooglePipes/BQ_Repair_%s/FilePickupPath" % env).value
			bq_schema_path = system.tag.read("GooglePipes/BQ_Repair_%s/SchemaPath" % env).value
			bq_key_file = system.tag.read("GooglePipes/BQ_Repair_%s/KeyfilePath" % env).value
			bq_upload_script = system.tag.read("GooglePipes/BQ_Repair_%s/ScriptPath" % env).value
			bq_dataset = system.tag.read("GooglePipes/BQ_Repair_%s/BigQueryDataset" % env).value
			bq_table = system.tag.read("GooglePipes/BQ_Repair_%s/BigqueryTable" % env).value
			bq_plant = system.tag.read("GooglePipes/BQ_Repair_%s/Plant" % env).value
			bq_jobtype = system.tag.read("GooglePipes/BQ_Repair_%s/JobType" % env).value
			bq_jobcolumn = system.tag.read("GooglePipes/BQ_Repair_%s/JobColumn" % env).value
			
			bq_upload('Repair_%s Upload' % env, csv_file_path, bq_schema_path, bq_key_file, bq_upload_script, bq_dataset, bq_table, bq_plant, bq_jobtype, bq_jobcolumn)


def BQ_RTY_Upload(envs):
	log = system.util.getLogger('GooglePipes.BQ_RTY_Upload')
	for env in envs:
		if system.tag.exists("GooglePipes/BQ_RTY_%s" % env):
			log.trace("In RTY Uploader")
			csv_file_path = system.tag.read("GooglePipes/BQ_RTY_%s/FilePickupPath" % env).value
			bq_schema_path = system.tag.read("GooglePipes/BQ_RTY_%s/SchemaPath" % env).value
			bq_key_file = system.tag.read("GooglePipes/BQ_RTY_%s/KeyfilePath" % env).value
			bq_upload_script = system.tag.read("GooglePipes/BQ_RTY_%s/ScriptPath" % env).value
			bq_dataset = system.tag.read("GooglePipes/BQ_RTY_%s/BigQueryDataset" % env).value
			bq_table = system.tag.read("GooglePipes/BQ_RTY_%s/BigqueryTable" % env).value
			bq_plant = system.tag.read("GooglePipes/BQ_RTY_%s/Plant" % env).value
			bq_jobtype = system.tag.read("GooglePipes/BQ_RTY_%s/JobType" % env).value
			bq_jobcolumn = system.tag.read("GooglePipes/BQ_RTY_%s/JobColumn" % env).value
			
			bq_upload('RTY_%s Upload' % env, csv_file_path, bq_schema_path, bq_key_file, bq_upload_script, bq_dataset, bq_table, bq_plant, bq_jobtype, bq_jobcolumn)


def get_configured_site(target_site):
	'''Does stuff
		Args:
			target_site (str): String value of the 'Site\Full Site'. Note: It was left this way to handle multiple sites in a single gateway
		Returns:
			str: String path of the MES Eqipment pathfor the site you are seeking the configuration of. Returns None if nothing is matched
	
	'''
	facility_string = 'Whirlpool MES'
	
	#browse the 'facility'
	obj = system.mes.loadMESObjectByEquipmentPath(facility_string)
	
	#get list of sites
	site_list = obj.getChildCollection().getList()
	
	
	if not site_list:
		print "No sites not found in tag tree"
		return None
	
	
	for site in site_list:
	
		#stringallthethingsbecause valuetypes
		site_string = str(site)
		
		#check if the site string is the same as the target
		if site_string == target_site:
			return '\\'.join([facility_string, site_string])
	
	print "Target site not found"
	return None
			
def get_configured_lines(site_path, search_area):
	'''
		Args:
			site_path (str): Equipment path the fricken \ one not the fricken / one of the site you are seeking the configuration of.
			area (str): The area where where you are seeking configured lines
		Returns:
			str: Array of configured lines in the MES model. Returns None if no lines found.
	'''

	obj = system.mes.loadMESObjectByEquipmentPath(site_path)
	area_list = obj.getChildCollection().getList()
	
	print site_path
	print search_area
	
	lines = []
	for area in area_list:
		area_object = system.mes.loadMESObject(area.getMESObjectUUID())
		
		
		print area_object.getName()
		print search_area
		
		if area_object.getName() == search_area:
			print 'found area'
			line_list = system.mes.loadMESObject(area_object.getUUID()).getChildCollection().getList()
			#check for no lines
			
			for line in line_list:
				#convert line to object
				#check for active
				#make list of active lines
				line_object = system.mes.loadMESObject(line.getMESObjectUUID())
				if line_object.isActive():
					lines.append(line_object.getName())
					

	return lines	

def get_shift_start(line, _date, shift):
	''' Returns the start time of the shift requested
	
		TODO: This should be abstracted into a utility call.
		TODO: Short term, send the actual error to logging because that generic stuff sucks.
		
		Args:
			line (str): Line you are checking
			date (date): date of the record
			shift (int): shift to get the start of
		Returns:
			start_time
			None if error (for now)
	'''
	logger = system.util.getLogger("GooglePipes.GetShiftStartEndTimes")
	
	call = system.db.createSProcCall("GetShiftStartEndTime", "IgnitionMES_Extension")
	
	call.registerInParam(1, system.db.NVARCHAR, line)
	call.registerInParam(2, system.db.DATE, _date)
	call.registerInParam(3, system.db.INTEGER, shift)
	
	try:  
		result =system.db.execSProcCall(call)
	except Exception, error:
		logger.error("Can't database right now, try again later" + str(error))
		return None	
	
	#holy frick the timeformatsgodwhy
	fixedDate = Date(call.getResultSet().getValueAt(0,"start_time").getTime())
	return system.date.format(fixedDate, 'YYYY-MM-dd HH:mm:ss')


#def FilenameFormatter(start, end):
#	'''
#		**DUPLICATE OF ANOTHER FUNCTION IN THIS LIBRARY**
#		Was used by BQ_Downtime.
#		Takes in the start/end time for this window and outputs a filename for that window.
#	'''
#	return "%s%s%s-%s%s%s.csv" % (system.date.getYear(start), system.date.getDayOfYear(start),system.date.getHour24(start),system.date.getYear(end), system.date.getDayOfYear(end),system.date.getHour24(end))
