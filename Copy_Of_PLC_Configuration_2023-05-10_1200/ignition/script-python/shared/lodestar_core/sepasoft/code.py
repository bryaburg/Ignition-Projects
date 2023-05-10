def get_downtime_events(line):
	sub_lines = shared.lodestar_core.production_model.get_sublines(line)
	ln = 'Line State Value > 0 AND ('
	
	if len(sub_lines) > 0:
		for row in sub_lines:
			ln = " Line Downtime Equipment Path LIKE '*%s*' OR " % str(row['path'][24:])
		
		ln = ln[:-3] + ')'
		
		sas_name = 'LineDowntime%s' % line
		settings_list = system.mes.analysis.getMESAnalysisSettingsList()
		if sas_name not in settings_list:
			analysis_setting = system.mes.analysis.createMESAnalysisSettings(sas_name)
			datapoints = [
				"Line State Event Begin",
				"Line State Event End",
				"Line State Duration",
				"Line Downtime Equipment Path",
				"Line Downtime Original Equipment Path",
				"Line Downtime Equipment Name",
				"Line Downtime Reason",
				"Line Downtime Reason Path",
				"Equipment Note",
				"Line State Value",
				"Line Downtime State Time Stamp",
				"Line Downtime End State Time Stamp",
				"Line Downtime Reason Split",
				"Line Downtime Can Revert Split",
				"Line Downtime Event Sequence",
				"Line Downtime Occurrence Count",
				"Line State Override Type",
				"Line State Override Scope",
				"Line State Overridden"
			]
			
			analysis_setting.setDataPoints(datapoints)
			analysis_setting.addParameter('ms')
			analysis_setting.setFilterExpression(ln)
			analysis_setting.setGroupBy('Line State Event Begin,Line Downtime Equipment Name,Line Downtime Equipment Path,Line Downtime State Time Stamp')
			
			system.mes.analysis.saveMESAnalysisSettings(analysis_setting)
		else:
			analysis_setting = system.mes.analysis.getMESAnalysisSettings(sas_name)
			fil = analysis_setting.getFilterExpression()
			
			if fil != ln:
				analysis_setting.setFilterExpression(ln)
				system.mes.analysis.saveMESAnalysisSettings(analysis_setting)
		
		# TODO: Extract this out to parameters of this function
		endTime = system.date.addHours(system.date.now(), 1) # look ahead to the next hour so you are sure to get the latest data
		endHour = system.date.getHour24(endTime)
		end_date = system.date.setTime(endTime, endHour, 0, 0) # set the end time to be the top of the next hour
		start_date = system.date.addHours(endTime, -25)
		
		params = {'ms':'0'}
		out =  system.mes.analysis.executeAnalysis(start_date, end_date, sas_name, params).getDataset()	
		if out != None:
			dt = out.toIgnitionDataset()
			return shared.lodestar_core.utilities.ds2ol(dt)
		else:
			return None