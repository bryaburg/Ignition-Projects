def import_whirlpack_records(records):
	max_inserts = 1000
	records_length = len(records)
	
	iterations = records_length / max_inserts
	remaining = records_length % max_inserts
	if remaining > 0:
		iterations += 1
	
	inserted = 0
	logger = system.util.getLogger("ApiLogger")
	
	logger.info("Inserting %s Whirlpack Records" % str(records_length))
	for i in range(iterations):
		logger.info('Inserted %s records' % str(inserted))
		range_start = i * max_inserts
		range_end = ((i + 1) * max_inserts) - 1
		if range_end > records_length:
			range_end = ((i * max_inserts) + (records_length % max_inserts)) - 1
		
		logger.info("Inserting Rows: [%s : %s]" % (str(range_start), str(range_end)))
		query = ("INSERT INTO IMPRT_WhirlpackKeyPFEP ("
				"[PFEP],[Item],[Supplier Comment],[Base Container],[BC Description],[BC Returnable?],[BC How Used],[BC Material],"
				"[BC Length],[BC Width],[BC Height],[BC Diameter],[Qty per BC],[UoM],[Mixed Load],[Load Code],[BC per Layer],"
				"[Layers per UL],[Unit Load],[UL Description],[UL Returnable?],[UL How Used],[UL Material],[UL Length],[UL Width],"
				"[UL Height],[UL Diameter],[Qty BC per UL],[Item Length],[Item Width],[Item Height],[UoM1],[Item Weight],"
				"[Weight UoM],[Updated By],[UpdateTime],[Phase],[Status],[Audit Status],[Audit Comment],[Audit Date]"
			") VALUES " )
		
		data = records[range_start:range_end]
		system.tag.writeBlocking('[default]lodestar_core/test_whirlpack_query', str(data))
		row_iter = 0
		for row in data:
			if len(row.keys()) != 41:
				logger.info('Row %s does not have the right amount of keys', str(row_iter))
			query += ( "('%s','%s','%s','%s','%s','%s','%s','%s',%s,%s,%s,%s,%s,'%s','%s','%s',%s,"
				"%s,'%s','%s','%s','%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,'%s',%s,'%s','%s','%s','%s','%s','%s','%s','%s'),")% (
				row['pfep'],row['item'],row['supplier_comment'],row['base_container'],row['bc_description'],row['bc_returnable'],row['bc_how_used'],
				row['bc_material'],row['bc_length'],row['bc_width'],row['bc_height'],row['bc_diameter'],row['qty_per_bc'],row['bc_uom'],row['mixed_load'],
				row['load_code'],row['bc_per_layer'],row['layers_per_ul'],row['unit_load'],row['ul_description'],row['ul_returnable'],row['ul_how_used'],
				row['ul_material'],row['ul_length'],row['ul_width'],row['ul_height'],row['ul_diameter'],row['qty_bc_per_ul'],row['item_length'],
				row['item_width'],row['item_height'],row['ul_uom'],row['item_weight'],row['weight_uom'],row['updated_by'],row['update_time'],
				row['phase'],row['status'],row['audit_status'],row['audit_comment'],row['audit_date']
			)
			
			row_iter += 1
		
		query = query[:-1]
		try:
	 		inserted += system.db.runUpdateQuery(str(query), 'IgnitionMES_Extension_PFEP')
	 	except e:
	 		logger.info(str(e))
	
	return inserted
	
	
def import_pfep_areas(records):
	logger = system.util.getLogger("ApiLogger")
	for record in records:
		params = {
			'AREA_SHORTHAND': record['AreaShorthand'],
			'AREA_DESCRIPTION': record['AreaDescription'],
			'LINE_ID': record['LineId'],
			'LINE_AREA_SEQUENCE': record['LineAreaSequence']
		}
		
		logger.info(str(params))
		shared.lodestar_core.db.run_named_query('materials/import_area', params)
	
def import_pfep_area_locations(records):
	logger = system.util.getLogger("ApiLogger")
	for record in records:
		params = {
			'AREA_SHORTHAND': record['AreaShorthand'],
			'LOCATION_NAME': record['LocationName'],
			'LOCATION_SEQUENCE': record['LocationSequence']
		}
		
		logger.info(str(params))
		shared.lodestar_core.db.run_named_query('materials/import_area_location', params)
	
def import_pfep_families(records):
	for record in records:
		params = {
			'LOGISTICS_FAMILY': record['FamilyDescription']
		}
		shared.lodestar_core.db.run_named_query('materials/import_logistics_family', params)
	
def import_pfep_family_parts(records):
	for record in records:
		params = {
			'LOGISTICS_FAMILY': record['FamilyDescription'],
			'PART_NUMBER': record['PartNumber']
		}
		
		shared.lodestar_core.db.run_named_query('materials/import_logistics_family_part', params)
	
def import_pfep_locations(records):
	for record in records:
		params = {
			'LOCATION_NAME': record['LocationName'],
			'LOCATION_TYPE': record['LocationTypeDescription'],
			'DISPLAY_DEVICE': record['DisplayDeviceDescription']
		}
		
		shared.lodestar_core.db.run_named_query('materials/import_location', params)
	
def import_pfep_location_parts(records):
	for record in records:
		params = {
			'PART_NUMBER': record['PartNumber'],
			'LOCATION_NAME': record['LocationName'],
			'MIN_UNITS': shared.lodestar_core.utilities.get_object_key(record, 'MinUnits'),
			'MAX_UNITS': shared.lodestar_core.utilities.get_object_key(record, 'MaxUnits'),
			'REPLENISH_UNITS': shared.lodestar_core.utilities.get_object_key(record, 'ReplenishUnits'),
			'UNIT_TYPE': shared.lodestar_core.utilities.get_object_key(record, 'UnitType'),
			'REPLENISH_ROUTE': shared.lodestar_core.utilities.get_object_key(record, 'ReplenishRoute'),
			'REPLENISH_SIGNAL': shared.lodestar_core.utilities.get_object_key(record, 'ReplenishSignal'),
			'EFFECTIVE_START': shared.lodestar_core.utilities.get_object_key(record, 'EffectiveStart'),
			'EFFECTIVE_END': shared.lodestar_core.utilities.get_object_key(record, 'EffectiveEnd'),
		}
		
		shared.lodestar_core.db.run_named_query('materials/import_location_part', params)
	
def import_pfep_routes(records):
	for record in records:
		params = {
			'ROUTE_NAME': record['RouteName'],
			'ROUTE_PITCH': record['Pitch'],
			'ROUTE_PITCH_TYPE': record['PitchType'],
			'ROUTE_DELIVERY_DEVICE': record['DeliveryDevice']
		}
		
		shared.lodestar_core.db.run_named_query('materials/import_route', params)
