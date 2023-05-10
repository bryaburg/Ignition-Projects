


log = system.util.getLogger('Stratum.Helpers.Downtime')


def getClusterFromEquipment(equipment):
	log.trace('getClusterFromEquipment %r ' % (equipment))
	Type = StratumHelpers.cache.getCache()
	
	while(equipment.cluster is None and equipment.parent is not None):
		log.trace('am i an infinite loop?')
		equipment = Type['Equipment'][equipment.parent]
	
	log.trace('getClusterFromEquipment is returning ' + equipment.cluster)
	return equipment.cluster


def getDowntimeConfig(cluster, faultcode):
	log.trace('getDowntimeConfig using cluster %r and faultcode %r ' % (cluster, faultcode))
	Type = StratumHelpers.cache.getCache()
	
	downtimeConfigsByFaultcode = Type['DowntimeConfiguration']['fault_code', faultcode]
	
	for downtimeConfig in downtimeConfigsByFaultcode:
		if downtimeConfig.cluster == cluster:
			return downtimeConfig
	
	log.debug('Faultcode %r not found in cluster %r  ' % (faultcode, cluster))
	noCodeDowntimeConfigs = Type['DowntimeConfiguration']['fault_code', -9000]
		
	for downtimeConfig in noCodeDowntimeConfigs:
		if downtimeConfig.cluster == cluster:
			return downtimeConfig
	
	log.errorf('No downtime config found for cluster %s or fault code %d ', cluster, faultcode)
	raise LookupError('No downtime config found for cluster %r or fault code %r ' % 
						(cluster, faultcode))
	
	
