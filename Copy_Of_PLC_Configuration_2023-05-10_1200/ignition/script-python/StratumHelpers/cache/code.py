from stratum.sync.bulk import fully_precache_instances, fully_precache_templates, fully_store_templates
from stratum.templatedefiner import TemplateDefiner
from stratum.core import UnresolvedReference
from shared.tools.global import ExtraGlobal

log = system.util.getLogger('Stratum.Cache')

def getCache():
	if (None, 'Type') not in ExtraGlobal.keys():
		log.info('Creating Type and saving to ExtraGlobal.')
		Type = fullyPrecache()
		saveObjectToCache(Type, 'Type')
	else:
		log.debug('Retrieving Type from ExtraGlobal.')
		Type = ExtraGlobal.access('Type')
	
	return Type


def getSkyline(name):
	if (None, name) not in ExtraGlobal.keys():
		log.info('Creating ' + name + ' and saving to ExtraGlobal.')
		skyline = AssemblyTags.TagChange.Skyline(
						weight_function = AssemblyTags.TagChange.event_weight,
						span_min_function = AssemblyTags.TagChange.event_start,
						span_max_function = AssemblyTags.TagChange.event_stop,
						)
		saveObjectToCache(newSkyline, name)
	else:
		log.debug('Retrieving ' + name + ' from ExtraGlobal.')
		skyline = ExtraGlobal.access(name)
	
	return skyline


def saveObjectToCache(object, name):
	log.debug('Saving/Updating ' + name + ' in ExtraGlobal.')
	ExtraGlobal.stash(object, name, lifespan=604800)


def fullyPrecache():
	log.debug('Fully Pre-cache Stratum.')
	Type = TemplateDefiner
	loadTemplates()
	loadInstances()
	UnresolvedReference.resolve_all()
	return Type


def loadTemplates():
	log.debug('Pre-cache Stratum Templates.')
	fully_precache_templates()
	
	
def loadInstances():
	log.debug('Pre-cache Stratum Instances.')
	fully_precache_instances()
	
	
def saveEntireCache():
	log.debug('Writing entire cache to database.')
	fully_store_templates(True)

