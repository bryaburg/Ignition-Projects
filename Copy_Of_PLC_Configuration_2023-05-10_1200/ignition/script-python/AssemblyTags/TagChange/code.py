
def tagChange():
	workstation_name = ''
	state = 0
	result = system.util.httpClient().get("http://localhost:81/")
	return result



from stratum.mixin import StratumMixin
from stratum.metaproperty import MetaProperty
from stratum.templatedefiner import TemplateDefiner
from stratum.template import Template
from uuid import UUID, uuid4
from shared.tools.global import ExtraGlobal
import arrow, time, heapq


def getTypeHelper():
	'''Either creates Type or grabs it from ExtraGlobal.

	Returns:
		TemplateDefiner: All the stratum things
	'''
	if (None, 'Type') not in ExtraGlobal.keys():
		from stratum.templatedefiner import TemplateDefiner
		from stratum.sync.bulk import fully_precache_instances, fully_precache_templates, fully_store_templates
		from stratum.core import UnresolvedReference
		log = system.util.getLogger('RunSimulator.ExtraGlobal')
		log.info('Creating Type and saving to ExtraGlobal.')
		Type = TemplateDefiner
		fully_precache_templates()
		fully_precache_instances()
		UnresolvedReference.resolve_all()
		ExtraGlobal.stash(Type, 'Type')
	else:
		log = system.util.getLogger('RunSimulator.ExtraGlobal')
		log.info('Retrieving Type from ExtraGlobal.')
		Type = ExtraGlobal.access('Type')
	
	return Type

class namespace_test():
	def doStuff(self):
		log = system.util.getLogger('testLogger')
		log.info('Class ran')
		
TOTAL_EVENTS_GENERATED = 0

def EventNameFunction():
	global TOTAL_EVENTS_GENERATED
	TOTAL_EVENTS_GENERATED += 1
	return 'Event %d_%07d' % (time.time(), TOTAL_EVENTS_GENERATED)

#class ProcessException2(StratumMixin):  #DowntimeEventWrapper was removed
#		    
#	__stratum__ = {
#		'name': EventNameFunction,
#	}    
#	
#	def __init__(self, location, code, time):
#		self.start = time
#		self.stop = None
#		self.location = location 
#		self.active = True
#		self.next_exception = None
#		self.code = code
#		self.downtime_config = None


class ProcessException(StratumMixin, Exception):  #DowntimeEventWrapper was removed
    
    __stratum__ = {
        'name': EventNameFunction,

        'property_map': {
            # our class             stratum template
            '_start':               'start',
            '_stop':                'stop',
            '_location':            'location',
            '_active':              'active',
            '_next_exception':      'next_exception',
            '_code':                'code',
            '_downtime_config':     'downtime_config',
        },
    }    
    
    def __init__(self, location, code, time):
        # super(ProcessException, self).__init__(location, code) # cooperate

        self._start = None
        self._stop = None
        self._active = None
        
        self._location = location
        self._code = code
        self.set_downtime_config(code, location)  #sets the _downtime_config attribute
        
        try:
            assert isinstance(self.__stratum_instance__.location, Template)
        except AssertionError as error:
            print ('Location did not get assigned')
            print (self)
            print (location, type(location))
            print (self.__stratum_instance__.location, type(self.__stratum_instance__.location))
            raise (error)

        self._next_exception = None
        
        # check if there's a pre-existing event to fallback to when this one ends
        # if self.location.exception and self.location.exception is not self:
        #     self._next_exception = self.location.exception
        # else:
        #     self._next_exception = self._check_neighbor_exceptions() # none means we're an original fault

        # start the event!
        self.active = (True, time)
        

    @property
    def start(self):
        return self._start
    
    @property
    def stop(self):
        return self._stop
    
    
    @property
    def active(self):
        return self._active
    
    @active.setter
    def active(self, new_state=(None, arrow.utcnow())): #datetime.now())):
        state, time = new_state
        
        # start the clock
        if self._active is None and state:
            self._start_event(time)
            
        # stop the clock, ending the event's duration
        else:
            assert state is False, (
                'Exceptions have a single duration. '
                'If restarted, a new event is required. '
                'Do not reuse old Exceptions.')
            self._stop_event(time)
    
    
    def _start_event(self, start=arrow.utcnow()):
        self._start = start
        self._active = True

        # TODO: put this back the way it was...
        
        try:
            print("\t (!) %r started %r @ %r (next: %r) (next next: %r)" % (type(self.__stratum_instance__).name, self.__stratum_instance__.name, self.location, self.next_exception, self.next_exception.next_exception))

        except:
            print("\t (!) %r started %r @ %r (next: %r)" % (type(self.__stratum_instance__).name, self.__stratum_instance__.name, self.location, self.next_exception))
    
    
    #TODO - joe - Stratum Python History Stream Value.py is not able to convert Arrow to Datetime.  We need fix later.  >>>>>>
    def _stop_event(self, stop=arrow.utcnow()):
        self._stop = stop.datetime  # <<<<<<
        self._active = False
        
        print("\t (X) %r stopped %r @ %r (next: %r)" % (type(self.__stratum_instance__).name, self.__stratum_instance__.name, self.location, self.next_exception))

    
    @property
    def next_exception(self):
        return self._next_exception
    
    @next_exception.setter
    def next_exception(self, new_value):
        # ---Debug logs---
        # if self._next_exception and new_value:
        #     print('\t\t', self.location,  'next_exception: ', self._next_exception.location, ' | new next_exception: ', new_value.location)
        # elif new_value:
        #     print('\t\t', self.location,  'next_exception: ', self._next_exception, ' | new next_exception: ', new_value.location)
        # else:
        #     print('\t\t', self.location,  'next_exception: ', self._next_exception, ' | new next_exception: ', new_value)
            
        self._next_exception = new_value
        
    @property
    def location(self):
        return self._location
    
    @property
    def downtime_config(self):
        return self._downtime_config
    
    def set_downtime_config(self, code, location):
        '''
            #downtime_params = (code, location)
        '''
        assert code and location, "Code %r or location %r are None, and that's fubar." % (code, location)
        cluster = StratumHelpers.Downtime.getClusterFromEquipment(location)
        self._downtime_config = StratumHelpers.Downtime.getDowntimeConfig(cluster, code)
    


class StationStop(ProcessException):
	"A station has stopped the line"
	
	def __str__(self):
		return type(self).__name__
	
	def __unicode__(self):
		return self.__str__()
	
	def __repr__(self):
		return 'StationStop[Location:%s, Active:%s, Next:%s]' % (self._location, self._active, self._next_exception)    


############################ Skyline stuff ####################################

def event_start(objective_downtime_event):
	return objective_downtime_event.entry.start

def event_stop(objective_downtime_event):
	return objective_downtime_event.entry.stop

def event_weight(objective_downtime_event):
	'''
		objective_downtime_event are of type StationStop
		Weights are 'smallest wins', like golf.
		
	'''
	return objective_downtime_event.entry.downtime_config.priority


class entry_struct(object):
	'''
		this is how we help skyline decide who wins
	'''

	def __init__(self, entry):
		self.entry = entry

	def __lt__(self, other):
		#print('Is ', self.entry.name, ' less than ', other.entry.name)
		return False

	def __gt__(self, other):
		return False #yes, this is on purpose.  we're ignoring when this happens.  see no evil.

	def __repr__(self):
		return(" struct " + str(self.entry))


class Skyline(object):
	"""
		Active heap is the color weight thing from the website. (if you understand this, then you're in the club)

		Skyline takes an overlapping set of events and outputs the non overlapping downtime based off of priority
		and then who came first.  Downtime may have the same event split into different sections that will need to
		be combined later.

		Skyline takes a set of overlapping downtimes and defines a set of critical points in the time domain where
		the active downtime event may change and iterates through them using a heap to find the next upcoming event.
		Once the event is started, it gets added by priority to the active heap so that the event with the lowest value
		comes first (lower priority number means it has a higher priority). The function will continue iterating until
		the cursor catches up and any new discrete non overlapping events are output.

		The function is designed for the data to be mutable.  When the skyline is defined the deafault is an event list of
		(start_time, end_time, priority) but it can be anything.  The implementation would just need to pass functions
		during initialization that would get the priority (weight), start (span_min_function), and end (span_max_function)
		when given a single entry of the event list.

		Notes:
		-A wrapper is used for heaps the entry given by stratum is not the same type as defined anywhere else so to
		avoid silly heap things, an event wrapper was needed to keep events consistent if they had the same critical
		points and priority.
		-All times are required to be time zone aware (default UTC)

		Usage:

		# Create the skyline generator- Done once
		sky = Skyline(
			weight_function = event_weight,
			span_min_function = event_start,
			span_max_function = event_stop,
			start_time = None
		)

		# Add the events to be skylined.
		sky.extend(event_list)

		# Run the skyline and get the outputs
		raw_segments = [downtime for downtime in sky]


		Inputs:
			weight_function: [optional] A function that will return the priority (min value is highest priority) when run
				on a single entry of the event_list.  Default is to return the third entry of a list.
			span_min_function: [optional] A function that will return the start time of a single of an entry when passed a
				single entry of the event_list.	 Default is to return the first entry of a list. The return must be time
				zone aware.
			span_max_function: [optional] A function that will return the end time of a single of an entry when passed a
				single entry of the event_list. Default is to return the second entry of a list. The return must be time
				zone aware.
			start_time: [optional] A timezone aware date time that will define the start of span to run the skyline over.
				It must be time zone aware.

			event_list: A list of entries to be skylined.  Each entry needs to work with the defined input functions when
				passed to the functions.

		Outputs:
			raw_segments: A list of non overlapping time stamps and entrys. Each entry of the list has the following format.
				[skyline_event_start_time, skyline_event_stop_time, event_list_entry]

				skyline_event_start_time: A time zone aware time stamp for when the entry started being the highest priorty.
				skyline_event_stop_time: A time zone aware time stamp for when the entry stopped  being the highest priorty.
				event_list_entry: The individual entry of event_list passed into the skyline via extend.



		^Bill wrote the pretty later.  Bill is not responsible for reality nor how close this doc string reflects said reality.

	"""
	def __init__(self,
				 weight_function  =None,
				 span_min_function=None,
				 span_max_function=None,
				 start_time       =None,
				):

		self._span_min_function = span_min_function or (lambda entry: entry[0])
		self._span_max_function = span_max_function or (lambda entry: entry[1])
		self._weight_function   = weight_function   or (lambda entry: entry[2])

		self._pending_entries = []
		self.critical_points = []

		self._unbounded_start = set()
		self._unbounded_stop = set()

		self._active_heap = []
		self.active_events = set()

		if start_time:
			self.cursor = start_time
		else:
			self.cursor = 0


	def _entry_slice(self, entry):
		return slice(self._span_min_function(entry), self._span_max_function(entry))


	def extend(self, fresh_entries):
		'''Converting a list of objects to entry_structs, in order, and adding them to _pending_entries.
		'''
		for entry in fresh_entries:
			self._pending_entries.append(entry_struct(entry))


	def append(self, new_entry):
		'''Converting a single object to entry_structs, and adding it to _pending_entries.
		'''
		self._pending_entries.append(entry_struct(new_entry))


	@property
	def active_heap(self):
		return self._active_heap

	@active_heap.setter
	def active_heap(self, new_list):
		# warning: ordering side effect on list passed in!
		# heapq.heapify(new_list)
		self._active_heap = new_list


	@property
	def _new_entries(self):
		while self._pending_entries:
			yield self._pending_entries.pop(0)


	@property
	def next_critical_point(self):

		# building/maintaining the list of critical points
		self._check_unbounded()
		self._integrate_new_entries()

		# yield entries while we can
		while self.critical_points:
			yield heapq.heappop(self.critical_points)


	@property
	def active_head(self):
		return self.active_heap[0][-1]  #returning entry (probably a StationStop), not entry_struct


	def _check_unbounded(self):
		# clean up any unbounded values that have become bounded (and thus subject to critical bounds)
		newly_bounded = set()

		# check those with unbounded START
		for entry in self._unbounded_start:
			entry_slice = self._entry_slice(entry)

			# now bounded from the start
			if entry_slice.start is not None:
				newly_bounded.add(entry)
				# add only if still going to be relevant
				# if entry_slice.start >= self.cursor:
				#	  print('NS add to critical points entry ', entry.entry.name, '.  start = ', entry_slice.start)
				#	  heapq.heappush(self.critical_points, (entry_slice.start, entry.entry.name, entry))

			# check if it's too late and now irrelevant
			elif entry_slice.stop and entry_slice.stop <= self.cursor:
				newly_bounded.add(entry)

		# remove the newly bounded entries
		if newly_bounded:
			self._unbounded_start -= newly_bounded
			newly_bounded.clear()

		# check those with unbounded STOP
		for entry in self._unbounded_stop:
			entry_slice = self._entry_slice(entry)

			# now bounded ending
			if entry_slice.stop is not None:
				newly_bounded.add(entry)
				# add only if still going to be relevant
				# if entry_slice.stop >= self.cursor:
				#	  print('NE add to critical points entry ', entry.entry.name, '.  stop	= ', entry_slice.start)
				#	  heapq.heappush(self.critical_points, (entry_slice.stop,  entry.entry.name, entry))

		# remove the newly bounded entries
		if newly_bounded:
			self._unbounded_stop -= newly_bounded
			newly_bounded.clear()


	def _integrate_new_entries(self):
		# clean up any pending entries and integrate them into the critical points
		for entry in self._new_entries:
			entry_slice = self._entry_slice(entry)

			# fully unbounded (background/baseline?)
			if entry_slice.start is None and entry_slice.stop is None:
				self._unbounded_start.add(entry) # track for checkup later
				self._unbounded_stop.add(entry) # track for checkup later
				self._add_active_entry(entry)

			# unbounded start
			elif entry_slice.start is None:
				# is there still active span?
				if entry_slice.stop >= self.cursor:
					self._unbounded_start.add(entry) # track for checkup later
					self._add_active_entry(entry)
					print('US add to critical points entry ', entry.entry.name, '.  stop  = ', entry_slice.start)
					heapq.heappush(self.critical_points, (entry_slice.stop,	 entry.entry.name, entry))

			# unbounded end
			elif entry_slice.stop is None:
				self._unbounded_stop.add(entry) # track for checkup later
				# has the span started yet?
				if entry_slice.start <= self.cursor:
					self._add_active_entry(entry)
				else:
					print('UE add to critical points entry ', entry.entry.name, '.  start = ', entry_slice.start)
					heapq.heappush(self.critical_points, (entry_slice.start,  entry.entry.name, entry))

			# normal bounded critical values
			else:
				if entry_slice.start >= self.cursor:
					print('N add to critical points entry ', entry.entry.name, '.  start = ', entry_slice.start)
					heapq.heappush(self.critical_points, (entry_slice.start, entry.entry.name, entry))
				if entry_slice.stop >= self.cursor:
					print('N add to critical points entry ', entry.entry.name, '.  stop  = ', entry_slice.start)
					heapq.heappush(self.critical_points, (entry_slice.stop, entry.entry.name, entry))

		if self.cursor == 0:
			self.cursor = self.critical_points[0][0]


	def __iter__(self):

		# every loop is ONE SEGMENT
		for current_critical_point, name, entry in self.next_critical_point:

			self._print_critical_points()

			weight = self.weight(entry)
			entry_slice = self._entry_slice(entry)

			start = self.cursor
			end   = current_critical_point

			if not start == end:
				yield start, end, self.active_head.entry if self.active_heap else None

			self.cursor = current_critical_point

			self._manage_entry(entry, entry_slice, weight)
			self._print_active_heap()


	def _manage_entry(self, entry, entry_slice=None, weight=None):
		# what to do with an entry after the entry's critical point is processed...?

		if entry_slice is None:
			entry_slice = self._entry_slice(entry)
		if weight is None:
			weight = self.weight(entry)

		# if stop is in the past
		if entry_slice.stop and entry_slice.stop < self.cursor:
			# remove the entry if it's still in active_events
			if entry.entry in self.active_events:
				self._remove_active_entry(entry)

		# add the start of the entry if it is equal to the cursor (in case it's masked when another closes later)
		elif entry_slice.start == self.cursor:
			self._add_active_entry(entry)

		# if active_events is empty then don't check the rest of the elif's... because we don't check for dupes when adding
		elif not self.active_events:
			pass

		# if stop exists and is not equal to the cursor, add entry to _active_entry
		elif entry_slice.stop and self.cursor != entry_slice.stop:
			self._add_active_entry(entry)

		# if entry weight is more skyward than the current heap's head weight, add entry to _active_entry
		elif weight < self.weight(self.active_head):
			self._add_active_entry(entry)

		# if stop is equal to cursor and in active_events, close the entry.
		elif entry_slice.stop == self.cursor:
			if entry.entry in self.active_events:
				self._remove_active_entry(entry)

			# clean out active_events that are no longer active
			while self.active_heap:
				head_entry_slice = self._entry_slice(self.active_head)

				# remove active_head from active_heap and active_events if it's no longer relevant
				if head_entry_slice.stop and head_entry_slice.stop <= self.cursor:
					self._remove_head_active_entry()
				else:
					break


	def weight(self, entry):
		return self._weight_function(entry)


	def _add_active_entry(self, entry):
		self.active_events.add(entry.entry)
		heapq.heappush(self.active_heap, (self.weight(entry), entry.entry.name, entry))


	def _remove_active_entry(self, entry):
		self.active_events.remove(entry.entry)

		temp_heap = []
		while self.active_heap:
			(temp_weight, temp_name, temp_entry) = heapq.heappop(self.active_heap)
			if temp_entry.entry != entry.entry:
				heapq.heappush(temp_heap, (temp_weight, temp_name, temp_entry))

		while temp_heap:
			heapq.heappush(self.active_heap, heapq.heappop(temp_heap))


	def _remove_head_active_entry(self):
		self.active_events.remove(heapq.heappop(self.active_heap)[-1].entry) #return the entry


	def _print_active_heap(self):
		pass #insert debug level if statement
#		  temp_heap = []

#		  print('\Print out of active heap in order:')
#		  while self.active_heap:
#			  (temp_weight, temp_name, temp_entry) = heapq.heappop(self.active_heap)
#			  print('weight: ',temp_weight,'entry: ', temp_entry)
#			  heapq.heappush(temp_heap, (temp_weight, temp_name, temp_entry))

#		  #print(temp_heap, '\\')
#		  while temp_heap:
#			  heapq.heappush(self.active_heap, heapq.heappop(temp_heap))


	def _print_critical_points(self):
		pass #insert debug level if statement
#		  temp_heap = []

#		  print('\Print out of critical Points in order:')
#		  while self.critical_points:
#			  (temp_weight, temp_name, temp_entry) = heapq.heappop(self.critical_points)
#			  print('weight: ',temp_weight,'entry: ', temp_entry)
#			  heapq.heappush(temp_heap, (temp_weight, temp_name, temp_entry))

#		  #print(temp_heap, '\\')
#		  while temp_heap:
#			  heapq.heappush(self.critical_points, heapq.heappop(temp_heap))


class ObjectiveDowntimeGenerator(object):

	# TODO - Joe - Init should take in a line (not a subjective_downtime) and grab the newest non-stopped objective_downtime for that line.
	def __init__(self, current_objective_downtime=None):
		self.current_objective_downtime = current_objective_downtime

	def generate_objective_downtime(self, start, stop, process_exception):
		if process_exception:
			if self.current_objective_downtime == None:
				self.current_objective_downtime = ObjectiveDowntime(start, process_exception)

			elif self.current_objective_downtime.process_exception != process_exception:
				self.current_objective_downtime.stop = start
				self.current_objective_downtime = ObjectiveDowntime(start, process_exception)

		elif self.current_objective_downtime:
			self.current_objective_downtime.stop = start
			self.current_objective_downtime = None
