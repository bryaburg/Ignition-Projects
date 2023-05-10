import json

def publish_object(obj):
	json_str = json.dumps(obj)
	system.cirruslink.engine.publish("Chariot SCADA", "ignition/test", bytearray(json_str, 'utf-8'), 2, False)

def publish_tag_change_event(event):
	path = event.getTagPath().toString()
	currentValue = str(event.getCurrentValue().getValue())
	timestamp = str(event.getCurrentValue().getTimestamp())
	
	publish_object({'path': path, 'value': currentValue, 'timestamp': timestamp})
	
def publish_tag_change(path, previous, current, initial):
	tag_change = {
		'path': str(path),
		'initial_change': str(initial),
		'previous': {
			'value': str(previous.value),
			'quality': str(previous.quality),
			'timestamp': str(previous.timestamp)
		},
		'current': {
			'value': str(current.value),
			'quality': str(current.quality),
			'timestamp': str(current.timestamp)
		}
	}
	
	publish_object(tag_change)