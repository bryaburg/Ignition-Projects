def get_code_description(code):
	if code == 200:
		return 'OK'
	elif code == 201:
		return 'CREATED'
	elif code == 400:
		return 'BAD_REQUEST'
	elif code == 401:
		return 'UNAUTHORIZED'
	elif code == 403:
		code == 'FORBIDDEN'
	elif code == 404:
		return 'NOT_FOUND'
	elif code == 404.1:
		return 'USER_NOT_FOUND'
	elif code == 500:
		return 'INTERNAL_SERVER_ERROR'

def is_authorized(headers, roles=[]):
	auth_header = shared.lodestar_core.utilities.get_object_key(headers, 'Authorization')
	if auth_header != None:
		auth_header_split = auth_header.split(' ')
		if auth_header_split[0] == 'Bearer' and len(auth_header_split) == 2:
			config = shared.lodestar_core.tags.get_udt('[default]lodestar_core/jwt_configuration')
			token = auth_header_split[1]
			
			valid_response = shared.lodestar_core.auth.verify_jwt(config, token, roles)
			if valid_response['valid'] != True:
				return False
		else:
			return False
	else:
		return False
		
	return True

def get_http_response(code, payload = None):
	response = {'code': code, 'code_description': get_code_description(code)}
	if payload != None:
		response['payload'] = payload
		
	return response