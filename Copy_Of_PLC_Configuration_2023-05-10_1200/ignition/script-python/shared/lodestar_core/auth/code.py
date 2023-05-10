import sys
import datetime
#import jwt
import uuid

#This is another comment
def is_blacklisted(config, token):
	if isinstance(token, unicode):
		token = decode_jwt(config, token)
			
	pyDataSet = system.dataset.toPyDataSet(config['blacklisted_tokens'])
	for row in pyDataSet:
		if row['jti'] == token['jti']:
			return True
		
	return False
	
def blacklist_jwt(config, token):		
	token = decode_jwt(config, token)
	if not is_blacklisted(config, token):
		config['blacklisted_tokens'] = system.dataset.addRow(config['blacklisted_tokens'], [token['jti'], token['refresh'], system.date.now()])		
		shared.lodestar_core.tags.update_udt('[default]lodestar_core/jwt_configuration', config)

def verify_jwt(config, token, roles = [], is_refresh = False):
	result = {'valid': True, 'code': 200, 'invalid_reason': ''}
	try:
		system.jwt.verify(token, config['secret_key'], config['issuer'], config['audience'])		
		decoded_token = decode_jwt(config, token)
		if decoded_token != None:
			if is_blacklisted(config, decoded_token) == True:
				raise Exception('Token has been blacklisted')
			
			if len(roles) > 0:
				for role in roles:				
					if not (role in decoded_token['roles']):
						raise Exception('Not authorized')
			elif not is_refresh:
				raise Exception('Not Authorized')																								
	except:
		e = sys.exc_info()[1]
		result['valid'] = False
		result['code'] = 401
		result['invalid_reason'] = '%s' % e
		logger = system.util.getLogger("ApiLogger")
		logger.info(str(e))
	finally:
		return result
		
def decode_jwt(config, token):		
	return system.jwt.decode(token)
	
def get_jwt(config, user):		
	now =  datetime.datetime.now()
	expires = now + datetime.timedelta(minutes=config['expires_delta'])

	payload = {
		'iss': config['issuer'],
		'sub': user['username'],
		'aud': config['audience'],
		'iat': now,
		#'nbf': now,
		'exp': expires,
		'jti': str(uuid.uuid1()),
		'name': "%s %s" % (user['first_name'], user['last_name']),
		'roles': user['roles'],
	}
	
	token = {
		'token': system.jwt.encode_hmac256(payload, config['secret_key']),
		'issued': payload['iat'],
		'expires': payload['exp'],
		'refresh_token': get_refresh_jwt(config, user)
	}
  
	return token
	
def get_refresh_jwt(config, user):
	if config == None or len(config.keys()) == 0 or user == None:
		return None
		
	now =  datetime.datetime.now()
	expires = now + datetime.timedelta(minutes=config['refresh_expires_delta'])
	payload = {
		'iss': config['issuer'],
		'sub': user['username'],
		'aud': config['audience'],
		'iat': now,
		#'nbf': now,
		'exp': expires,
		'jti': str(uuid.uuid1()),
	}
	
	return system.jwt.encode_hmac256(payload, config['secret_key'])

def get_user(source, username, auth_roles):
	if isinstance(source, unicode):
		source = str(source)
	
	user = system.user.getUser(source, username)
	if user != None:
		roles = []
		for role in user.getRoles():
			roles.append(str(role))

		return {
			'username': user.get('username'),
			'first_name': user.get('firstname'),
			'last_name': user.get('lastname'),
			'roles': get_user_roles(roles, auth_roles)
		}
	else:
		return None
		
def get_user_from_jwt(jwt_token):
	decoded = decode_jwt(jwt_token)
	user = {
		'username': decoded['sub'],
		'name': decoded['name'],
		'roles': decoded['roles']
	}
	
	return user
		
def get_user_roles(user_roles, auth_roles):
	roles = []
	for (k, v) in auth_roles.iteritems():
		role_split = v.split(';')
		for role in role_split:
			if role == 'OVERRIDE_EVERYONE':
				roles.append(k.upper())
				continue
			if role in user_roles:
				roles.append(k.upper())
				
	return roles