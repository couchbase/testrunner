from .ldap_user import LdapUser
from .internal_user import InternalUser
from remote.remote_util import RemoteMachineShellConnection
import urllib.request, urllib.parse, urllib.error
import json

class RbacBase:

    def __init__(self,
                 host=None,
                 source=None):
        self.host = host
        self.source = source

    '''
    Enable LDAP on node
    '''
    def enable_ldap(self, rest):
        content = rest.ldapRestOperationGetResponse()
        if not content['enabled']:
            api = rest.baseUrl + 'settings/saslauthdAuth'
            params = urllib.parse.urlencode({"enabled": 'true', "admins": [], "roAdmins": []})
            status, content, header = rest._http_request(api, 'POST', params)
            return json.loads(content)

    '''
         user_list = [{'id':ritam,'password':'password','name':'newname'}]
    '''
    def create_user_source(self,user_list=None,source=None,host=None):
        for user in user_list:
            userid = user['id']
            password = user['password']
            user_name = user['name']
            if source == 'ldap':
                return LdapUser(userid, password, host).user_setup()
            if source == 'builtin':
                payload = "name=" + user_name + "&roles=&password=" + password
                return InternalUser(userid, payload, host).user_setup()

    '''
    user_role_list = list of user information and role assignment
                     [{'id':ritam,'name'=ritamsharma,'roles'='cluster_admin:admin'}]
    '''
    def add_user_role(self,user_role_list,rest,source=None):
        if source:
            self.source = source
        response_return = []
        for user_role in user_role_list:
            final_roles = ""
            userid = user_role['id']
            username = user_role['name']
            user_role_param = user_role['roles'].split(":")
            if len(user_role_param) == 1:
                final_roles = user_role_param[0]
            else:
                for role in user_role_param:
                    final_roles = role + "," + final_roles
            payload="name="+username+"&roles="+final_roles
            if self.source == "ldap":
                response = rest.set_user_roles(userid, payload)
            elif self.source == 'builtin':
                if 'password' in list(user_role.keys()):
                    payload=payload+'&password='+user_role['password']
                cluster_compatibility = rest.check_cluster_compatibility("5.0")
                if cluster_compatibility is None:
                    pre_spock = True
                else:
                    pre_spock = not cluster_compatibility
                if pre_spock:
                    return None
                response = rest.add_set_builtin_user(userid, payload)
            response_return.append({'id':userid,'reponse':response})
        return response_return

    '''
    user_id_list - list of user that needs to be deleted
                    [ritam,arun]
    '''
    def remove_user_role(self,user_id_list,rest,source=None):
        response_return = []
        for user in user_id_list:
            if self.source == 'ldap':
                response = rest.delete_user_roles(user)
            else:
                cluster_compatibility = rest.check_cluster_compatibility("5.0")
                if cluster_compatibility is None:
                    pre_spock = True
                else:
                    pre_spock = not cluster_compatibility
                if pre_spock:
                    return None
                response = rest.delete_builtin_user(user)
            response_return.append({'id':user,'response':response})
        return response_return

    def check_user_permission(self, user, password, user_per_list, rest):
        response = rest.check_user_permission(user, password, user_per_list)
        print(response)
        return response





