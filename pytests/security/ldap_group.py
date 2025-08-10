from remote.remote_util import RemoteMachineShellConnection
from .ldap_user import LdapUser

class ServerInfo():

    def __init__(self,
                 ip,
                 port,
                 ssh_username,
                 ssh_password,
                 ssh_key=''):

        self.ip = ip
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.port = port
        self.ssh_key = ssh_key

class LdapGroup():
    LDAP_HOST = "172.23.124.20"
    LDAP_PORT = "389"
    LDAP_DN = "ou=Users,dc=couchbase,dc=com"
    LDAP_OBJECT_CLASS = "inetOrgPerson"
    LDAP_ADMIN_USER = "cn=admin,dc=couchbase,dc=com"
    LDAP_ADMIN_PASS = "p@ssw0rd"
    LDAP_GROUP_OBJECT_CLASS = "groupOfNames"
    LDAP_GROUP_DN = "ou=Groups,dc=couchbase,dc=com"

    def __init__(self,
                 group_name=None,
                 user_list=None,
                 host=None
                 ):

        self.host = host
        self.group_name = group_name
        self.user_list = user_list
        self.ldap_server = ServerInfo(self.LDAP_HOST, self.LDAP_PORT, 'root', 'couchbase')

    def create_group(self):
        member_list = ''
        if len(self.user_list) == 1:
            member_list = 'member: cn=' + self.user_list[0] + ',' + self.LDAP_DN
        else:
            for member in self.user_list:
                member_list = 'member: cn=' + member + ',' + self.LDAP_DN + "\n" + member_list

        userCreateCmmd = 'dn: cn=' + self.group_name + ',' + self.LDAP_GROUP_DN + "\n" + \
                         'cn: ' + self.group_name + "\n" + \
                         'objectClass: ' + self.LDAP_GROUP_OBJECT_CLASS + '\n' + \
                         member_list

        fileName = 'name.ldif'
        # Execute ldapadd command to add users to the system
        shell = RemoteMachineShellConnection(self.ldap_server)
        try:
            shell.write_remote_file("/tmp", fileName, userCreateCmmd)
            command = "ldapadd -f /tmp/" + fileName + " -D " + self.LDAP_ADMIN_USER + " -w " + self.LDAP_ADMIN_PASS
            # command = "ldapadd -h " + self.LDAP_HOST + " -p " + self.LDAP_PORT + " -f /tmp/" + fileName + " -D " + \
            #           self.LDAP_ADMIN_USER + " -w " + self.LDAP_ADMIN_PASS
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
            command = "rm -rf /tmp/*.ldif"
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
        finally:
            shell.disconnect()
            return o

    def delete_group(self):
        userDeleteCmd = 'ldapdelete cn=' + self.group_name + "," + self.LDAP_GROUP_DN
        # userDeleteCmd = 'ldapdelete -h ' + self.LDAP_HOST + " -p " + self.LDAP_PORT + ' cn=' + self.group_name + "," + self.LDAP_GROUP_DN
        shell = RemoteMachineShellConnection(self.ldap_server)
        try:
            command = userDeleteCmd + " -D " + self.LDAP_ADMIN_USER + " -w " + self.LDAP_ADMIN_PASS
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
        finally:
            shell.disconnect()

    def update_user_group(self, group_name=None, user=None, action='Add', host=None, addGrp=False, grp_name=None):
        member_list = ''
        o = ''
        if addGrp:
            member_list = 'member: cn=' + grp_name + ',' + self.LDAP_GROUP_DN + '\n' + member_list
        elif len(user) == 1:
            member_list = 'member: cn=' + user[0] + ',' + self.LDAP_DN
        else:
            for member in user:
                member_list = 'member: cn=' + member + ',' + self.LDAP_DN + "\n" + member_list

        if action == 'Add':
            command = 'changetype: modify' + '\n' + 'add: member'
        else:
            command = 'changetype: modify' + '\n' + 'delete: member'

        UserCreateCmmd = 'dn: cn=' + group_name + ',' + self.LDAP_GROUP_DN + '\n' + command + '\n' + member_list

        fileName = 'name.ldif'
        # Execute ldapadd command to add users to the system
        shell = RemoteMachineShellConnection(self.ldap_server)
        try:
            shell.write_remote_file("/tmp", fileName, str(UserCreateCmmd))
            command = "ldapadd -f /tmp/" + fileName + " -D " + self.LDAP_ADMIN_USER + " -w " + self.LDAP_ADMIN_PASS
            # command = "ldapadd -h " + self.LDAP_HOST + " -p " + self.LDAP_PORT + " -f /tmp/" + fileName + " -D " + \
            #           self.LDAP_ADMIN_USER + " -w " + self.LDAP_ADMIN_PASS
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
            command = "rm -rf /tmp/*.ldif"
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
        finally:
            shell.disconnect()
            return o

    def group_setup(self, group_name=None, user_list=None, host=None):
        if group_name is not None:
            self.group_name = group_name
        if user_list is not None:
            self.user_list = user_list
        if host is not None:
            self.host = host

        self.delete_group()
        self.create_group()