from .user_base_abc import UserBase
from remote.remote_util import RemoteMachineShellConnection

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

class LdapUser(UserBase):

    LDAP_HOST = "172.23.124.20"
    LDAP_PORT = "389"
    LDAP_DN = "ou=Users,dc=couchbase,dc=com"
    LDAP_OBJECT_CLASS = "inetOrgPerson"
    LDAP_ADMIN_USER = "cn=admin,dc=couchbase,dc=com"
    LDAP_ADMIN_PASS = "p@ssw0rd"
    LDAP_SERVER = ServerInfo(LDAP_HOST, LDAP_PORT, 'root', 'couchbase')

    def __init__(self,
                 user_name=None,
                 password=None,
                 host=None
                 ):

        self.user_name = user_name
        self.password = password
        self.host = host

    def create_user(self):
        userCreateCmmd = 'dn: cn=' + self.user_name + "," + self.LDAP_DN + "\n" \
                            "cn:" + self.user_name + "\n" \
                            "sn: " + self.user_name + "\n" \
                            'objectClass: ' + self.LDAP_OBJECT_CLASS + "\n" \
                            "userPassword :" +  self.password + "\n" \
                            "uid: " + self.user_name + "\n"
        fileName = 'name.ldif'
        # Execute ldapadd command to add users to the system
        shell = RemoteMachineShellConnection(self.LDAP_SERVER)
        o =''
        r =''
        try:
            shell.write_remote_file("/tmp", fileName, userCreateCmmd)
            command = "ldapadd " + "-f /tmp/" + fileName + " -D " + self.LDAP_ADMIN_USER + " -w " + self.LDAP_ADMIN_PASS
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


    def delete_user(self):
        if isinstance(self.user_name, list)==True and len(self.user_name)==1:
            self.user_name=self.user_name[0]
        userDeleteCmd = 'ldapdelete cn=' + self.user_name + "," + self.LDAP_DN
        # userDeleteCmd = 'ldapdelete -h ' + self.LDAP_HOST + " -p " + self.LDAP_PORT + ' cn=' + self.user_name + "," + self.LDAP_DN
        shell = RemoteMachineShellConnection(self.LDAP_SERVER)
        try:
            command = userDeleteCmd + " -D " + self.LDAP_ADMIN_USER + " -w " + self.LDAP_ADMIN_PASS
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
        finally:
            shell.disconnect()


    def user_setup(self,user_name=None,password=None,host=None):
        if user_name == None:
            user_name = self.user_name
        if password == None:
            password = self.password
        if host == None:
            host = self.host

        self.delete_user()
        self.create_user()