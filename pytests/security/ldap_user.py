from .user_base_abc import UserBase
from remote.remote_util import RemoteMachineShellConnection

class LdapUser(UserBase):

    LDAP_HOST = "172.23.120.205"
    LDAP_PORT = "389"
    LDAP_DN = "ou=Users,dc=couchbase,dc=com"
    LDAP_OBJECT_CLASS = "inetOrgPerson"
    LDAP_ADMIN_USER = "cn=Manager,dc=couchbase,dc=com"
    LDAP_ADMIN_PASS = "p@ssword"

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
        shell = RemoteMachineShellConnection(self.host)
        try:
            shell.write_remote_file("/tmp", fileName, userCreateCmmd)
            command = "ldapadd -h " + self.LDAP_HOST + " -p " + self.LDAP_PORT + " -f /tmp/" + fileName + " -D " + \
                      self.LDAP_ADMIN_USER + " -w " + self.LDAP_ADMIN_PASS
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
            command = "rm -rf /tmp/*.ldif"
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
        finally:
            shell.disconnect()
            return o


    def delete_user(self):
        userDeleteCmd = 'ldapdelete -h ' + self.LDAP_HOST + " -p " + self.LDAP_PORT + ' cn=' + self.user_name + "," + self.LDAP_DN
        shell = RemoteMachineShellConnection(self.host)
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