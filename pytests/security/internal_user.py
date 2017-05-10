import logger
from membase.api.rest_client import RestConnection
from user_base_abc import UserBase

log = logger.Logger.get_logger()

class InternalUser(UserBase):


    def __init__(self,
                 user_id=None,
                 payload=None,
                 host=None
                 ):

        self.user_id = user_id
        self.payload = payload
        self.host = host


    '''
    payload=name=<nameofuser>&roles=admin,cluster_admin&password=<password>
    if roles=<empty> user will be created with no roles
    '''
    def create_user(self):
        rest = RestConnection(self.host)
        response = rest.add_set_builtin_user(self.user_id,self.payload)
        return response

    def delete_user(self):
        response = True
        try:
            rest = RestConnection(self.host)
            response = rest.delete_builtin_user(self.user_id)
        except Exception as e:
            log.info ("Exception while deleting user. Exception is -{0}".format(e))
            response = False
        return response

    def change_password(self,user_id=None, password=None,host=None):
        if user_id:
            self.user_id = user_id
        if password:
            self.password = password
        if host:
            self.host = host

        rest = RestConnection(self.host)
        response = rest.change_password_builtin_user(self.user_id,self.password)


    def user_setup(self,user_id=None,host=None,payload=None):
        if user_id:
            self.user_id = user_id
        if host :
            self.host = host
        if payload:
            self.payload = payload
        rest = RestConnection(self.host)
        versions = rest.get_nodes_versions()
        pre_spock = False
        for version in versions:
            if "5" > version:
                pre_spock = True
        if pre_spock:
            log.info("At least one of the node in the cluster is on "
                     "pre-spock version. Not creating user since RBAC is a "
                     "spock feature.")
            return
        self.delete_user()
        self.create_user()