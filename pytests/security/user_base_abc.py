from abc import ABCMeta, abstractmethod


class UserBase():

    __metaclass__ = ABCMeta

    @abstractmethod
    def create_user(self):
        pass

    @abstractmethod
    def delete_user(self):
        pass

    @abstractmethod
    def user_setup(self):
        pass
