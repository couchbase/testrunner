from abc import ABCMeta, abstractmethod


class UserBase(metaclass=ABCMeta):

    @abstractmethod
    def create_user(self):
        pass

    @abstractmethod
    def delete_user(self):
        pass

    @abstractmethod
    def user_setup(self):
        pass
