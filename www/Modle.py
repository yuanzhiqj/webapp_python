import time, uuid

from ORM import Model, StringField, BooleanField, FloatField, TextField

class User(Model):
    __table__ = 'users'

    id = IntegetField(primary_key=True)
    name = StringField()

    user = yield from User.find('123')