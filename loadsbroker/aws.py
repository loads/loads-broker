"""AWS Higher Level Abstractions

This module contains higher-level AWS abstractions to make working with
AWS instances and collections of instances easier and less error-prone.

"""
import concurrent.futures


"""An AWS instance is responsible for maintaining information about
itself and updating its state when asked to."""
class EC2Instance:
    pass

"""An AWS Collection is a group of instances for a given allocation
request"""
class EC2Collection:
    pass

"""An AWS EC2 Pool is responsible for allocating and dispersing
:ref:`EC2Instance`s and terminating idle instances."""
class EC2Pool:
    def __init__(self, access_key=None, secret_key=None, max_idle=600):
        pass

    def allocate_instances(count=1, type="m3.large", region="us-west-2"):
        pass
