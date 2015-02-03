import boto.ec2
from fabric.api import run, env, cd
from fabric.decorators import roles
from fabric.contrib.files import exists
from time import sleep
'''
layout:

1) always: get all hostnames with boto

2) function: boto: start instances (start crawlers)

3) function: boto: stop instances (stop crawlers)

4) function: remote: git pull origin master

5) function: remote: execute on crawlers

6) function: remote: execute on server

'''

env.hosts = [] #hosts here
instance_hash = {}

def instance_info():
    crawler_names = set([]) #crawler names here
    server_name = set(['mongo'])
    instances = {k:None for k in crawler_names | server_name}

    print instances

    #don't commit your credentials!
    ec2conn = boto.ec2.connect_to_region('', #region
        aws_access_key_id = '', #access key
        aws_secret_access_key = '' #secret key
        )

    res = ec2conn.get_all_reservations()
    for reservation in res:
        for inst in reservation.instances:
            print inst.tags['Name']
            name = inst.tags['Name']
            if name in instances:
                #instances[name]['dns_name'] = inst.dns_name
                #instances[name]['state'] = inst.state
                #instances[name]['id'] = inst.id
                instances[name] = inst

    instance_hash = instances

    env.hosts = ['ubuntu@' + inst.dns_name for name, inst in instances.items()]
    env.roledefs['crawlers'] = [inst.dns_name for name, inst in instances.items() if inst.tags['Name'] in crawler_names]
    env.roledefs['server'] = [inst.dns_name for name, inst in instances.items() if inst.tags['Name'] in server_name]
    env.user = 'ubuntu'

def start_instances():
    instance_hash = get_instance_info()

    for name, inst in instance_hash.items():
        if inst.state == 'stopped':
            print inst.start()
        else:
            print 'INSTANCE {} is in STATE {}'.format(name, inst.state)

def stop_instances():
    for name, inst in instance_hash.items():
        if inst.state == 'running':
            print inst.stop()
        else:
            print 'INSTANCE {} is in STATE {}'.format(name, inst.state)

def update_instances():
    #git update code on all instances
    with cd('/home/ubuntu/aws_crawler'):
        run('git pull origin master')

def clone_to_instances():
    with cd('/home/ubuntu'):
        if not exists('aws_crawler'):
            run('git clone https://bitbucket.org/georgeberry/aws_crawler')

def clone_test():
    with cd('/home/ubuntu'):
        run('git clone https://bitbucket.org/georgeberry/aws_crawler')

@roles('crawlers')
def start_script():
    env.user = 'ubuntu'
    with cd('/home/ubuntu/aws_crawler'):
        run('if ! screen -list | grep -q "crawler"; then ./start_screen.sh; sleep 3; fi')