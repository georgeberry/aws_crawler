import boto.ec2
import boto.utils


class CrawlerEnv:
    def __init__(self, test=True):
        ec2conn = boto.ec2.connect_to_region('',
            aws_access_key_id = '',
            aws_secret_access_key = ''
            )
        #gets credentials for this machine by name
        this_inst = boto.utils.get_instance_metadata()['instance-id']
        res = ec2conn.get_all_reservations()
        for reservation in res:
            for inst in reservation.instances:
                if inst.id == this_inst:
                    print inst.tags['Name']
                    self.instance_name = inst.tags['Name']
                    break

        if test:
            self.test = test
            self.confs = {
                'mongo_host':'localhost',
                'mongo_port': 27017
            }

        else:
            self.test = False

            #res = ec2conn.get_all_reservations() #gets instance reservations

            mongo_host = None

            for reservation in res:
                inst = reservation.instances[0]
                if inst.tags['Name'] == 'mongo': #picks mongo server
                    mongo_host = inst.dns_name
                    break

            self.confs = {
                'mongo_host': mongo_host,
                'mongo_port': 9889
            }

