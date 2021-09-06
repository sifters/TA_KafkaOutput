from time import sleep
from json import dumps
from kafka import KafkaProducer
import configparser

cfg = configparser.ConfigParser()
cfg.read('./config.ini')
bs = cfg.get('default','bootstrap_servers').split(',')
dest_topic = cfg.get('default','topic')



#splunk add-on builder helper functions
def process_event(helper, *args, **kwargs):

    setup_param_bootstrap_servers = helper.get_global_setting("bootstrap_servers").split(',')
    alert_action_param_topic = helper.get_param("kafka_topic")
    helper.log_info('setup_param_bootstrap_servers={}'.format(setup_param_bootstrap_servers))
    helper.log_info('kafka_topic={}'.format(alert_action_param_topic))

    events = helper.get_events()
    producer = KafkaProducer(bootstrap_servers = bs, value_serializer=lambda x:dumps(x).encode('utf-8'), retries=5)

    for event in events:
        producer.send(dest_topic, value=event)
        sleep(.01)

    return 0