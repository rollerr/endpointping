import boto3

import argparse
import json
import pingparsing
import socket

from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from yaml import load, FullLoader


class ConcurrentPing():
    def __init__(self):
        self.parsed_args = parse_arguments()
        self.path = self.parsed_args.filename
        self.loaded_yaml = load_config_file(self.path)
        self.load_ipv4_endpoints = self.loaded_yaml['endpoints']['s3_ipv4']
        self.flood = self.loaded_yaml['ping_settings']['flood']
        self.icmp_count = self.loaded_yaml['ping_settings']['icmp_count']
        self.timeout = self.loaded_yaml['global_settings']['timer']

    def process_all_ping(self):
        with ThreadPoolExecutor(max_workers=8) as executor:
            return executor.map(self.ping_endpoint, self.load_ipv4_endpoints)

    def ping_endpoint(self, ip_endpoint):
        ping_parser = pingparsing.PingParsing()
        transmitter = pingparsing.PingTransmitter()
        transmitter.destination = ip_endpoint
        transmitter.count = self.icmp_count
        transmitter.ping_option = "-f" if self.flood else None
        ping_result = transmitter.ping()
        return ping_parser.parse(ping_result).as_dict()


def load_config_file(path):
    with open(path) as in_file:
        return load(in_file, Loader=FullLoader)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--filename")
    return parser.parse_args()


def parse_results(result):
    transmitted = int(result['packet_transmit'])
    received = int(result['packet_receive'])
    loss = int(result['packet_loss_rate'])
    latency = int(result['rtt_avg'])
    destination = str(result['destination'])
    metric_values = {
        'transmitted_count': transmitted,
        'received_count': received,
        'loss': loss,
        'latency': latency}
    return {destination: metric_values, 'ALL': metric_values}


def send_to_cloudwatch(transmit_cloudwatch):
    hostname = socket.gethostname().lower()
    client = boto3.client('cloudwatch', region_name='us-east-1')
    metric_data = []
    for values in transmit_cloudwatch:
        for destination, ping_metrics in values.items():
            for value in ping_metrics:
                temp_dict = {
                    'MetricName': value,
                    'Dimensions': [
                        {
                            'Name': 'destination',
                            'Value': destination
                        },
                        {
                            'Name': 'Source',
                            'Value': hostname if destination.lower() != 'all' else 'ALL'
                        }
                    ],
                    'Value': ping_metrics[value],
                }
                metric_data.append(temp_dict)
    client.put_metric_data(Namespace='IPV4', MetricData=metric_data)


def main():
    while True:
        transmit_cloudwatch = []
        ping_executor = ConcurrentPing()
        results = ping_executor.process_all_ping()
        for result in results:
            if result['packet_transmit']:
                transmit_cloudwatch.append(parse_results(result))
            else:
                print(f'warn: {result}')
        send_to_cloudwatch(transmit_cloudwatch)
        print('transmitted')
        sleep(ping_executor.timeout)


if __name__ == '__main__':
    main()
