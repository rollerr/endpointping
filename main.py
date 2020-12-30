import boto3
import dns.resolver
import pingparsing

import argparse
import json
import socket

from concurrent.futures import ThreadPoolExecutor, as_completed
from dns.resolver import NXDOMAIN, Timeout
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
        self.load_dns_endpoints = self.loaded_yaml['endpoints']['dns']
        self.dns_hosts_query = self.loaded_yaml['dns_settings']['hosts']
        self.mapping = {'ping': {'function': self.ping_endpoint, 'endpoints': self.load_ipv4_endpoints},
                        'dns': {'function': self.dns_ping, 'endpoints': self.load_dns_endpoints}}

    def process_all_concurrent(self, command):
        command_to_run = self.mapping[command]['function']
        endpoints = self.mapping[command]['endpoints']
        with ThreadPoolExecutor(max_workers=8) as executor:
            return executor.map(command_to_run, endpoints)

    def ping_endpoint(self, ip_endpoint):
        ping_parser = pingparsing.PingParsing()
        transmitter = pingparsing.PingTransmitter()
        transmitter.destination = ip_endpoint
        transmitter.count = self.icmp_count
        transmitter.ping_option = "-f" if self.flood else None
        ping_result = transmitter.ping()
        return parse_results(ping_parser.parse(ping_result).as_dict())

    def dns_ping(self, ip_endpoint):
        resolver = dns.resolver.Resolver()
        resolver.nameservers = [ip_endpoint]
        host = self.dns_hosts_query[0]
        latency = 0
        try:
            dns_response = resolver.resolve(host).response
            latency = dns_response.time * 1000
            print(dns_response.answer)
            print(latency)
            print(f'name server: {ip_endpoint}')
            response = 1
        except(NXDOMAIN, Timeout):
            response = 0
            print(f'warn no results on dns query. host: {host}, resolver: {ip_endpoint}')
        metric_values = {
            'response': response,
            'latency': latency}
        return {ip_endpoint: metric_values, 'ALL': metric_values}


def load_config_file(path):
    with open(path) as in_file:
        return load(in_file, Loader=FullLoader)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--filename")
    return parser.parse_args()


def parse_results(result):
    try:
        transmitted = int(result['packet_transmit'])
        received = int(result['packet_receive'])
        loss = int(result['packet_loss_rate'])
        latency = int(result['rtt_avg'])
        destination = str(result['destination'])
        metric_values = {
            'transmitted_count': transmitted,
            'result': 1,
            'received_count': received,
            'loss': loss,
            'latency': latency}
    except TypeError:  # None returned
        metric_values = {'result':0}
    return {destination: metric_values, 'ALL': metric_values}


def send_to_cloudwatch(transmit_cloudwatch, service_name):
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
                            'Value': hostname
                        }
                    ],
                    'Value': ping_metrics[value],
                }
                metric_data.append(temp_dict)
    client.put_metric_data(Namespace=f'prod/ipv4/{service_name}', MetricData=metric_data)


def process_for_cloudwatch(array, service_name):
    transmit_cloudwatch = []
    for result in array:
        transmit_cloudwatch.append(result)
    send_to_cloudwatch(transmit_cloudwatch, service_name)


def main():
    while True:
        transmit_cloudwatch = []
        ping_executor = ConcurrentPing()
        dns_results = ping_executor.process_all_concurrent('dns')
        process_for_cloudwatch(dns_results, 'dns')
        ping_results = ping_executor.process_all_concurrent('ping')
        process_for_cloudwatch(ping_results, 'ping')
        print('transmitted')
        sleep(ping_executor.timeout)


if __name__ == '__main__':
    main()
