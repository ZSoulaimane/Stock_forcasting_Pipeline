import paramiko
from scp import SCPClient
import os

ROOT_PATH = os.getcwd()

def createSSHClient(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client


def launch(**context):
    ssh = createSSHClient("localhost", 2222, 'maria_dev', "maria_dev")
    scp = SCPClient(ssh.get_transport())
    scp.put(ROOT_PATH+ '/airflow/data/aymane.sql', "/home/maria_dev")
    
launch()