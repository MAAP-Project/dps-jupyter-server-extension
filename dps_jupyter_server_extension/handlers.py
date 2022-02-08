import json

from jupyter_server.base.handlers import APIHandler
from notebook.base.handlers import IPythonHandler
from jupyter_server.utils import url_path_join
import tornado
from maap.maap import MAAP
import functools
import os
import xml.etree.ElementTree as ET
import xmltodict
import logging

logging.basicConfig(format='%(asctime)s %(message)s')


@functools.lru_cache(maxsize=128)
def get_maap_config(host):
    path_to_json = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../', 'maap_environments.json')

    with open(path_to_json) as f:
        data = json.load(f)

    match = next((x for x in data if host in x['ade_server']), None)
    maap_config = next((x for x in data if x['default_host'] == True), None) if match is None else match

    return maap_config


def maap_api(host):
    return get_maap_config(host)['api_server']


def parse_params(args):
    params = {}
    for key, value in args.items():
        params[key] = value[0].decode("utf-8")
    return params


class DescribeCapabilitiesHandler(IPythonHandler):
    '''
    Describes server capabilities and lists the processes that are available.

    Inputs: 
        None

    Returns: 
        JSON object containing maap-py response status code and body in 
        OGC-compliant XML format.
    '''
    def get(self):
        maap = MAAP(maap_api(self.request.host))

        try:
            resp = maap.getCapabilities()
            self.finish({"status_code": resp.status_code, "response": resp.text})
        except:
            msg = "Failed to query endpoint: " + self.request.host + self.request.uri
            logging.ERROR(msg)
            self.finish({"status_code": 500, "response": msg})


class SubmitJobHandler(IPythonHandler):
    def get(self):
        maap = MAAP(maap_api(self.request.host))

        try:
            resp = maap.submitJob()
            if resp.ok:
                self.finish({"status_code": resp.status_code, "response": resp.json()})
            else:
                # Response from failed request is in XML format for this MAAP API endpoint
                self.finish({"status_code": resp.status_code, "response": resp.text})
        except:
            msg = "Failed to query endpoint: " + self.request.host + self.request.uri
            logging.ERROR(msg)
            self.finish({"status_code": 500, "response": msg})


class RevokeJobHandler(IPythonHandler):
    '''
    Revokes jobs. This will stop a job that is running/queued and move
    it to a revoked state.

    Inputs: 
        job_id: str
            unique job identifier (e.g. a9cc35d6-b36a-4c77-9b59-7a5de0c47c02)

    Returns: 
        JSON object containing maap-py response status code and body in 
        OGC-compliant XML format.
    '''
    def get(self):
        maap = MAAP(maap_api(self.request.host))
        params = parse_params(self.request.arguments)

        try:
            resp = maap.deleteJob(params["job_id"])
            if resp.ok:
                self.finish({"status_code": resp.status_code, "response": resp.json()})
            else:
                # Response from failed request is in XML format for this MAAP API endpoint
                self.finish({"status_code": resp.status_code, "response": resp.text})
        except:
            msg = "Failed to query endpoint: " + self.request.host + self.request.uri
            logging.ERROR(msg)
            self.finish({"status_code": 500, "response": msg})


class JobResultHandler(IPythonHandler):
    '''
    Returns result for a given job.

    Inputs: 
        job_id: str
            unique job identifier (e.g. a9cc35d6-b36a-4c77-9b59-7a5de0c47c02)

    Returns: 
        JSON object containing maap-py response status code and body in 
        OGC-compliant XML format.
    '''
    def get(self):
        maap = MAAP(maap_api(self.request.host))
        params = parse_params(self.request.arguments)

        try:
            resp = maap.getJobResult(params["job_id"])
            if resp.ok:
                self.finish({"status_code": resp.status_code, "response": resp.json()})
            else:
                # Response from failed request is in XML format for this MAAP API endpoint
                self.finish({"status_code": resp.status_code, "response": resp.text})
        except:
            msg = "Failed to query endpoint: " + self.request.host + self.request.uri
            logging.ERROR(msg)
            self.finish({"status_code": 500, "response": msg})


class JobMetricsHandler(IPythonHandler):
    '''
    Returns metrics for a given job.

    Inputs: 
        job_id: str
            unique job identifier (e.g. a9cc35d6-b36a-4c77-9b59-7a5de0c47c02)

    Returns: 
        JSON object containing maap-py response status code and body in 
        OGC-compliant XML format.
    '''
    def get(self):
        maap = MAAP(maap_api(self.request.host))
        params = parse_params(self.request.arguments)

        try:
            resp = maap.getJobMetrics(params["job_id"])
            if resp.ok:
                self.finish({"status_code": resp.status_code, "response": resp.json()})
            else:
                # Response from failed request is in XML format for this MAAP API endpoint
                self.finish({"status_code": resp.status_code, "response": resp.text})
        except:
            msg = "Failed to query endpoint: " + self.request.host + self.request.uri
            logging.ERROR(msg)
            self.finish({"status_code": 500, "response": msg})


class JobStatusHandler(IPythonHandler):
    '''
    Returns the status for a given job.

    Inputs: 
        job_id: str
            unique job identifier (e.g. a9cc35d6-b36a-4c77-9b59-7a5de0c47c02)

    Returns: 
        JSON object containing maap-py response status code and body in 
        OGC-compliant XML format.
    '''
    def get(self):
        maap = MAAP(maap_api(self.request.host))
        params = parse_params(self.request.arguments)

        try:
            resp = maap.getJobStatus(params["job_id"])
            if resp.ok:
                self.finish({"status_code": resp.status_code, "response": resp.json()})
            else:
                # Response from failed request is in XML format for this MAAP API endpoint
                self.finish({"status_code": resp.status_code, "response": resp.text})
        except:
            msg = "Failed to query endpoint: " + self.request.host + self.request.uri
            logging.ERROR(msg)
            self.finish({"status_code": 500, "response": msg})


class ListJobsHandler(IPythonHandler):
    '''
    Lists the jobs submitted for a given user.

    Inputs: 
        username: str
            jupyter user

    Returns: 
        JSON object containing maap-py response status code and body in 
        OGC-compliant XML format.
    '''
    def get(self):
        maap = MAAP(maap_api(self.request.host))
        params = parse_params(self.request.arguments)

        try:
            resp = maap.listJobs(params["username"])
            if resp.ok:
                self.finish({"status_code": resp.status_code, "response": resp.json()})
            else:
                # Response from failed request is in XML format for this MAAP API endpoint
                self.finish({"status_code": resp.status_code, "response": resp.text})
        except:
            msg = "Failed to query endpoint: " + self.request.host + self.request.uri
            logging.ERROR(msg)
            self.finish({"status_code": 500, "response": msg})


def setup_handlers(web_app):
    host_pattern = ".*$"

    base_url = web_app.settings["base_url"]
    web_app.add_handlers(host_pattern, [(url_path_join(base_url, "dps-jupyter-server-extension", "describeCapabilities"), DescribeCapabilitiesHandler)])
    web_app.add_handlers(host_pattern, [(url_path_join(base_url, "dps-jupyter-server-extension", "submitJob"), SubmitJobHandler)])
    web_app.add_handlers(host_pattern, [(url_path_join(base_url, "dps-jupyter-server-extension", "revokeJob"), RevokeJobHandler)])
    web_app.add_handlers(host_pattern, [(url_path_join(base_url, "dps-jupyter-server-extension", "jobResult"), JobResultHandler)])
    web_app.add_handlers(host_pattern, [(url_path_join(base_url, "dps-jupyter-server-extension", "jobMetrics"), JobMetricsHandler)])
    web_app.add_handlers(host_pattern, [(url_path_join(base_url, "dps-jupyter-server-extension", "jobStatus"), JobStatusHandler)])
    web_app.add_handlers(host_pattern, [(url_path_join(base_url, "dps-jupyter-server-extension", "listJobs"), ListJobsHandler)])