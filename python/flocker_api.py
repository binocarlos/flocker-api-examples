#!/usr/bin/env python3

import http.client
import json
import os
import ssl
import tempfile

class FlockerApi(object):
    DEFAULT_PLUGIN_DIR = os.environ.get('CERT_DIR', '/etc/flocker')

    def __init__(self, api_version = 1):
        control_service = os.environ.get("CONTROL_SERVICE", "localhost")
        control_port = os.environ.get("CONTROL_PORT", 4523)

        self._api_version = api_version

        key_file = os.environ.get("KEY_FILE", "%s/plugin.key" % self.DEFAULT_PLUGIN_DIR)
        cert_file = os.environ.get("CERT_FILE", "%s/plugin.crt" % self.DEFAULT_PLUGIN_DIR)
        ca_file = os.environ.get("CA_FILE", "%s/cluster.crt" % self.DEFAULT_PLUGIN_DIR)

        # Create a certificate chain and then pass that into the SSL system.
        cert_with_chain_tempfile = tempfile.NamedTemporaryFile()

        temp_cert_with_chain_path = cert_with_chain_tempfile.name
        os.chmod(temp_cert_with_chain_path, 0o0600)

        # Write our cert and append the CA cert to build the chain
        with open(cert_file, 'rb') as cert_file_obj:
            cert_with_chain_tempfile.write(cert_file_obj.read())

        cert_with_chain_tempfile.write('\n'.encode('utf-8'))

        with open(ca_file, 'rb') as cacert_file_obj:
            cert_with_chain_tempfile.write(cacert_file_obj.read())

        # Reset file pointer for the SSL context to read it properly
        cert_with_chain_tempfile.seek(0)

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.load_cert_chain(temp_cert_with_chain_path, key_file)

        self._http_client = http.client.HTTPSConnection(control_service,
                                                        control_port,
                                                        context=ssl_context)

    # XXX: These should really be generic functions created dynamically
    def get(self, endpoint, data = None):
        return self._make_api_request('GET',
                                      "/v%s/%s" % (self._api_version, endpoint),
                                      data)

    def post(self, endpoint, data = None):
        return self._make_api_request('POST',
                                      "/v%s/%s" % (self._api_version, endpoint),
                                      data)

    def delete(self, endpoint, data = None):
        return self._make_api_request('DELETE',
                                      "/v%s/%s" % (self._api_version, endpoint),
                                      data)

    def _make_api_request(self, method, endpoint, data = None):
      # Convert data to string if it's not yet in this format
      if data and not isinstance(data, str):
          data = json.dumps(data).encode('utf-8')

      headers = {"Content-type": "application/json"}
      self._http_client.request(method, endpoint, data,
                                headers=headers)

      response = self._http_client.getresponse()

      status =  response.status
      body =  response.read()

      print('Status:', status)

      # If you want debugging
      # print('Body:', body)

      print()

      return json.loads(body.decode('utf-8'))

    # Specific API requests
    def get_version(self):
      version = self.get('version')
      return version['flocker']

    def create_volume(self, name, size_in_gb, primary_id, profile = None):
        if not isinstance(size_in_gb, int):
            print('Error! Size must be an integer!')
            exit(1)

        data = {
            'primary': primary_id,
            'maximum_size': size_in_gb << 30,
            'metadata': {
               'name': name
            }
        }

        if profile:
            data['metadata']['clusterhq:flocker:profile'] = profile

        return api.post('configuration/datasets', data)

    def delete_volume(self, dataset_id):
        return self.delete('configuration/datasets/%s' % dataset_id)

    def get_volumes(self):
        return api.get('configuration/datasets')

if __name__ == '__main__':
    api = FlockerApi()

    # Show us the version of Flocker
    print("Version:", api.get_version())

    # Get current volumes (datasets)
    print('Datasets:')
    datasets = api.get_volumes()
    print(json.dumps(datasets, sort_keys=True, indent=4))


    print('Trying to reuse the primary from returned list')
    primary_id = datasets[0]['primary']
    print('Primary:', primary_id)

    print('Create volume:')
    # Create a Flocker volume of size 10GB
    dataset_create_result = api.create_volume('my-test-volume3', 10, primary_id, profile = "gold")
    print(json.dumps(dataset_create_result, sort_keys=True, indent=4))

    volume_id = dataset_create_result['dataset_id']
    delete_result = api.delete_volume(volume_id)
    print(json.dumps(delete_result, sort_keys=True, indent=4))
