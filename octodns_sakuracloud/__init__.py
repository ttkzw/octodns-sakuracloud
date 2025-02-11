'''octodns_sakuracloud'''

#
#
#

from importlib import import_module
from urllib.parse import quote_plus
import html
from logging import getLogger
from requests import request, RequestException, HTTPError
from requests.auth import HTTPBasicAuth

from octodns.provider import ProviderException
from octodns.provider.base import BaseProvider
from octodns.record import Record, TxtValue, CaaValue


# TODO: remove __VERSION__ with the next major version release
__version__ = __VERSION__ = '0.0.1'


def _add_trailing_dot(value):
    if len(value) > 0 and value[-1] != '.':
        value = f'{value}.'
    return value


def _remove_trailing_dot(value):
    if len(value) > 0 and value[-1] == '.':
        value = value[:-1]
    return value


class SakuraCloudAPI:
    '''SakuraCloudAPI'''

    def __init__(
        self, access_token, access_token_secret, endpoint, timeout, log
    ):
        self.log = log
        self._auth = HTTPBasicAuth(access_token, access_token_secret)
        self._endpoint = endpoint
        self._timeout = timeout
        self._common_service_item_map = None

    def _request(self, method, path, data=None):
        # Response body structure of an error response:
        #
        #   {
        #     "is_fatal": true,
        #     "serial": "ffffffffffffffffffffffffffffffff",
        #     "status": "401 Unauthorized",
        #     "error_code": "unauthorized",
        #     "error_msg": "error-unauthorized"
        #   }
        self.log.debug('_request: method=%s, path=%s', method, path)

        headers = {}
        if data is not None:
            headers["Content-Type"] = "application/json; charset=UTF-8"

        url = f"{self._endpoint}{path}"
        try:
            resp = request(
                method=method,
                url=url,
                json=data,
                headers=headers,
                auth=self._auth,
                timeout=self._timeout,
            )
            self.log.debug('_request:   status=%d', resp.status_code)
        except RequestException as e:
            raise SakuraCloudException(
                f"Request error: method={method}, url={url}: {e}"
            ) from e

        try:
            resp.raise_for_status()
        except HTTPError as e:
            err = e.response.json()
            raise SakuraCloudException(
                f"HTTP error: method={method}, url={url}, "
                f"status={err["status"]}, serial={err["serial"]}, "
                f"error_code={err["error_code"]}, "
                f"error_msg={html.unescape(err['error_msg'])}"
            ) from e
        return resp

    def _get_common_service_items(self):
        '''_get_common_service_items return all the zones in the account.'''
        # Response body structure:
        #
        # {
        #   "From": 0,
        #   "Count": 1,
        #   "Total": 1,
        #   "CommonServiceItems": [
        #     {
        #       "Index": 0,
        #       "ID": "999999999999",
        #       "Name": "example.com",
        #       "Description": "",
        #       "Settings": {
        #         "DNS": {
        #           "ResourceRecordSets": [
        #             {
        #               "Name": "a",
        #               "Type": "A",
        #               "RData": "192.0.2.1",
        #               "TTL": 600
        #             },
        #             ...
        #           ]
        #         }
        #       },
        #       "SettingsHash": "ffffffffffffffffffffffffffffffff",
        #       "Status": {
        #         "Zone": "example.com",
        #         "NS": [
        #           "ns1.gslbN.sakura.ne.jp",
        #           "ns2.gslbN.sakura.ne.jp"
        #         ]
        #       },
        #       "ServiceClass": "cloud/dns",
        #       "Availability": "available",
        #       "CreatedAt": "2006-01-02T15:04:05+07:00",
        #       "ModifiedAt": "2006-01-02T15:04:05+07:00",
        #       "Provider": {
        #         "ID": 9999999,
        #         "Class": "dns",
        #         "Name": "gslbN.sakura.ne.jp",
        #         "ServiceClass": "cloud/dns"
        #       },
        #       "Icon": null,
        #       "Tags": []
        #     }
        #   ],
        #   "is_ok": true
        # }
        common_service_items = []

        next_from = 0
        count = 100
        query = ""

        while True:
            path = "/commonserviceitem"
            if next_from > 0:
                # The query string is similar to the flow-style YAML.
                #   {From: 0, Count: 10}
                query = quote_plus(
                    "{From: " + next_from + ", Count: " + count + "}"
                )
                path = f"/commonserviceitem?{query}"

            resp = self._request("GET", path)
            resp_data = resp.json()
            common_service_items.extend(resp_data["CommonServiceItems"])

            count = resp_data["Count"]
            next_from = resp_data["From"] + resp_data["Count"]
            if next_from == resp_data["Total"]:
                break

        return common_service_items

    def get_common_service_item_map(self):
        '''get_common_service_item_map return all the zones in the account'''
        if self._common_service_item_map is not None:
            return self._common_service_item_map

        self._common_service_item_map = {}

        items = self._get_common_service_items()

        for item in items:
            if item["ServiceClass"] != "cloud/dns":
                continue
            zone_name = _add_trailing_dot(item["Status"]["Zone"])
            self._common_service_item_map[zone_name] = item

        return self._common_service_item_map

    def _post_common_service_item(self, req_item):
        '''_post_common_service_item submits a CommonServiceItem to the API
        and create the zone.
        '''
        # Request body structure:
        #
        #   {
        #     "CommonServiceItem": {
        #       "Name": "example.com",
        #       "Status": {
        #         "Zone": "example.com"
        #       },
        #       "Settings": {
        #         "DNS": {
        #           "ResourceRecordSets": []
        #         }
        #       },
        #       "Provider": {
        #         "Class": "dns"
        #       },
        #     }
        #   }
        #
        # Response body structure:
        #
        #   {
        #     "CommonServiceItem": {
        #       "ID": "999999999999",
        #       "Name": "example.com",
        #       "Description": "",
        #       "Settings": {
        #         "DNS": {
        #           "ResourceRecordSets": [
        #             {
        #               "Name": "a",
        #               "Type": "A",
        #               "RData": "192.0.2.1",
        #               "TTL": 600
        #             },
        #             ...
        #           ]
        #         }
        #       },
        #       "SettingsHash": "ffffffffffffffffffffffffffffffff",
        #       "Status": {
        #         "Zone": "example.com",
        #         "NS": [
        #           "ns1.gslbN.sakura.ne.jp",
        #           "ns2.gslbN.sakura.ne.jp"
        #         ]
        #       },
        #       "ServiceClass": "cloud/dns",
        #       "Availability": "available",
        #       "CreatedAt": "2006-01-02T15:04:05+07:00",
        #       "ModifiedAt": "2006-01-02T15:04:05+07:00",
        #       "Provider": {
        #         "ID": 9999999,
        #         "Class": "dns",
        #         "Name": "gslbN.sakura.ne.jp",
        #         "ServiceClass": "cloud/dns"
        #       },
        #       "Icon": null,
        #       "Tags": []
        #     },
        #     "Success": true,
        #     "is_ok": true
        #   }
        resp = self._request("POST", "/commonserviceitem", data=req_item)
        resp_data = resp.json()
        return resp_data["CommonServiceItem"]

    def create_zone(self, zone_name):
        '''create_zone submits a CommonServiceItem to the API and create the
        zone.
        '''
        name = _remove_trailing_dot(zone_name)
        req_item = {
            "CommonServiceItem": {
                "Name": name,
                "Status": {"Zone": name},
                "Settings": {"DNS": {"ResourceRecordSets": []}},
                "Provider": {"Class": "dns"},
            }
        }

        item = self._post_common_service_item(req_item)
        self._common_service_item_map[zone_name] = item

    def _put_common_service_item(self, item_id, req_item):
        '''_put_common_service_item submits a CommonServiceItem to the API and
        updates the zone data.
        '''
        # Request body structure:
        #
        #   {
        #     "CommonServiceItem": {
        #       "Settings": {
        #         "DNS": {
        #           "ResourceRecordSets": [
        #             {
        #               "Name": "a",
        #               "Type": "A",
        #               "RData": "192.0.2.1",
        #               "TTL": 600
        #             },
        #             ...
        #           ]
        #         }
        #       }
        #     }
        #   }
        #
        # Response body structure:
        #
        #   {
        #     "CommonServiceItem": {
        #       "ID": "999999999999",
        #       "Name": "example.com",
        #       "Description": "",
        #       "Settings": {
        #         "DNS": {
        #           "ResourceRecordSets": [
        #             {
        #               "Name": "a",
        #               "Type": "A",
        #               "RData": "192.0.2.1",
        #               "TTL": 600
        #             },
        #             ...
        #           ]
        #         }
        #       },
        #       "SettingsHash": "ffffffffffffffffffffffffffffffff",
        #       "Status": {
        #         "Zone": "example.com",
        #         "NS": [
        #           "ns1.gslbN.sakura.ne.jp",
        #           "ns2.gslbN.sakura.ne.jp"
        #         ]
        #       },
        #       "ServiceClass": "cloud/dns",
        #       "Availability": "available",
        #       "CreatedAt": "2006-01-02T15:04:05+07:00",
        #       "ModifiedAt": "2006-01-02T15:04:05+07:00",
        #       "Provider": {
        #         "ID": 9999999,
        #         "Class": "dns",
        #         "Name": "gslbN.sakura.ne.jp",
        #         "ServiceClass": "cloud/dns"
        #       },
        #       "Icon": null,
        #       "Tags": []
        #     },
        #     "Success": true,
        #     "is_ok": true
        #   }
        resp = self._request(
            "PUT", f"/commonserviceitem/{item_id}", data=req_item
        )
        resp_data = resp.json()
        return resp_data["CommonServiceItem"]

    def update_zone(self, zone_name, rrsets):
        '''update_zone submits a CommonServiceItem to the API and updates the
        zone data.
        '''
        req_item = {
            "CommonServiceItem": {
                "Settings": {"DNS": {"ResourceRecordSets": rrsets}}
            }
        }

        item = self._put_common_service_item(
            self._common_service_item_map[zone_name]['ID'], req_item
        )
        self._common_service_item_map[zone_name] = item


class SakuraCloudException(ProviderException):
    '''SakuraCloudException'''


class SakuraCloudProvider(BaseProvider):
    '''SakuraCloud Provider

    sakuracloud:
        class: octodns_sakuracloud.SakuraCloudProvider
        # The access token for an API key
        access_token:
        # The access token secret for an API key
        access_token_secret:
        # The endpoint for APIs
        endpoint:
        #
        # The `endpoint` is optional. If omitted, the default endpoint is used.
        #
        # Endpoints are as follows:
        #
        # - Ishikari Zone 1
        #   - https://secure.sakura.ad.jp/cloud/zone/is1a/api/cloud/1.1
        # - Ishikari Zone 2
        #   - https://secure.sakura.ad.jp/cloud/zone/is1b/api/cloud/1.1
        # - Tokyo Zone 1
        #   - https://secure.sakura.ad.jp/cloud/zone/tk1a/api/cloud/1.1
        # - Tokyo Zone 2
        #   - https://secure.sakura.ad.jp/cloud/zone/tk1b/api/cloud/1.1
        #
        # DNS service is global, so you can use any of them.
        # The default is the Ishikari Zone 1.
    '''

    SUPPORTS_GEO = False
    SUPPORTS_DYNAMIC = False
    SUPPORTS_ROOT_NS = False
    SUPPORTS = set(
        (
            'A',
            'AAAA',
            'ALIAS',
            'CAA',
            'CNAME',
            'HTTPS',
            'MX',
            'NS',
            'PTR',
            'SRV',
            'SVCB',
            'TXT',
        )
    )

    TIMEOUT = 60

    DEFAULT_ENDPOINT = (
        "https://secure.sakura.ad.jp/cloud/zone/is1a/api/cloud/1.1"
    )

    DEFAULT_TTL = 3600

    def __init__(
        self,
        id,
        access_token,
        access_token_secret,
        endpoint=DEFAULT_ENDPOINT,
        timeout=TIMEOUT,
        *args,
        **kwargs,
    ):  # pylint: disable=W1113,W0622
        self.log = getLogger(f'SakuraCloudProvider[{id}]')
        self.log.debug('__init__: id=%s', id)

        self.api = SakuraCloudAPI(
            access_token=access_token,
            access_token_secret=access_token_secret,
            endpoint=endpoint,
            timeout=timeout,
            log=self.log,
        )
        super().__init__(id, *args, **kwargs)

    def _create_zone(self, zone_name):
        self.api.create_zone(zone_name=zone_name)

    def _update_zone(self, zone_name, rrsets):
        self.api.update_zone(zone_name=zone_name, rrsets=rrsets)

    def list_zones(self):
        '''list_zones returns a list of zone names.'''
        self.log.debug('list_zones:')
        item_map = self.api.get_common_service_item_map()
        return sorted(item_map.keys())

    def populate(self, zone, target=False, lenient=False):
        self.log.debug(
            'populate: name=%s, target=%s, lenient=%s',
            zone.name,
            target,
            lenient,
        )

        before = len(zone.records)
        exists = False

        item_map = self.api.get_common_service_item_map()
        if zone.name not in item_map:
            return False
        exists = True

        rrset_map = {}
        item = item_map[zone.name]
        for rr in item["Settings"]["DNS"]["ResourceRecordSets"]:
            _type = rr["Type"]
            if _type not in self.SUPPORTS:
                continue
            key = rr["Name"] + '\0' + rr["Type"]
            if key not in rrset_map:
                record_name = rr["Name"]
                if record_name == "@":
                    record_name = ""
                rrset_map[key] = {
                    "name": record_name,
                    "type": rr["Type"],
                    "rdatas": [rr["RData"]],
                    "ttl": rr.get('TTL', SakuraCloudProvider.DEFAULT_TTL),
                }
            else:
                rrset_map[key]["rdatas"].append(rr["RData"])

        module = import_module("octodns.record")

        for rrset in rrset_map.values():
            _class_name = f'{rrset["type"].title()}Record'
            cls = getattr(module, _class_name)
            values = cls.parse_rdata_texts(rrset["rdatas"])
            if rrset["type"] in ["HTTPS", "SVCB"]:
                # workaround against error: Unknown SvcParam
                for value in values:
                    if "" in value["svcparams"]:
                        value["svcparams"] = {}
            data = {"type": rrset["type"], "ttl": rrset["ttl"]}
            if len(values) == 1:
                data["value"] = values[0]
            else:
                data["values"] = values
            self.log.debug(data)
            record = Record.new(
                zone, rrset['name'], data, source=self, lenient=lenient
            )
            zone.add_record(record, lenient=lenient)

        self.log.info(
            'populate:   found %s records, exists=%s',
            len(zone.records) - before,
            exists,
        )
        return exists

    def _apply(self, plan):
        desired = plan.desired
        changes = plan.changes

        self.log.debug(
            '_apply: zone=%s, len(changes)=%d', desired.name, len(changes)
        )

        if desired.name not in self.api.get_common_service_item_map():
            self._create_zone(desired.name)

        rrsets = []
        for record in desired.records:
            record_name = record.name
            if record.name == "":
                record_name = "@"
            _, ttl, _type, values = record.rrs
            if _type == "TXT":
                values = TxtValue.process(values)
            for value in values:
                if _type in ["TXT", "HTTPS", "SVCB"]:
                    # Revert escaped semicolons in _ChunkedValue.
                    value = value.replace('\\;', ';')
                if _type == "CAA":
                    v = CaaValue.parse_rdata_text(value)
                    value = f'{v["flags"]} {v["tag"]} "{v["value"]}"'
                rr = {"Name": record_name, "Type": _type, "RData": value}
                if ttl != SakuraCloudProvider.DEFAULT_TTL:
                    rr["TTL"] = ttl
                rrsets.append(rr)

        self._update_zone(desired.name, rrsets)
