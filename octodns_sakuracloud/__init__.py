"""octodns_sakuracloud"""

#
#
#

from __future__ import annotations

from html import unescape
from importlib import import_module
from logging import Logger, getLogger
from types import ModuleType
from typing import Any
from urllib.parse import quote_plus

from requests import HTTPError, RequestException, Response, request
from requests.auth import HTTPBasicAuth

from octodns.provider import ProviderException
from octodns.provider.base import BaseProvider
from octodns.provider.plan import Plan
from octodns.record import CaaValue, Change, Record, TxtValue
from octodns.zone import Zone

# TODO: remove __VERSION__ with the next major version release
__version__ = __VERSION__ = '0.0.1'


def _add_trailing_dot(value: str) -> str:
    if len(value) > 0 and value[-1] != '.':
        value = f'{value}.'
    return value


def _remove_trailing_dot(value: str) -> str:
    if len(value) > 0 and value[-1] == '.':
        value = value[:-1]
    return value


class SakuraCloudAPI:
    """SakuraCloudAPI"""

    def __init__(
        self,
        access_token: str,
        access_token_secret: str,
        endpoint: str,
        timeout: int,
        log: Logger,
    ) -> None:
        """Create a SakuraCloudAPI object.

        :param access_token: an access token for API key
        :type  access_token: str
        :param access_token_secret: an access token secret for API key
        :type  access_token_secret: str
        :param endpoint: an endpoint of API
        :type  endpoint: str
        :param timeout: request timeout
        :type  timeout: int
        :param log: request timeout
        :type  log: logging.Logger
        """
        self._auth = HTTPBasicAuth(access_token, access_token_secret)
        self._endpoint: str = endpoint
        self._timeout: int = timeout
        self._common_service_item_map = None
        self.log: Logger = log

    def _request(
        self, method: str, path: str, json: dict[str, dict[str, Any]] = None
    ) -> dict[str, Any]:
        """Wrapper method for `requests.request()`.

        :param method: method for the new :class:`Request` object: ``GET``,
            ``POST``, ``PUT``.
        :type  method: str
        :param url: URL for the new :class:`Request` object.
        :type  url: str
        :param json: (optional) A JSON serializable Python object to send in
            the body of the :class:`Request`.
        :type  json: dict
        :return: Dictionary that decodes the JSON response body.
        :rtype: dict
        """
        self.log.debug('_request: method=%s, path=%s', method, path)

        headers: dict[str, str] = {}
        if json is not None:
            headers["Content-Type"] = "application/json; charset=UTF-8"

        url: str = f"{self._endpoint}{path}"
        try:
            resp: Response = request(
                method=method,
                url=url,
                json=json,
                headers=headers,
                auth=self._auth,
                timeout=self._timeout,
            )
            self.log.debug('_request:   status=%d', resp.status_code)
        except RequestException as e:
            raise SakuraCloudException(
                f"Request error: method={method}, url={url}: {e}"
            ) from e

        # Response body structure of an error response:
        #
        #   {
        #     "is_fatal": true,
        #     "serial": "ffffffffffffffffffffffffffffffff",
        #     "status": "401 Unauthorized",
        #     "error_code": "unauthorized",
        #     "error_msg": "error-unauthorized"
        #   }
        try:
            resp.raise_for_status()
        except HTTPError as e:
            err: dict[str, Any] = e.response.json()
            raise SakuraCloudException(
                f"HTTP error: method={method}, url={url}, "
                f"status={err['status']}, serial={err['serial']}, "
                f"error_code={err['error_code']}, "
                f"error_msg={unescape(err['error_msg'])}"
            ) from e
        return resp.json()

    def _get_common_service_items(self) -> list[dict[str, Any]]:
        """Gets all the zones in the account.

        :return: List of CommonServiceItem.
        :rtype: list
        """
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

        # The query string is similar to the flow-style YAML.
        #   {Count: 0}
        query: str = quote_plus("{Count: 0}")
        path = f"/commonserviceitem?{query}"
        resp_data: dict[str, Any] = self._request("GET", path)
        common_service_items: list[dict[str, Any]] = resp_data[
            "CommonServiceItems"
        ]
        return common_service_items

    def _get_common_service_item_map(self) -> dict[str, dict[str, Any]]:
        """Get map of all the zones in the account.

        :return: Dictionary of zone name and CommonServiceItem.
        :rtype: dict
        """
        if self._common_service_item_map is not None:
            return self._common_service_item_map

        self._common_service_item_map: dict[str, Any] = {}

        items: list[dict[str, Any]] = self._get_common_service_items()

        for item in items:
            if item["ServiceClass"] != "cloud/dns":
                continue
            zone_name: str = _add_trailing_dot(item["Status"]["Zone"])
            self._common_service_item_map[zone_name] = item

        return self._common_service_item_map

    def get_zone(self, zone_name: str) -> dict[str, Any] | None:
        """Gets a zone data.

        :param zone_name: A zone name.
        :type  zone_name: str
        :return: CommonServiceItem.
        :rtype: dict
        """
        return self._get_common_service_item_map().get(zone_name)

    def get_zone_names(self) -> list[str]:
        """Returns a list of zone names.

        :return: list of CommonServiceItem.
        :rtype: list
        """
        return list(self._get_common_service_item_map().keys())

    def _post_common_service_item(
        self, data: dict[str, dict[str, Any]]
    ) -> dict[str, Any]:
        """Submits a CommonServiceItem to the API and create the zone.

        :param data: Request data.
        :type  data: dict[str, dict[str, Any]]
        :return: CommonServiceItem.
        :rtype: dict[str, Any]
        """
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
        return self._request("POST", "/commonserviceitem", json=data)

    def create_zone(self, zone_name: str) -> dict[str, Any]:
        """Submits a CommonServiceItem to the API and create the zone.

        :param zone_name: A zone name.
        :type  zone_name: str
        :return: CommonServiceItem.
        :rtype: dict
        """
        name: str = _remove_trailing_dot(zone_name)
        req_item: dict[str, dict[str, Any]] = {
            "CommonServiceItem": {
                "Name": name,
                "Status": {"Zone": name},
                "Settings": {"DNS": {"ResourceRecordSets": []}},
                "Provider": {"Class": "dns"},
            }
        }

        resp_data: dict[str, dict[str, Any]] = self._post_common_service_item(
            req_item
        )
        item: dict[str, Any] = resp_data["CommonServiceItem"]
        self._get_common_service_item_map()[zone_name] = item

    def _put_common_service_item(
        self, item_id: str, data: dict[str, dict[str, Any]]
    ) -> dict[str, Any]:
        """Submits a CommonServiceItem to the API and updates the zone data.

        :param item_id: ID of CommonServiceItem.
        :type  item_id: str
        :param data: Request data.
        :type  data: dict
        :return: response.
        :rtype: dict
        """
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
        return self._request("PUT", f"/commonserviceitem/{item_id}", json=data)

    def update_zone(
        self, zone_name: str, rrsets: list[dict[str, str | int]]
    ) -> dict[str, Any]:
        """Submits a CommonServiceItem to the API and updates the zone data.

        :param zone_name: A zone name.
        :type  zone_name: str
        :param rrsets: A list of ResourceRecordSet.
        :type  rrsets: list
        :return: CommonServiceItem.
        :rtype: dict
        """
        req_data: dict[str, dict[str, Any]] = {
            "CommonServiceItem": {
                "Settings": {"DNS": {"ResourceRecordSets": rrsets}}
            }
        }

        resp_data: dict[str, Any] = self._put_common_service_item(
            self._common_service_item_map[zone_name]['ID'], req_data
        )
        item: dict[str, Any] = resp_data["CommonServiceItem"]
        self._get_common_service_item_map()[zone_name] = item


class SakuraCloudException(ProviderException):
    """SakuraCloudException"""


class SakuraCloudProvider(BaseProvider):  # noqa: #501
    """SakuraCloud Provider

    ```
    sakuracloud:
      class: octodns_sakuracloud.SakuraCloudProvider
      # The access token for an API key
      access_token:
      # The access token secret for an API key
      access_token_secret:
      # The endpoint for APIs
      endpoint:
      #
      # The `endpoint` is optional. If omitted, the default endpoint is
      # assumed.
      #
      # Endpoints are as follows:
      #
      # - Ishikari first Zone
      #   - https://secure.sakura.ad.jp/cloud/zone/is1a/api/cloud/1.1
      # - Ishikari second Zone
      #   - https://secure.sakura.ad.jp/cloud/zone/is1b/api/cloud/1.1
      # - Tokyo first Zone
      #   - https://secure.sakura.ad.jp/cloud/zone/tk1a/api/cloud/1.1
      # - Tokyo second Zone
      #   - https://secure.sakura.ad.jp/cloud/zone/tk1b/api/cloud/1.1
      #
      # DNS service is independent of zones, so you can use any of these
      # endpoints. The default is the Ishikari first Zone.
    ```

    Example config file with variables:

    ```
    ---
    providers:
      config:
        class: octodns.provider.yaml.YamlProvider
        directory: ./config (example path to directory of zone files)
      sakuracloud:
        class: octodns_sakuracloud.SakuraCloudProvider
        access_token: env/SAKURACLOUD_ACCESS_TOKEN
        access_token_secret: env/SAKURACLOUD_ACCESS_TOKEN_SECRET

    zones:
      example.com.:
        sources:
          - config
        targets:
          - sakuracloud
    ```
    """

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

    def __init__(  # pylint: disable=W1113,W0622
        self,
        id: str,
        access_token: str,
        access_token_secret: str,
        endpoint: str = DEFAULT_ENDPOINT,
        timeout: int = TIMEOUT,
        *args,
        **kwargs,
    ) -> None:
        """Creates an SakuraCloudProvider object.

        :param id: ID.
        :type  id: int
        :param access_token: The access token for an API key.
        :type  access_token: str
        :param access_token_secret: The access token secret for an API key.
        :type  access_token_secret: str
        :param endpoint: The endpoint for APIs.
        :type  endpoint: str
        :param timeout: Request timeout.
        :type  timeout: int
        """
        self.log: Logger = getLogger(f'SakuraCloudProvider[{id}]')
        self.log.debug('__init__: id=%s', id)

        self.api = SakuraCloudAPI(
            access_token=access_token,
            access_token_secret=access_token_secret,
            endpoint=endpoint,
            timeout=timeout,
            log=self.log,
        )
        super().__init__(id, *args, **kwargs)

    def list_zones(self) -> list[str]:
        """Returns a list of zone names.

        This method is required by octodns.manager.Manager._preprocess_zones().

        :return: A list of zone names.
        :rtype: list
        """
        self.log.debug('list_zones:')
        return sorted(self.api.get_zone_names())

    def populate(
        self, zone, target: bool = False, lenient: bool = False
    ) -> bool:
        """Loads all records the provider knows about for the provided zone.

        Required function of manager.py to collect records from zone.

        This method overrides octodns.source.base.BaseSource.populate().

        :param zone: A dns zone
        :type  zone: octodns.zone.Zone
        :param target: If True, the populate call is being made to load the
                current state of the provider.
        :type  target: bool
        :param lenient: Unused.
        :type  lenient: bool
        :return: When target is True (loading current state) this method should
            return True if the zone exists or False if it does not.
        :rtype: bool
        """
        self.log.debug(
            'populate: name=%s, target=%s, lenient=%s',
            zone.name,
            target,
            lenient,
        )

        before: int = len(zone.records)

        item: dict[str, Any] | None = self.api.get_zone(zone.name)
        if item is None:
            return False
        exists = True

        rrset_map: dict[str, dict[str, Any]] = {}
        for rr in item["Settings"]["DNS"]["ResourceRecordSets"]:
            _type: str = rr["Type"]
            if _type not in self.SUPPORTS:
                continue
            key: str = rr["Name"] + '\0' + rr["Type"]
            if key not in rrset_map:
                record_name: str = rr["Name"]
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

        module: ModuleType = import_module("octodns.record")

        for rrset in rrset_map.values():
            _class_name: str = f'{rrset["type"].title()}Record'
            cls: Record = getattr(module, _class_name)
            values: list[str] = cls.parse_rdata_texts(rrset["rdatas"])
            data: dict[str, Any] = {"type": rrset["type"], "ttl": rrset["ttl"]}
            if len(values) == 1:
                data["value"] = values[0]
            else:
                data["values"] = values
            self.log.debug(data)
            record: Record = Record.new(
                zone, rrset['name'], data, source=self, lenient=lenient
            )
            zone.add_record(record, lenient=lenient)

        self.log.info(
            'populate:   found %s records, exists=%s',
            len(zone.records) - before,
            exists,
        )
        return exists

    def _apply(self, plan: Plan) -> None:
        """Submits actual planned changes to the provider. Returns the number of
        changes made.

        This method overrides octodns.provider.base.BaseProvider._apply()

        :param plan: Contains the zones and changes to be made
        :type  plan: octodns.provider.plan.Plan
        """
        desired: Zone = plan.desired
        changes: list[Change] = plan.changes

        self.log.debug(
            '_apply: zone=%s, len(changes)=%d', desired.name, len(changes)
        )

        if desired.name not in self.list_zones():
            self.api.create_zone(desired.name)

        rrsets: list[dict[str, Any]] = []
        for record in desired.records:
            record_name: str = record.name
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
                    v: dict[str, Any] = CaaValue.parse_rdata_text(value)
                    value: str = f'{v["flags"]} {v["tag"]} "{v["value"]}"'
                rr: dict[str, str | int] = {
                    "Name": record_name,
                    "Type": _type,
                    "RData": value,
                }
                if ttl != SakuraCloudProvider.DEFAULT_TTL:
                    rr["TTL"] = ttl
                rrsets.append(rr)

        self.api.update_zone(desired.name, rrsets)
