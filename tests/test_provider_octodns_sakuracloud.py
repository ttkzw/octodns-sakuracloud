#
#
#

from logging import getLogger
from unittest import TestCase
from unittest.mock import patch

from octodns.provider.base import Plan
from octodns.record import Create, Delete, Record, Update
from octodns.zone import Zone

from octodns_sakuracloud import (  # SakuraCloudException,
    SakuraCloudAPI,
    SakuraCloudProvider,
    _add_trailing_dot,
    _remove_trailing_dot,
)

zone = Zone(name='unit.tests.', sub_zones=[])
octo_records = []
octo_records.append(
    Record.new(
        zone, '', {'ttl': 0, 'type': 'A', 'values': ['1.2.3.4', '10.10.10.10']}
    )
)
octo_records.append(
    Record.new(
        zone, 'a', {'ttl': 1, 'type': 'A', 'values': ['1.2.3.4', '1.1.1.1']}
    )
)
octo_records.append(
    Record.new(zone, 'aa', {'ttl': 3600, 'type': 'A', 'values': ['1.2.4.3']})
)
octo_records.append(
    Record.new(zone, 'aaa', {'ttl': 2, 'type': 'A', 'values': ['1.1.1.3']})
)
octo_records.append(
    Record.new(
        zone, 'cname', {'ttl': 3, 'type': 'CNAME', 'value': 'a.unit.tests.'}
    )
)
octo_records.append(
    Record.new(zone, '', {'ttl': 3, 'type': 'ALIAS', 'value': 'a.unit.tests.'})
)
octo_records.append(
    Record.new(
        zone,
        'mx1',
        {
            'ttl': 3,
            'type': 'MX',
            'values': [
                {'priority': 10, 'value': 'mx1.unit.tests.'},
                {'priority': 20, 'value': 'mx2.unit.tests.'},
            ],
        },
    )
)
octo_records.append(
    Record.new(
        zone,
        'mx2',
        {
            'ttl': 3,
            'type': 'MX',
            'values': [{'priority': 10, 'value': 'mx1.unit.tests.'}],
        },
    )
)
octo_records.append(
    Record.new(
        zone, 'foo', {'ttl': 5, 'type': 'NS', 'value': 'ns1.unit.tests.'}
    )
)
octo_records.append(
    Record.new(
        zone,
        '_srv._tcp',
        {
            'ttl': 6,
            'type': 'SRV',
            'values': [
                {
                    'priority': 10,
                    'weight': 20,
                    'port': 30,
                    'target': 'foo-1.unit.tests.',
                },
                {
                    'priority': 12,
                    'weight': 30,
                    'port': 30,
                    'target': 'foo-2.unit.tests.',
                },
            ],
        },
    )
)
octo_records.append(
    Record.new(
        zone,
        '_srv2._tcp',
        {
            'ttl': 7,
            'type': 'SRV',
            'values': [
                {
                    'priority': 12,
                    'weight': 17,
                    'port': 1,
                    'target': 'srvfoo.unit.tests.',
                }
            ],
        },
    )
)
octo_records.append(
    Record.new(
        zone, 'txt1', {'ttl': 8, 'type': 'TXT', 'value': 'txt singleton test'}
    )
)
octo_records.append(
    Record.new(
        zone,
        'txt2',
        {
            'ttl': 9,
            'type': 'TXT',
            'values': ['txt multiple test', 'txt multiple test 2'],
        },
    )
)
octo_records.append(
    Record.new(
        zone,
        'caa',
        {
            'ttl': 9,
            'type': 'CAA',
            'value': {'flags': 0, 'tag': 'issue', 'value': 'ca.unit.tests'},
        },
    )
)
octo_records.append(
    Record.new(
        zone,
        '_8443._https',
        {
            'ttl': 9,
            'type': 'SVCB',
            'value': {
                'svcpriority': 1,
                'targetname': '.',
                'svcparams': {'alpn': ['h2']},
            },
        },
    )
)
octo_records.append(
    Record.new(
        zone,
        'www',
        {
            'ttl': 9,
            'type': 'HTTPS',
            'value': {
                'svcpriority': 1,
                'targetname': '.',
                'svcparams': {'alpn': ['h2']},
            },
        },
    )
)
octo_records.append(
    Record.new(
        zone,
        '',
        {
            'ttl': 9,
            'type': 'HTTPS',
            'value': {'svcpriority': 0, 'targetname': 'pool.unit.tests.'},
        },
    )
)
for record in octo_records:
    zone.add_record(record)

# This is the format which the google API likes.
resource_record_sets = [
    {"Name": '@', "Type": 'A', "RData": '1.2.3.4', "TTL": 0},
    {"Name": '@', "Type": 'A', "RData": '10.10.10.10', "TTL": 0},
    {"Name": 'a', "Type": 'A', "RData": '1.1.1.1', "TTL": 1},
    {"Name": 'a', "Type": 'A', "RData": '1.2.3.4', "TTL": 1},
    {"Name": 'aa', "Type": 'A', "RData": '1.2.4.3', "TTL": 3600},
    {"Name": 'aaa', "Type": 'A', "RData": '1.1.1.3', "TTL": 2},
    {"Name": '@', "Type": 'ALIAS', "RData": 'a.unit.tests.', "TTL": 3},
    {"Name": 'cname', "Type": 'CNAME', "RData": 'a.unit.tests.', "TTL": 3},
    {"Name": 'mx1', "Type": 'MX', "RData": '10 mx1.unit.tests.', "TTL": 3},
    {"Name": 'mx1', "Type": 'MX', "RData": '20 mx2.unit.tests.', "TTL": 3},
    {"Name": 'mx2', "Type": 'MX', "RData": '10 mx1.unit.tests.', "TTL": 3},
    {"Name": 'foo', "Type": 'NS', "RData": 'ns1.unit.tests.', "TTL": 5},
    {
        "Name": '_srv._tcp',
        "Type": 'SRV',
        "RData": '10 20 30 foo-1.unit.tests.',
        "TTL": 6,
    },
    {
        "Name": '_srv._tcp',
        "Type": 'SRV',
        "RData": '12 30 30 foo-2.unit.tests.',
        "TTL": 6,
    },
    {
        "Name": '_srv2._tcp',
        "Type": 'SRV',
        "RData": '12 17 1 srvfoo.unit.tests.',
        "TTL": 7,
    },
    {"Name": 'txt1', "Type": 'TXT', "RData": 'txt singleton test', "TTL": 8},
    {"Name": 'txt2', "Type": 'TXT', "RData": 'txt multiple test', "TTL": 9},
    {"Name": 'txt2', "Type": 'TXT', "RData": 'txt multiple test 2', "TTL": 9},
    {"Name": 'caa', "Type": 'CAA', "RData": '0 issue ca.unit.tests', "TTL": 9},
    {"Name": '_8443._https', "Type": 'SVCB', "RData": '1 . alpn=h2', "TTL": 9},
    {"Name": 'www', "Type": 'HTTPS', "RData": '1 . alpn=h2', "TTL": 9},
    {"Name": '@', "Type": 'HTTPS', "RData": '0 pool.unit.tests.', "TTL": 9},
]

sakuracloud_zone = {
    "Index": 0,
    "ID": "999999999999",
    "Name": "unit.tests",
    "Description": "",
    "Settings": {"DNS": {"ResourceRecordSets": resource_record_sets}},
    "SettingsHash": "ffffffffffffffffffffffffffffffff",
    "Status": {
        "Zone": "unit.tests",
        "NS": ["ns1.gslbN.sakura.ne.jp", "ns2.gslbN.sakura.ne.jp"],
    },
    "ServiceClass": "cloud/dns",
    "Availability": "available",
    "CreatedAt": "2006-01-02T15:04:05+07:00",
    "ModifiedAt": "2006-01-02T15:04:05+07:00",
    "Provider": {
        "ID": 9999999,
        "Class": "dns",
        "Name": "gslbN.sakura.ne.jp",
        "ServiceClass": "cloud/dns",
    },
    "Icon": None,
    "Tags": [],
}

response_common_service_items = {
    "From": 0,
    "Count": 2,
    "Total": 2,
    "CommonServiceItems": [
        {
            "Index": 0,
            "ID": "999999999999",
            "Name": "unit.tests",
            "Description": "",
            "Settings": {"DNS": {"ResourceRecordSets": resource_record_sets}},
            "SettingsHash": "ffffffffffffffffffffffffffffffff",
            "Status": {
                "Zone": "unit.tests",
                "NS": ["ns1.gslbN.sakura.ne.jp", "ns2.gslbN.sakura.ne.jp"],
            },
            "ServiceClass": "cloud/dns",
            "Availability": "available",
            "CreatedAt": "2006-01-02T15:04:05+07:00",
            "ModifiedAt": "2006-01-02T15:04:05+07:00",
            "Provider": {
                "ID": 9999999,
                "Class": "dns",
                "Name": "gslbN.sakura.ne.jp",
                "ServiceClass": "cloud/dns",
            },
            "Icon": None,
            "Tags": [],
        },
        {
            "Index": 1,
            "ID": "888888888888",
            "Name": "unit.tests",
            "Description": "",
            "ServiceClass": "cloud/foo",
            "Availability": "available",
            "CreatedAt": "2006-01-02T15:04:05+07:00",
            "ModifiedAt": "2006-01-02T15:04:05+07:00",
            "Provider": {
                "ID": 9999999,
                "Class": "foo",
                "ServiceClass": "cloud/foo",
            },
            "Icon": None,
            "Tags": [],
        },
    ],
    "is_ok": True,
}

response_common_service_items_without_zones = {
    "From": 0,
    "Count": 0,
    "Total": 0,
    "CommonServiceItems": [],
    "is_ok": True,
}

request_common_service_item_for_post = {
    "CommonServiceItem": {
        "Name": "unit.tests",
        "Status": {"Zone": "unit.tests"},
        "Settings": {"DNS": {"ResourceRecordSets": []}},
        "Provider": {"Class": "dns"},
    }
}

response_common_service_item_for_post = {
    "CommonServiceItem": {
        "ID": "999999999999",
        "Name": "unit.tests",
        "Description": "",
        "Settings": {"DNS": {"ResourceRecordSets": []}},
        "SettingsHash": "ffffffffffffffffffffffffffffffff",
        "Status": {
            "Zone": "unit.tests",
            "NS": ["ns1.gslbN.sakura.ne.jp", "ns2.gslbN.sakura.ne.jp"],
        },
        "ServiceClass": "cloud/dns",
        "Availability": "available",
        "CreatedAt": "2006-01-02T15:04:05+07:00",
        "ModifiedAt": "2006-01-02T15:04:05+07:00",
        "Provider": {
            "ID": 9999999,
            "Class": "dns",
            "Name": "gslbN.sakura.ne.jp",
            "ServiceClass": "cloud/dns",
        },
        "Icon": None,
        "Tags": [],
    },
    "Success": True,
    "is_ok": True,
}

request_common_service_item_for_put = {
    "CommonServiceItem": {
        "Settings": {"DNS": {"ResourceRecordSets": resource_record_sets}}
    }
}

response_common_service_item_for_put = {
    "CommonServiceItem": {
        "ID": "999999999999",
        "Name": "unit.tests",
        "Description": "",
        "Settings": {"DNS": {"ResourceRecordSets": resource_record_sets}},
        "SettingsHash": "ffffffffffffffffffffffffffffffff",
        "Status": {
            "Zone": "unit.tests",
            "NS": ["ns1.gslbN.sakura.ne.jp", "ns2.gslbN.sakura.ne.jp"],
        },
        "ServiceClass": "cloud/dns",
        "Availability": "available",
        "CreatedAt": "2006-01-02T15:04:05+07:00",
        "ModifiedAt": "2006-01-02T15:04:05+07:00",
        "Provider": {
            "ID": 9999999,
            "Class": "dns",
            "Name": "gslbN.sakura.ne.jp",
            "ServiceClass": "cloud/dns",
        },
        "Icon": None,
        "Tags": [],
    },
    "Success": True,
    "is_ok": True,
}


class TestAddTrailingDot(TestCase):
    def test_add_trailing_dot(self):
        for expected, test in [
            ['tests.', 'tests'],
            ['tests.', 'tests.'],
            ['unit.tests.', 'unit.tests.'],
            ['unit.tests.', 'unit.tests'],
        ]:
            self.assertEqual(expected, _add_trailing_dot(test))


class TestRemoveTrailingDot(TestCase):

    def test_remove_trailing_dot(self):
        for expected, arg in [
            ['tests', 'tests'],
            ['tests', 'tests.'],
            ['unit.tests', 'unit.tests.'],
            ['unit.tests', 'unit.tests'],
        ]:
            self.assertEqual(expected, _remove_trailing_dot(arg))


class TestSakuraCloudAPI(TestCase):

    def _get_api(self):
        log = getLogger('SakuraCloudProvider')
        return SakuraCloudAPI("", "", "http://localhost", 1, log)

    @patch('octodns_sakuracloud.SakuraCloudAPI._request')
    def test_get_zone(self, mock_request):
        api = self._get_api()

        mock_request.return_value = response_common_service_items
        self.assertDictEqual(sakuracloud_zone, api.get_zone("unit.tests."))

    @patch('octodns_sakuracloud.SakuraCloudAPI._request')
    def test_get_zone_names(self, mock_request):
        api = self._get_api()

        mock_request.return_value = response_common_service_items
        self.assertListEqual(["unit.tests."], api.get_zone_names())

    @patch('octodns_sakuracloud.SakuraCloudAPI._request')
    def test_create_zone(self, mock_request):
        api = self._get_api()

        mock_request.return_value = response_common_service_items_without_zones
        api.get_zone_names()

        mock_request.return_value = response_common_service_item_for_post
        api.create_zone("unit.tests.")

        for c in mock_request.mock_calls:
            if c.args[0] == "POST":
                self.assertEqual('/commonserviceitem', c.args[1])
                self.assertEqual(
                    request_common_service_item_for_post, c.kwargs["json"]
                )

    @patch('octodns_sakuracloud.SakuraCloudAPI._request')
    def test_update_zone(self, mock_request):
        api = self._get_api()

        mock_request.return_value = response_common_service_items_without_zones
        api.get_zone_names()

        mock_request.return_value = response_common_service_item_for_post
        api.create_zone("unit.tests.")

        mock_request.return_value = response_common_service_item_for_post
        api.update_zone("unit.tests.", resource_record_sets)

        for c in mock_request.mock_calls:
            if c.args[0] == "PUT":
                self.assertEqual('/commonserviceitem/999999999999', c.args[1])
                self.assertEqual(
                    request_common_service_item_for_put, c.kwargs["json"]
                )


class TestSakuraCloudProvider(TestCase):

    def _get_provider(self):
        return SakuraCloudProvider(
            1, "", "", endpoint="http://localhost/", timeout=60
        )

    @patch.object(SakuraCloudAPI, 'get_zone_names')
    def test_list_zones(self, mock_get_zone_names):
        provider = self._get_provider()

        for expected, arg in [
            [[], []],
            [["unit.tests."], ["unit.tests."]],
            [
                ["a.unit.tests.", "b.unit.tests."],
                ["b.unit.tests.", "a.unit.tests."],
            ],
        ]:
            mock_get_zone_names.return_value = arg
            self.assertListEqual(expected, provider.list_zones())

    @patch.object(SakuraCloudAPI, 'get_zone')
    def test_populate(self, mock_get_zone):
        provider = self._get_provider()

        mock_get_zone.return_value = sakuracloud_zone
        actual_zone = Zone('unit.tests.', [])
        provider.populate(actual_zone)
        self.assertSetEqual(set(octo_records), actual_zone.records)

        mock_get_zone.return_value = None
        actual_zone = Zone('unit.tests.', [])
        self.assertFalse(provider.populate(actual_zone))

    @patch('octodns_sakuracloud.SakuraCloudAPI')
    def test_apply(self, mock_api):
        provider = self._get_provider()

        self.maxDiff = True

        mock_api.get_zone_names.return_value = ['unit.tests.']

        apply_z = Zone("unit.tests.", [])
        create_r = Record.new(
            apply_z,
            '',
            {'ttl': 0, 'type': 'A', 'values': ['1.2.3.4', '10.10.10.10']},
        )
        delete_r = Record.new(
            apply_z,
            'a',
            {'ttl': 1, 'type': 'A', 'values': ['1.2.3.4', '1.1.1.1']},
        )
        update_existing_r = Record.new(
            apply_z, 'aa', {'ttl': 9001, 'type': 'A', 'values': ['1.2.4.3']}
        )
        update_new_r = Record.new(
            apply_z, 'aa', {'ttl': 666, 'type': 'A', 'values': ['1.4.3.2']}
        )

        existing = Zone('unit.tests.', [])
        existing.add_record(update_existing_r)
        existing.add_record(delete_r)

        desired = Zone('unit.tests.', [])
        desired.add_record(create_r)
        desired.add_record(update_new_r)

        changes = []
        changes.append(Create(create_r))
        changes.append(Delete(delete_r))
        changes.append(Update(existing=update_existing_r, new=update_new_r))

        provider.apply(
            Plan(
                existing=existing, desired=desired, changes=changes, exists=True
            )
        )

        rrsets = [
            {'Name': '@', 'Type': 'A', 'RData': '1.2.3.4', 'TTL': 0},
            {'Name': '@', 'Type': 'A', 'RData': '10.10.10.10', 'TTL': 0},
            {'Name': 'aa', 'Type': 'A', 'RData': '1.4.3.2', 'TTL': 666},
        ]

        for c in mock_api.mock_calls:
            if str(c).startswith('call().update_zone('):
                self.assertEqual("unit.tests.", c.args[0])
                actual = sorted(
                    c.args[1],
                    key=lambda x: ':'.join([x['Name'], x['Type'], x['RData']]),
                )
                self.assertEqual(rrsets, actual)

        # create zone
        mock_api.get_zone_names.return_value = []

        apply_z = Zone("unit.tests.", [])

        desired = Zone('unit.tests.', [])
        for r in octo_records:
            desired.add_record(r)

        existing = None

        changes = []
        changes.append(Create(create_r))

        provider.apply(
            Plan(
                existing=existing, desired=desired, changes=changes, exists=True
            )
        )

        for c in mock_api.mock_calls:
            if str(c).startswith('call().create_zone'):
                self.assertEqual("unit.tests.", c.args[0])

            # if str(c).startswith('call().update_zone('):
            #     self.assertEqual("unit.tests.", c.args[0])
            #     actual = sorted(
            #         c.args[1],
            #         key=lambda x: ':'.join([x['Name'], x['Type'], x['RData']]),
            #     )
            #     self.assertEqual(rrsets, actual)
