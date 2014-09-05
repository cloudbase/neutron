# Copyright 2014 Cloudbase Solutions SRL
#
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Unit tests for the Hyper-V Utils class.
"""

import mock

from neutron.plugins.hyperv.agent import utils
from neutron.plugins.hyperv.common import constants
from neutron.tests import base


class HyperVUtilsTestCase(base.BaseTestCase):

    FAKE_VLAN_ID = 500

    def setUp(self):
        super(HyperVUtilsTestCase, self).setUp()
        self.utils = utils.HyperVUtils()

    @mock.patch.object(utils.HyperVUtils, "_get_vswitch_external_port")
    def _check_set_switch_ext_port_trunk_vlan(self,
            mock_get_vswitch_external_port, desired_endpoint_mode,
            trunked_list):
        vswitch_external_port = mock_get_vswitch_external_port.return_value
        vlan_endpoint = mock.MagicMock()
        vswitch_external_port.associators.return_value = [vlan_endpoint]
        vlan_endpoint_settings = mock.MagicMock()
        vlan_endpoint_settings.TrunkedVLANList = trunked_list
        vlan_endpoint.associators.return_value = [vlan_endpoint_settings]

        self.utils.set_switch_external_port_trunk_vlan(
            mock.sentinel.FAKE_VSWITCH_NAME, self.FAKE_VLAN_ID,
            desired_endpoint_mode)

        vswitch_external_port.associators.assert_called_once_with(
            wmi_association_class=self.utils._BINDS_TO)
        vlan_endpoint.associators.assert_called_once_with(
            wmi_result_class=self.utils._VLAN_ENDPOINT_SET_DATA)

        self.assertEqual(desired_endpoint_mode,
                         vlan_endpoint.DesiredEndpointMode)
        self.assertIn(self.FAKE_VLAN_ID,
                      vlan_endpoint_settings.TrunkedVLANList)

    @mock.patch.object(utils.HyperVUtils, "_get_vswitch_external_port")
    def test_set_switch_ext_port_trunk_vlan_internal(self,
            mock_get_vswitch_external_port):
        mock_get_vswitch_external_port.return_value = None

        self.utils.set_switch_external_port_trunk_vlan(
            mock.sentinel.FAKE_VSWITCH_NAME, self.FAKE_VLAN_ID,
            constants.TRUNK_ENDPOINT_MODE)

    def test_set_switch_ext_port_trunk_vlan_trunked_missing(self):
        self._check_set_switch_ext_port_trunk_vlan(
            desired_endpoint_mode=constants.TRUNK_ENDPOINT_MODE,
            trunked_list=[])

    def test_set_switch_ext_port_trunk_vlan_trunked_added(self):
        self._check_set_switch_ext_port_trunk_vlan(
            desired_endpoint_mode=constants.TRUNK_ENDPOINT_MODE,
            trunked_list=[self.FAKE_VLAN_ID])
