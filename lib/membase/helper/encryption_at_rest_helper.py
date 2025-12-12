import json
import random
import string
from datetime import datetime, timedelta

import logger
from lib.membase.api.rest_client import RestConnection



class EncryptionUtil:
    def __init__(self, task_manager):
        self.log = logger.Logger.get_logger()

    @staticmethod
    def generate_random_name(prefix, length=8):
        chars = string.ascii_letters + string.digits
        suffix = ''.join(random.choice(chars) for _ in range(length))
        return prefix + suffix

    @staticmethod
    def create_secret_params(secret_type="cb-server-managed-aes-key-256",
                             name="Default secret", usage=None,
                             autoRotation=True, rotationIntervalInDays=60,
                             rotationIntervalInSeconds=None, keyARN=None,
                             region=None, useIMDS=None, credentialsFile=None,
                             configFile=None, profile=None,
                             caSelection=None, reqTimeoutMs=None,
                             encryptionApproach=None, encryptWith=None,
                             encryptWithKeyId=None, activeKey=None,
                             keyPath=None, certPath=None, keyPassphrase=None,
                             host=None, port=None):
        if usage is None:
            usage = ["bucket-encryption"]

        data = {}
        if host is None:
            data = {
                "autoRotation": autoRotation,
                "rotationIntervalInDays": rotationIntervalInDays
            }

            if rotationIntervalInSeconds is not None:
                data["nextRotationTime"] = (datetime.utcnow() + timedelta(
                    seconds=rotationIntervalInSeconds)).isoformat() + "Z"
            else:
                data["nextRotationTime"] = (datetime.utcnow() + timedelta(
                    days=rotationIntervalInDays)).isoformat() + "Z"

        if keyARN is not None:
            data["keyARN"] = keyARN
        if region is not None:
            data["region"] = region
        if useIMDS is not None:
            data["useIMDS"] = useIMDS
        if credentialsFile is not None:
            data["credentialsFile"] = credentialsFile
        if configFile is not None:
            data["configFile"] = configFile
        if profile is not None:
            data["profile"] = profile

        if caSelection is not None:
            data["caSelection"] = caSelection
        if reqTimeoutMs is not None:
            data["reqTimeoutMs"] = reqTimeoutMs
        if encryptionApproach is not None:
            data["encryptionApproach"] = encryptionApproach
        if encryptWith is not None:
            data["encryptWith"] = encryptWith
        if encryptWithKeyId is not None:
            data["encryptWithKeyId"] = encryptWithKeyId
        if activeKey is not None:
            data["activeKey"] = activeKey
        if keyPath is not None:
            data["keyPath"] = keyPath
        if certPath is not None:
            data["certPath"] = certPath
        if keyPassphrase is not None:
            data["keyPassphrase"] = keyPassphrase
        if host is not None:
            data["host"] = host
        if port is not None:
            data["port"] = port

        params = {
            "type": secret_type,
            "name": name,
            "usage": usage,
            "data": data
        }
        return params

    def bypass_encryption_restrictions(self, server):
        rest = RestConnection(server)
        status, content = rest.diag_eval("ns_config:set(test_bypass_encr_cfg_restrictions, true).")
        self.log.info("Bypassed encryption restrictions. Status: {0}, Output: {1}".format(status, content))
        return status, content

    def set_encryption_ids(self, test_obj, encryption_result):
        """Set the returned encryption IDs back to test_obj"""
        if 'KMIP_id' in encryption_result and encryption_result['KMIP_id'] is not None:
            test_obj.KMIP_id = encryption_result['KMIP_id']
        if 'encryption_at_rest_id' in encryption_result and encryption_result['encryption_at_rest_id'] is not None:
            test_obj.encryption_at_rest_id = encryption_result['encryption_at_rest_id']
        if 'config_encryption_at_rest_id' in encryption_result and encryption_result['config_encryption_at_rest_id'] is not None:
            test_obj.config_encryption_at_rest_id = encryption_result['config_encryption_at_rest_id']
        if 'log_encryption_at_rest_id' in encryption_result and encryption_result['log_encryption_at_rest_id'] is not None:
            test_obj.log_encryption_at_rest_id = encryption_result['log_encryption_at_rest_id']
        if 'audit_encryption_at_rest_id' in encryption_result and encryption_result['audit_encryption_at_rest_id'] is not None:
            test_obj.audit_encryption_at_rest_id = encryption_result['audit_encryption_at_rest_id']

    def setup_encryption_at_rest(self, cluster_master, bypass_encryption_func,
                                 create_KMIP_secret=False, enable_encryption_at_rest=False,
                                 enable_config_encryption_at_rest=False, enable_log_encryption_at_rest=False,
                                 enable_audit_encryption_at_rest=False, secret_rotation_interval=None,
                                 kmip_key_uuid=None, client_certs_path=None, KMIP_pkcs8_file_name=None,
                                 KMIP_cert_file_name=None, private_key_passphrase=None, kmip_host_name=None,
                                 KMIP_for_config_encryption=False, config_dekLifetime=None,
                                 config_dekRotationInterval=None, KMIP_for_log_encryption=False,
                                 log_dekLifetime=None, log_dekRotationInterval=None,
                                 KMIP_for_audit_encryption=False, audit_dekLifetime=None,
                                 audit_dekRotationInterval=None):
        result = {}
        rest = RestConnection(cluster_master)
        bypass_encryption_func()

        if create_KMIP_secret:
            params = EncryptionUtil.create_secret_params(
                name=EncryptionUtil.generate_random_name("kmip"),
                secret_type="kmip-aes-key-256",
                usage=["KEK-encryption", "bucket-encryption", "config-encryption", "log-encryption",
                       "audit-encryption"],
                caSelection="useSysAndCbCa", reqTimeoutMs=5000, encryptionApproach="useGet",
                encryptWith="nodeSecretManager", encryptWithKeyId=-1,
                activeKey={"kmipId": kmip_key_uuid},
                keyPath=client_certs_path + KMIP_pkcs8_file_name,
                certPath=client_certs_path + KMIP_cert_file_name,
                keyPassphrase=private_key_passphrase,
                host=kmip_host_name, port=5696
            )
            status, response = rest.create_secret(params)
            response_dict = json.loads(response)
            result['KMIP_id'] = response_dict.get('id')

        if enable_encryption_at_rest:
            self.log.info("Initializing encryption at rest")
            log_params = EncryptionUtil.create_secret_params(
                name=EncryptionUtil.generate_random_name("EncryptionSecret"),
                rotationIntervalInSeconds=secret_rotation_interval
            )
            self.log.info("Log Params: {}".format(log_params))
            status, response = rest.create_secret(log_params)
            response_dict = json.loads(response)
            result['encryption_at_rest_id'] = response_dict.get('id')
            self.log.info("Encryption at rest ID: {0}".format(result['encryption_at_rest_id']))

        if enable_config_encryption_at_rest:
            self.log.info("Initializing config encryption at rest")
            log_params = EncryptionUtil.create_secret_params(
                name=EncryptionUtil.generate_random_name("ConfigEncryptionSecret"),
                usage=["config-encryption"],
                rotationIntervalInSeconds=secret_rotation_interval
            )
            self.log.info("Config Log Params: {}".format(log_params))
            status, response = rest.create_secret(log_params)
            response_dict = json.loads(response)
            config_encryption_at_rest_id = response_dict.get('id')
            if KMIP_for_config_encryption:
                config_encryption_at_rest_id = result.get('KMIP_id')
            result['config_encryption_at_rest_id'] = config_encryption_at_rest_id
            self.log.info("Config encryption at rest ID: {0}".format(config_encryption_at_rest_id))
            valid_params = {
                "config.encryptionMethod": "encryptionKey",
                "config.encryptionKeyId": config_encryption_at_rest_id
            }
            if config_dekLifetime is not None:
                valid_params["config.dekLifetime"] = config_dekLifetime
            if config_dekRotationInterval is not None:
                valid_params["config.dekRotationInterval"] = config_dekRotationInterval
            status, response = rest.configure_encryption_at_rest(valid_params)
            self.log.info("Config encryption at rest status: {0}".format(status))
            if not status:
                raise Exception("Failed to enable config encryption values: {0}".format(response))

        if enable_log_encryption_at_rest:
            self.log.info("Initializing log encryption at rest")
            log_params = EncryptionUtil.create_secret_params(
                name=EncryptionUtil.generate_random_name("LogEncryptionSecret"),
                usage=["log-encryption"],
                rotationIntervalInSeconds=secret_rotation_interval
            )
            self.log.info("Log Log Params: {}".format(log_params))
            status, response = rest.create_secret(log_params)
            response_dict = json.loads(response)
            log_encryption_at_rest_id = response_dict.get('id')
            if KMIP_for_log_encryption:
                log_encryption_at_rest_id = result.get('KMIP_id')
            result['log_encryption_at_rest_id'] = log_encryption_at_rest_id
            self.log.info("Log encryption at rest ID: {0}".format(log_encryption_at_rest_id))
            valid_params = {
                "log.encryptionMethod": "encryptionKey",
                "log.encryptionKeyId": log_encryption_at_rest_id
            }
            if log_dekLifetime is not None:
                valid_params["log.dekLifetime"] = log_dekLifetime
            if log_dekRotationInterval is not None:
                valid_params["log.dekRotationInterval"] = log_dekRotationInterval
            status, response = rest.configure_encryption_at_rest(valid_params)
            self.log.info("Log encryption at rest status: {0}".format(status))
            if not status:
                raise Exception("Failed to set valid log encryption values: {0}".format(response))

        if enable_audit_encryption_at_rest:
            self.log.info("Initializing audit encryption at rest")
            log_params = EncryptionUtil.create_secret_params(
                name=EncryptionUtil.generate_random_name("AuditEncryptionSecret"),
                usage=["audit-encryption"],
                rotationIntervalInSeconds=secret_rotation_interval
            )
            self.log.info("Audit Log Log Params: {}".format(log_params))
            status, response = rest.create_secret(log_params)
            response_dict = json.loads(response)
            audit_encryption_at_rest_id = response_dict.get('id')
            if KMIP_for_audit_encryption:
                audit_encryption_at_rest_id = result.get('KMIP_id')
            result['audit_encryption_at_rest_id'] = audit_encryption_at_rest_id
            self.log.info("Audit encryption at rest ID: {0}".format(audit_encryption_at_rest_id))
            valid_params = {
                "audit.encryptionMethod": "encryptionKey",
                "audit.encryptionKeyId": audit_encryption_at_rest_id
            }
            if audit_dekLifetime is not None:
                valid_params["audit.dekLifetime"] = audit_dekLifetime
            if audit_dekRotationInterval is not None:
                valid_params["audit.dekRotationInterval"] = audit_dekRotationInterval
            status, response = rest.configure_encryption_at_rest(valid_params)
            self.log.info("Audit encryption at rest status: {0}".format(status))
            if not status:
                raise Exception("Failed to set valid Audit encryption values: {0}".format(response))

        return result
