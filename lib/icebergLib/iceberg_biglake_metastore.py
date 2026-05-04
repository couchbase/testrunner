from icebergLib.iceberg_base import IcebergBase
import requests


class BigLakeMetastoreCatalog:
    """
    BigLake Metastore Catalog provisioning helpers for Iceberg tables.
    Ported from TAF icebergLib.

    API Reference: https://docs.cloud.google.com/biglake/docs/reference/rest/v1/iceberg.v1.restcatalog.extensions.projects.catalogs
    """

    def __init__(self, state: IcebergBase):
        self.state = state

    def create_gcs_bucket(self):
        """Create GCS bucket if it doesn't exist."""
        try:
            response = requests.post(
                f"https://storage.googleapis.com/storage/v1/b?project={self.state.gcs_project_id}",
                headers={
                    "Authorization": f"Bearer {self.state.gcp_access_token()}",
                    "Content-Type": "application/json"
                },
                json={
                    "name": self.state.iceberg_bucket,
                    "location": self.state.gcs_bucket_location,
                    "locationType": "region",
                    "storageClass": "STANDARD",
                    "iamConfiguration": {
                        "uniformBucketLevelAccess": {
                            "enabled": True
                        },
                        "publicAccessPrevention": "enforced"
                    }
                }
            )
            if response.status_code == 200:
                print(f"GCS bucket {self.state.iceberg_bucket} created successfully.")
                return True
            else:
                print(f"Error while creating GCS bucket {self.state.iceberg_bucket}: {response.text}")
                return False
        except Exception as e:
            print(f"Error while creating GCS bucket {self.state.iceberg_bucket}: {str(e)}")
            return False

    def create_biglake_metastore_catalog(self):
        """Create BigLake Metastore catalog if it doesn't exist."""
        try:
            response = requests.post(
                f"https://biglake.googleapis.com/iceberg/v1/restcatalog/extensions/projects/{self.state.gcs_project_id}/catalogs?iceberg-catalog-id={self.state.iceberg_bucket}",
                headers={
                    "Authorization": f"Bearer {self.state.gcp_access_token()}",
                    "Content-Type": "application/json"
                },
                json={
                    "credential-mode": "CREDENTIAL_MODE_END_USER",
                    "catalog-type": "CATALOG_TYPE_GCS_BUCKET",
                    "description": "Analytics Iceberg Catalog"
                }
            )
            if response.status_code == 200:
                print(f"Biglake Metastore catalog {self.state.iceberg_bucket} created successfully.")
                return True
            else:
                print(f"Error while creating BigLake Metastore catalog {self.state.iceberg_bucket}: {response.text}")
                return False
        except Exception as e:
            print(f"Error while creating BigLake Metastore catalog {self.state.iceberg_bucket}: {str(e)}")
            return False

    def delete_biglake_metastore_catalog(self):
        """Delete BigLake Metastore catalog if it exists."""
        try:
            response = requests.delete(
                f"https://biglake.googleapis.com/iceberg/v1/restcatalog/extensions/projects/{self.state.gcs_project_id}/catalogs/{self.state.iceberg_bucket}",
                headers={
                    "Authorization": f"Bearer {self.state.gcp_access_token()}",
                    "Content-Type": "application/json"
                }
            )
            if response.status_code == 200:
                print(f"Biglake Metastore catalog {self.state.iceberg_bucket} deleted successfully.")
                return True
            else:
                print(f"Error while deleting BigLake Metastore catalog {self.state.iceberg_bucket}: {response.text}")
                return False
        except Exception as e:
            print(f"Error while deleting BigLake Metastore catalog {self.state.iceberg_bucket}: {str(e)}")
            return False

    def _delete_gcs_bucket_objects(self):
        """Delete all objects in the GCS bucket (required before deleting a non-empty bucket)."""
        from urllib.parse import quote
        base_url = f"https://storage.googleapis.com/storage/v1/b/{self.state.iceberg_bucket}"
        headers = {
            "Authorization": f"Bearer {self.state.gcp_access_token()}",
            "Content-Type": "application/json"
        }
        page_token = None
        deleted_count = 0
        while True:
            list_url = f"{base_url}/o"
            if page_token:
                list_url += f"?pageToken={quote(page_token)}"
            list_resp = requests.get(list_url, headers=headers)
            if list_resp.status_code != 200:
                return False
            data = list_resp.json()
            items = data.get("items") or []
            for obj in items:
                name = obj.get("name")
                if name:
                    del_url = f"{base_url}/o/{quote(name, safe='')}"
                    del_resp = requests.delete(del_url, headers=headers)
                    if del_resp.status_code in (200, 204):
                        deleted_count += 1
                    else:
                        print(f"Warning: failed to delete object {name}: {del_resp.text}")
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        if deleted_count:
            print(f"Deleted {deleted_count} object(s) from GCS bucket {self.state.iceberg_bucket}.")
        return True

    def delete_gcs_bucket(self):
        """Delete GCS bucket if it exists. Empties the bucket first if not empty."""
        try:
            # GCS does not allow deleting a non-empty bucket; delete all objects first
            response = requests.delete(
                f"https://storage.googleapis.com/storage/v1/b/{self.state.iceberg_bucket}",
                headers={
                    "Authorization": f"Bearer {self.state.gcp_access_token()}",
                    "Content-Type": "application/json"
                }
            )
            if response.status_code == 204:
                print(f"GCS bucket {self.state.iceberg_bucket} deleted successfully.")
                return True
            if response.status_code == 409 and "not empty" in response.text.lower():
                if not self._delete_gcs_bucket_objects():
                    print(f"Error while emptying GCS bucket {self.state.iceberg_bucket}.")
                    return False
                # Retry bucket delete after emptying
                response = requests.delete(
                    f"https://storage.googleapis.com/storage/v1/b/{self.state.iceberg_bucket}",
                    headers={
                        "Authorization": f"Bearer {self.state.gcp_access_token()}",
                        "Content-Type": "application/json"
                    }
                )
                if response.status_code == 200:
                    print(f"GCS bucket {self.state.iceberg_bucket} deleted successfully.")
                    return True
            print(f"Error while deleting GCS bucket {self.state.iceberg_bucket}: {response}")
        except Exception as e:
            print(f"Error while deleting GCS bucket {self.state.iceberg_bucket}: {str(e)}")
