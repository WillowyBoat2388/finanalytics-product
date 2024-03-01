import io
import pickle
import json
from typing import Any, Dict, Optional, Union

from dagster import (
    ConfigurableIOManager,
    IOManager,
    InputContext,
    MetadataValue,
    OutputContext,
    ResourceDependency,
    _check as check,
    io_manager,
)
from dagster._annotations import deprecated
from dagster._core.storage.io_manager import dagster_maintained_io_manager
from dagster._core.storage.upath_io_manager import UPathIOManager
from dagster._utils.cached_method import cached_method
from pydantic import Field
from upath import UPath

from dagster_aws.s3 import S3Resource

from datetime import datetime as dt

class JSONObjectS3IOManager(UPathIOManager):
    def __init__(
        self,
        s3_bucket: str,
        s3_session: Any,
        s3_prefix: Optional[str] = None,
        extension: Optional[str] = None,
    ):
        self.bucket = check.str_param(s3_bucket, "s3_bucket")
        check.opt_str_param(s3_prefix, "s3_prefix")
        self.s3 = s3_session
        self.s3.list_objects(Bucket=s3_bucket, Prefix=s3_prefix, MaxKeys=1)
        self.extension = extension
        base_path = UPath(s3_prefix) if s3_prefix else None
        super().__init__(base_path=base_path)

    def get_path_for_partition(self, context: InputContext | OutputContext, path: UPath, partition: str) -> UPath:
        date = dt.now().strftime("%Y-%m-%d")
        partition_date = partition + "_" + date
        return super().get_path_for_partition(context, path, partition_date)

    def _get_paths_for_partitions(self, context: InputContext):
        return super()._get_paths_for_partitions(context)

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        try:
            s3_obj = io.BytesIO(self.s3.get_object(Bucket=self.bucket, Key=path.as_posix())["Body"].read())
            return json.loads(s3_obj)
        except self.s3.exceptions.NoSuchKey:
            raise FileNotFoundError(f"Could not find file {path} in S3 bucket {self.bucket}")

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath) -> None:
        if self.path_exists(path):
            context.log.warning(f"Removing existing S3 object: {path}")
            self.unlink(path)

        
        json_obj = json.dumps(obj)
        self.s3.put_object(Bucket=self.bucket, Key=path.as_posix(), Body=json_obj)

    def path_exists(self, path: UPath) -> bool:
        try:
            self.s3.get_object(Bucket=self.bucket, Key=path.as_posix())
        except self.s3.exceptions.NoSuchKey:
            return False
        return True

    def get_loading_input_log_message(self, path: UPath) -> str:
        return f"Loading S3 object from: {self._uri_for_path(path)}"

    def get_writing_output_log_message(self, path: UPath) -> str:
        return f"Writing S3 object at: {self._uri_for_path(path)}"

    def unlink(self, path: UPath) -> None:
        self.s3.delete_object(Bucket=self.bucket, Key=path.as_posix())

    def make_directory(self, path: UPath) -> None:
        # It is not necessary to create directories in S3
        return None

    def get_metadata(self, context: OutputContext, obj: Any) -> Dict[str, MetadataValue]:
        path = self._get_path(context)
        return {"uri": MetadataValue.path(self._uri_for_path(path))}

    def get_op_output_relative_path(self, context: Union[InputContext, OutputContext]) -> UPath:
        return UPath("storage", super().get_op_output_relative_path(context))

    def _uri_for_path(self, path: UPath) -> str:
        return f"s3://{self.bucket}/{path.as_posix()}"


class S3JSONIOManager(ConfigurableIOManager):
    """Persistent IO manager using S3 for storage.

    Serializes objects via pickling. Suitable for objects storage for distributed executors, so long
    as each execution node has network connectivity and credentials for S3 and the backing bucket.

    Assigns each op output to a unique filepath containing run ID, step key, and output name.
    Assigns each asset to a single filesystem path, at "<base_dir>/<asset_key>". If the asset key
    has multiple components, the final component is used as the name of the file, and the preceding
    components as parent directories under the base_dir.

    Subsequent materializations of an asset will overwrite previous materializations of that asset.
    With a base directory of "/my/base/path", an asset with key
    `AssetKey(["one", "two", "three"])` would be stored in a file called "three" in a directory
    with path "/my/base/path/one/two/".

    Example usage:

    .. code-block:: python

        from dagster import asset, Definitions
        from dagster_aws.s3 import S3PickleIOManager, S3Resource


        @asset
        def asset1():
            # create df ...
            return df

        @asset
        def asset2(asset1):
            return asset1[:5]

        defs = Definitions(
            assets=[asset1, asset2],
            resources={
                "io_manager": S3PickleIOManager(
                    s3_resource=S3Resource(),
                    s3_bucket="my-cool-bucket",
                    s3_prefix="my-cool-prefix",
                )
            }
        )

    """

    s3_resource: ResourceDependency[S3Resource]
    s3_bucket: str = Field(description="S3 bucket to use for the file manager.")
    s3_prefix: str = Field(
        default="dagster", description="Prefix to use for the S3 bucket for this file manager."
    )
    s3_suffix: str = Field(
        default=".json", description="Suffix to use for the S3 bucket for this file manager."
    )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @cached_method
    def inner_io_manager(self) -> JSONObjectS3IOManager:
        return JSONObjectS3IOManager(
            s3_bucket=self.s3_bucket,
            s3_session=self.s3_resource.get_client(),
            s3_prefix=self.s3_prefix,
            extension=self.s3_suffix,
        )

    def load_input(self, context: InputContext) -> Any:
        return self.inner_io_manager().load_input(context)

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        return self.inner_io_manager().handle_output(context, obj)


# @deprecated(
#     breaking_version="2.0",
#     additional_warn_text="Please use S3PickleIOManager instead.",
# )
# class ConfigurableJSONObjectS3IOManager(S3JSONIOManager):
#     """Renamed to S3PickleIOManager. See S3PickleIOManager for documentation."""

#     pass


# @dagster_maintained_io_manager
# @io_manager(
#     config_schema=S3JSONIOManager.to_config_schema(),
#     required_resource_keys={"s3"},
# )
# def s3_json_io_manager(init_context):
#     """Persistent IO manager using S3 for storage.

#     Serializes objects via pickling. Suitable for objects storage for distributed executors, so long
#     as each execution node has network connectivity and credentials for S3 and the backing bucket.

#     Assigns each op output to a unique filepath containing run ID, step key, and output name.
#     Assigns each asset to a single filesystem path, at "<base_dir>/<asset_key>". If the asset key
#     has multiple components, the final component is used as the name of the file, and the preceding
#     components as parent directories under the base_dir.

#     Subsequent materializations of an asset will overwrite previous materializations of that asset.
#     With a base directory of "/my/base/path", an asset with key
#     `AssetKey(["one", "two", "three"])` would be stored in a file called "three" in a directory
#     with path "/my/base/path/one/two/".

#     Example usage:

#     1. Attach this IO manager to a set of assets.

#     .. code-block:: python

#         from dagster import Definitions, asset
#         from dagster_aws.s3 import s3_pickle_io_manager, s3_resource


#         @asset
#         def asset1():
#             # create df ...
#             return df

#         @asset
#         def asset2(asset1):
#             return asset1[:5]

#         defs = Definitions(
#             assets=[asset1, asset2],
#             resources={
#                 "io_manager": s3_pickle_io_manager.configured(
#                     {"s3_bucket": "my-cool-bucket", "s3_prefix": "my-cool-prefix"}
#                 ),
#                 "s3": s3_resource,
#             },
#         )


#     2. Attach this IO manager to your job to make it available to your ops.

#     .. code-block:: python

#         from dagster import job
#         from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

#         @job(
#             resource_defs={
#                 "io_manager": s3_pickle_io_manager.configured(
#                     {"s3_bucket": "my-cool-bucket", "s3_prefix": "my-cool-prefix"}
#                 ),
#                 "s3": s3_resource,
#             },
#         )
#         def my_job():
#             ...
#     """
#     s3_session = init_context.resources.s3
#     s3_bucket = init_context.resource_config["s3_bucket"]
#     s3_prefix = init_context.resource_config.get("s3_prefix")  # s3_prefix is optional
#     json_io_manager = JSONObjectS3IOManager(s3_bucket, s3_session, s3_prefix=s3_prefix)
#     return json_io_manager