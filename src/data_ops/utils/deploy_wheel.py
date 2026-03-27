"""Wheel deployment utility for Unity Catalog Volumes.

Copies a built wheel artifact from the workspace (where DABs uploads it)
to a Unity Catalog Volume so it is available for cluster libraries and
team development.

Usage as a script::

    python -m data_ops.utils.deploy_wheel --catalog dev --wheel-path /Workspace/.bundle/.../my.whl

Usage from code::

    from data_ops.utils.deploy_wheel import deploy_wheel
    deploy_wheel(dbutils, catalog="dev", wheel_workspace_path="/Workspace/.../my.whl")
"""

import argparse
from pathlib import PurePosixPath
from typing import Any


def deploy_wheel(
    dbutils: Any,
    catalog: str,
    wheel_workspace_path: str,
    volume_path: str | None = None,
) -> str:
    """Copy a wheel artifact from the workspace to a Unity Catalog Volume.

    Args:
        dbutils: Databricks DBUtils instance for filesystem operations.
        catalog: Unity Catalog name (e.g. ``"dev"``, ``"sit"``, ``"prod"``).
        wheel_workspace_path: Workspace path to the wheel artifact as resolved
            by a DABs artifact reference (e.g.
            ``/Workspace/.bundle/<target>/artifacts/.internal/<wheel>.whl``).
        volume_path: Destination volume path relative to ``/Volumes/``.
            Defaults to ``{catalog}/gold/wheels``.

    Returns:
        The full destination path where the wheel was copied.

    Example::

        dest = deploy_wheel(dbutils, catalog="dev",
                            wheel_workspace_path="/Workspace/.bundle/.../foo.whl")
        print(dest)  # /Volumes/dev/gold/wheels/foo.whl
    """
    volume_dir = f"/Volumes/{volume_path}" if volume_path else f"/Volumes/{catalog}/gold/wheels"
    wheel_name = PurePosixPath(wheel_workspace_path).name
    dest_path = f"{volume_dir}/{wheel_name}"

    print(f"Source : {wheel_workspace_path}")
    print(f"Dest   : {dest_path}")

    dbutils.fs.cp(wheel_workspace_path, dest_path, recurse=False)

    print(f"Successfully deployed {wheel_name} to {dest_path}")
    return dest_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy a wheel to a Unity Catalog Volume")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name (dev, sit, prod)")
    parser.add_argument(
        "--wheel-path",
        required=True,
        help="Workspace path to the wheel artifact (set by DABs artifact reference)",
    )
    parser.add_argument(
        "--volume-path",
        default=None,
        help="Destination path relative to /Volumes/ (default: {catalog}/gold/wheels)",
    )
    return parser.parse_args(argv)


def run_deploy_wheel(argv: list[str] | None = None) -> None:
    from pyspark.dbutils import DBUtils  # type: ignore[import-unresolved]
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    deploy_wheel(
        dbutils=dbutils,
        catalog=args.catalog,
        wheel_workspace_path=args.wheel_path,
        volume_path=args.volume_path,
    )


if __name__ == "__main__":
    run_deploy_wheel()
