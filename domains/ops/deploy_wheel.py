"""Deploy the data_ops wheel to a Unity Catalog Volume.

Copies the built wheel artifact from the workspace (where DABs uploads it)
to /Volumes/{catalog}/gold/wheels/ so it is available for team development
and job dependencies.
"""

import argparse
from pathlib import PurePosixPath

from pyspark.dbutils import DBUtils  # type: ignore[import-unresolved]
from pyspark.sql import SparkSession


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy data_ops wheel to UC Volume")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name (dev, sit, prod)")
    parser.add_argument(
        "--wheel-path",
        required=True,
        help="Workspace path to the wheel artifact (set by DABs artifact reference)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    catalog: str = args.catalog
    wheel_workspace_path: str = args.wheel_path

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    wheel_name = PurePosixPath(wheel_workspace_path).name
    volume_dir = f"/Volumes/{catalog}/gold/wheels"
    dest_path = f"{volume_dir}/{wheel_name}"

    # The artifact reference resolves to a workspace path like
    # /Workspace/.bundle/<target>/artifacts/.internal/<wheel>.whl
    # dbutils.fs.cp handles workspace paths directly.
    print(f"Source : {wheel_workspace_path}")
    print(f"Dest   : {dest_path}")

    dbutils.fs.cp(wheel_workspace_path, dest_path, recurse=False)

    print(f"Successfully deployed {wheel_name} to {dest_path}")


if __name__ == "__main__":
    main()
