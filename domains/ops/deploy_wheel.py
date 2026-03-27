"""Deploy the data_ops wheel to a Unity Catalog Volume.

Entry point for the DABs job. Delegates to the shared utility in
``data_ops.utils.deploy_wheel``.
"""

from data_ops.utils.deploy_wheel import run_deploy_wheel

if __name__ == "__main__":
    run_deploy_wheel()
