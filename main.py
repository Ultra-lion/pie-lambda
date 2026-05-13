
from docker_builder.build_images import build_n_deploy, teardown, run_existing
from argparse import ArgumentParser


from control_plane.control_plane_db import ControlPlaneDB


if __name__=="__main__":
    args = parser.parse_args()

    config_file_path = args.get("config_file_path")
    command = args.get("command")

    if not config_file_path:
        raise Exception("Need config file path")

    config = json.load(config_file_path)

    if not config:
        raise Exception("Need config file")

    if not command:
        raise Exception("Need a command")
    
    control_plane_db = ControlPlaneDB()
    
    if command=="buildNdeploy":
        build_n_deploy(config)
    elif command=="teardown":
        teardown(config)
    elif command=="RunExisting":
        run_existing()
