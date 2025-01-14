import argparse
from Proxy.src.Proxy import *
import atexit
def main():
    parser = argparse.ArgumentParser(description='Launch Sandbox Proxy')
    subparsers = parser.add_subparsers(dest='subcommand')

    parser_compute = subparsers.add_parser('compute')
    parser_compute.add_argument('-t', '--institution-name', type=str, help='name of the institution', required=True, dest='target')
    parser_compute.add_argument('-s', '--stop-folder', type=str, help='stop folder', required=True, dest='stopFolder')
    parser_compute.add_argument('-a', '--archive-folder', type=str, help='Archive folder', required=True, dest='archiveInfoPath')
    parser_compute.add_argument('-d', '--data-folder', type=str, help='Data folder', required=True, dest='dataStoragePath')
    parser_compute.add_argument('-l', '--log-folder', type=str, help='Log folder', required=True, dest='messageLogFolder')
    parser_compute.add_argument('-g', '--gui-list', type=str, help='Associated GUI list', required=True, dest='GUI_list')
    parser_compute.add_argument('-lf', '--log-file', type=str, help='Log File', required=True, dest='log_file')
    parser_compute.add_argument('-c', '--config-file', type=str, help='Configuration file', required=True, dest='Config_file')

    args = parser.parse_args()

    if args.subcommand == 'compute':
        compute(args)


def compute(args):
    print(args.archiveInfoPath, args.dataStoragePath, args.messageLogFolder)
    # Proxy.remove_all_tasks(f"{args.archiveInfoPath}/TASK_list_log.csv") uncomment to clean tasks
    proxy = Proxy(args.target,
                   args.stopFolder,
                   args.GUI_list,
                   args.archiveInfoPath,
                   args.dataStoragePath,
                   args.messageLogFolder,
                   args.log_file)
    atexit.register(proxy.stop_proxy)

    try:
        proxy.start()
    except Exception as e:
        print(f'Crash! Error: {e}')
    finally:
        proxy.stop_proxy()


if __name__ == "__main__":
    main()