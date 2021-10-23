from argparse import ArgumentParser, RawDescriptionHelpFormatter
from authtypes import SendTypes
from sortingmethods import SortingMethods
from host import Host
import os
import json
import const

class Config:
    def __init__(self):
        self.InputFolder:str = os.getcwd()
        self.SortingMethod = SortingMethods.ByName
        self.RemoteHosts = []
        self.LogFolder = const.LOG_FOLDER_PATH

    def load(self, filename):
        fd = None
        err = ""
        try:
            fd = open(filename, "rt")
            data = json.load(fd)
            if data:
                if data.get(const.INPUT_FOLDER_KEY):
                    self.InputFolder = data.get(const.INPUT_FOLDER_KEY)

                if data.get(const.LOG_FOLDER_KEY):
                    self.LogFolder = data.get(const.LOG_FOLDER_KEY)

                if data.get(const.SORTING_KEY):
                    self.SortingMethod = SortingMethods(data.get(const.SORTING_KEY))

                if data.get(const.HOSTS_KEY):
                    hosts = data.get(const.HOSTS_KEY)
                    if hosts is not None:
                        for h in hosts:
                            host = Host()
                            if h.get(const.ADDRESS_KEY):
                                host.Address = h.get(const.ADDRESS_KEY)
                            if h.get(const.PORT_KEY):
                                host.Port = h.get(const.PORT_KEY)
                            if h.get(const.USERNAME_KEY):
                                host.UserName = h.get(const.USERNAME_KEY)
                            if h.get(const.PASSWORD_KEY):
                                host.Password = h.get(const.PASSWORD_KEY)
                            if h.get(const.SENDTYPE_KEY):
                                host.SendType =  SendTypes(h.get(const.SENDTYPE_KEY))
                            if h.get(const.CERTPATH_KEY):
                                host.CertPath = h.get(const.CERTPATH_KEY)
                            if h.get(const.REMOTE_PATH_KEY):
                                host.RemotePath = h.get(const.REMOTE_PATH_KEY)
                            if h.get(const.TIMEOUT_KEY):
                                host.ConnectTimeout = h.get(const.TIMEOUT_KEY)
                            if h.get(const.COMPRESSION_KEY):
                                host.Compression = h.get(const.COMPRESSION_KEY)

                            if host.Address:
                                self.RemoteHosts.append(host)

        except Exception as ex:
            err = str(ex)
            return f"Config file could not read: {ex}"
        finally:
            if fd is not None:
                fd.close()



    def parsearguments(self):
        parser = ArgumentParser(
            usage="python %(prog)scdraggregator.py [PROCESS OPTIONS]|[TEST OPTIONS]",
            description="Examine over cdr files and generate aggregated data ",
            allow_abbrev=False,
            epilog= """
                Examples:
                1.  The following example filters .cdr files which is contains '15' in name in the /home/input/test folder and generate output files for "query times" and "reason code" to /home/output.
                    Once all files processed then program terminates 
                    
                    python ./%(prog)s --input=/home/input/test --output=/home/output --input-pattern=*15*.cdr --output-types Query Code 
                    
                    
                2.  The following example watch working directory and  generate output files to /home/output folder continously per 15 minutes. Node name will be set as "Odine Test Node"
                    Once all files processed then program waits new files until to terminates by user
                    
                    python ./%(prog)s --output=/home/output --interval=15 --node="Odine Test Node"
                    
                    
                3.  Program watch working directory and waits the new files with default values. Then generates output files to same directory or all output types
                    Once all files processed then program waits new files until to terminates by user
                
                    python ./%(prog)s
                    
            """
        )
        common_options_group = parser.add_argument_group("COMMON OPTIONS")
        process_options_group = parser.add_argument_group("PROCESS OPTIONS")
        test_options_group = parser.add_argument_group("TEST OPTIONS")

        process_options_group.add_argument("-i", "--input", metavar="", dest="input", type=str, required=False,
                                           help="Indicates the folder which will be used as source to get cdr files. Default: working directory")
        process_options_group.add_argument("-o", "--output", metavar="", dest="output", type=str, required=False,
                                           help="Indicates which folder will used to store the files contains aggregated data, Default: working directory")

        process_options_group.add_argument("-op", "--output-pattern", metavar="", dest="output_pattern", type=str, required=False,
                                           help="Template of output file name. [NODE], [TIMESTAMP] and [TYPE] variables will be replaced. Default: [NODE]_[TIMESTAMP]_[TYPE].csv")
        process_options_group.add_argument("--interval", metavar="", dest="interval", type=int, required=False,
                                          help="Indicates how many minutes the files will be created in an hour. Default: 15 minutes")
        process_options_group.add_argument("-qt", "--query-time-interval", metavar="", dest="query_time_interval",
                                           type=int, required=False,
                                           help="Indicates how many minute intervals the data will be grouped in each file for query times. Default: 5 minutes")
        process_options_group.add_argument("-cc", "--cause-code-interval", metavar="", dest="cause_code_interval",
                                           type=int, required=False,
                                           help="Indicates how many minute intervals the data will be grouped in each file for cause codes. Default: 5 minutes")
        process_options_group.add_argument("-cps", "--cps-metric-interval", metavar="", dest="cps_metric_interval",
                                           type=int, required=False,
                                           help="Indicates how many minute intervals the data will be grouped in each file for cps metrics. Default: 1 minutes")
        process_options_group.add_argument("--node", metavar="", dest="node", type=str, required=False,
                                           help="Node name which is used for replace [NODE] variable. Default: current machine name")
        test_options_group.add_argument("-s", "--source", metavar="", dest="input", type=str, required=False,
                                        help="Indicates the folder which will be used as source to get cdr files. Default: working directory")
        test_options_group.add_argument("-ip", "--input-pattern", metavar="", dest="input_pattern", type=str, required=False,
                                        help="Accept pattern for input file names. Wildcards (* or ?) can be used. Available only for on-demand process. Default: *.cdr")


        common_options_group.add_argument("-l", "--log-folder", metavar="", dest="log_folder", type=str,
                                          required=False,
                                          help="Folder where log files will be stored. Default: log folder in current directory ")
        common_options_group.add_argument("-qts", "--query-time-suffix", metavar="", dest="query_time_suffix", type=str,
                                          required=False,
                                          help="Suffix which will be replaced with [TYPE] variable on file name. Default: querytimes")
        common_options_group.add_argument("-ccs", "--cause-code-suffix", metavar="", dest="cause_code_suffix", type=str,
                                          required=False,
                                          help="Suffix which will be replaced with [TYPE] variable on file name. Default: causecodes")
        common_options_group.add_argument("-cpss", "--cps-metric-suffix", metavar="", dest="cps_metric_suffix",
                                          type=str, required=False,
                                          help="Suffix which will be replaced with [TYPE] variable on file name. Default: cpsmetrics")

        common_options_group.add_argument("-t", "--test", dest="ondemand", required=False, action="store_true",
                                          help="Specifies working mode. Watching mode enabled if omitted. "
                                               "If this option set, program will be terminated all files in folder processed. Otherwise program will be waits to new files until to terminated by use")
        return parser.parse_args()
