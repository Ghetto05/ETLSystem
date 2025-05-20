from os import path

# dynamically get path of script file and place log next to it
log_file_path = path.join(path.dirname(path.abspath(__file__)), "log.txt")


def log(message, log_to_console):
    # optionally: print to console
    if log_to_console:
        print(message)
    # open log file, write output after new line, close files
    f = open(log_file_path, "a")
    f.write("\n")
    f.write(message)
    f.close()


def init_logger():
    # create log file
    file = open(log_file_path, "w")
    file.write("")
    file.close()
