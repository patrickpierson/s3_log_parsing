#!/usr/bin/env python
# import various modules
import re, glob, errno, csv, os.path
from pprint import pprint

# s3 log patterns
s3_line_logpats  = r'(\S+) (\S+) \[(.*?)\] (\S+) (\S+) ' \
           r'(\S+) (\S+) (\S+) "([^"]+)" ' \
           r'(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) ' \
           r'"([^"]+)" "([^"]+)"'

s3_line_logpat = re.compile(s3_line_logpats)

(S3_LOG_BUCKET_OWNER, S3_LOG_BUCKET, S3_LOG_DATETIME, S3_LOG_IP,
S3_LOG_REQUESTOR_ID, S3_LOG_REQUEST_ID, S3_LOG_OPERATION, S3_LOG_KEY,
S3_LOG_HTTP_METHOD_URI_PROTO, S3_LOG_HTTP_STATUS, S3_LOG_S3_ERROR,
S3_LOG_BYTES_SENT, S3_LOG_OBJECT_SIZE, S3_LOG_TOTAL_TIME,
S3_LOG_TURN_AROUND_TIME, S3_LOG_REFERER, S3_LOG_USER_AGENT) = range(17)

# s3 log pattern names
s3_names = ("bucket_owner", "bucket", "datetime", "ip", "requestor_id",
"request_id", "operation", "key", "http_method_uri_proto", "http_status",
"s3_error", "bytes_sent", "object_size", "total_time", "turn_around_time",
"referer", "user_agent")

def parse_s3_log_line(line):  # function to parse s3 log lines
    match = s3_line_logpat.match(line)
    result = [match.group(1+n) for n in range(17)]
    return result

#def dump_parsed_s3_line(parsed):
#    for (name, val) in zip(s3_names, parsed):
#        print("%s: %s" % (name, val))

def print_s3_values(writer):  # print s3 values to file
    stuff =  ",".join(s3_names)
    #line = writer.write(stuff + '\n')
    writer.write(stuff + '\n')

def csv_parsed_s3_line(parsed, writer):  # print parsed s3 lines to csv file for analysis
    stuff = ",".join(parsed)
    #line = writer.write(stuff + '\n')
    writer.write(stuff + '\n')

def count_bytes_sent(dictreader):  # count bytes sent in s3 logs
    count = 0
    for row in dictreader:
        if row['bytes_sent'] == '-':
            pass
        else:
            count += int((row['bytes_sent']))
    print str(count/1024) + 'KB'

def show_404_count(dictreader):  # show 404 errors in s3 logs
    count404 = 0
    for row in dictreader:
        if row['http_status'] == '404':
            count404 += 1
    print str(count404) + " 404 errors."

def show_ips_count(dictreader):  # show ips count in s3 logs
    ips = {}
    for row in dictreader:
        if row['ip'] == '-':
            pass
        else:
            if row['ip'] in ips:
                ips[row['ip']] += 1
            else:
                ips[row['ip']] = 1
    pprint(ips)

def show_useragent_count(dictreader):  # show useragents in s3 logs
    user_agents = {}
    for row in dictreader:
        if row['user_agent'] == '-':
            pass
        else:
            if row['user_agent'] in user_agents:
                user_agents[row['user_agent']] += 1
            else:
                user_agents[row['user_agent']] = 1
    pprint(user_agents)

def test():  # test function
    l = r'607c4573f2972c26aff39f7e56ff0490881a35c19b9bf94072cbab8c3219f948 kjkpub [06/Mar/2009:23:13:28 +0000] 41.221.20.231 65a011a29cdf8ec533ec3d1ccaae921c C46E93FF2E865AC1 REST.GET.OBJECT sumatrapdf/rel/SumatraPDF-0.9.1.zip "GET /sumatrapdf/rel/SumatraPDF-0.9.1.zip HTTP/1.1" 206 - 43457 1003293 697 611 "http://kjkpub.s3.amazonaws.com/sumatrapdf/rel/" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)"'
    print_s3_values()
    parsed = parse_s3_log_line(l)
    csv_parsed_s3_line(parsed)

def main():
    global dictreader  # global set for dictreader
    filename = 'test.csv'  # text.csv for log analysis
    if os.path.isfile(filename):  # test if test.csv exits if so print available tasks
        print "Available Tasks"
        print "----------------"
        print "count bytes sent"
        print "show 404 count"
        print "show ips count"
        print "show useragent count"
        print "quit"
        dictreader = csv.DictReader(open(filename, "r"), delimiter=',')  # open test.csv for reading
    else:
        print "Run 'parse s3 logs'"  # if test,csv doesn't exist promt user to parse s3 logs

    task_name = raw_input("Task: ")  # take input from user for task

    if task_name == 'parse s3 logs':  # if task is parse s3 logs
        # promt user for log location TODO: make log location take input better
        path = raw_input('Logs location: "./logs/*"')
        if path == '':  # if path is nothing use default logs location
            path = './logs/*'
        writetofile = 'test.csv'  # use test.csv to write to
        writer = open(writetofile, "w+")
        files = glob.glob(path)  # set glob files to pull data
        print_s3_values(writer)  # call print s3 values
        for name in files:  # 'file' is a builtin type, 'name' is a less-ambiguous variable name.
            try:
                with open(name) as f:  # No need to specify 'r': this is the default.
                    parsed = parse_s3_log_line(f.read())
                    csv_parsed_s3_line(parsed, writer)

            except IOError as exc:
                if exc.errno != errno.EISDIR:  # Do not fail if a directory is found, just ignore it.
                    raise # Propagate other kinds of IOError.
        writer.close() # close file
        main() # call main to allow user to run other tasks
    elif task_name == 'count bytes sent':  # call count bytes sent if user requests
        count_bytes_sent(dictreader)
    elif task_name == 'show 404 count':  # call show 404 count if user requests
        show_404_count(dictreader)
    elif task_name == 'show ips count':  # call show ips count if user requests
        show_ips_count(dictreader)
    elif task_name == 'show useragent count':  # call show useragent count if user requests
        show_useragent_count(dictreader)
    elif task_name == 'quit':  # quit
        os.remove('test.csv')
        exit()
    else:
        print "Invalid Task\n\n"  # print invalid task if doesnt match
        main()

if __name__ == "__main__":
    main()