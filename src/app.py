from datetime import datetime
import os
import sys
import csv
import json
import re
import logging
import optparse
import collections
import dedupe
import platform
from collections import defaultdict
# import pandas as pd
import datetime
from datetime import datetime

# from openpyxl import Workbook
from unidecode import unidecode
from flask import Flask
from flask import Flask, request, jsonify

currentDateTime = datetime.now().strftime("%Y%m%d%H%M%S")
input_file = 'sites_2023-10-14_13-05-24.csv' if platform.system() == 'Windows' else '/home/ec2-user/dedeupe_files/site_input.csv'
output_file = f"Sites_duplicate_dl_for_training_out_{currentDateTime}.csv" if platform.system() == 'Windows' else '/home/ec2-user/dedeupe_files/site_duplicate_dl_for_training_out.csv'
training_file = 'Sites_NIC_Training.json' if platform.system() == 'Windows' else '/home/ec2-user/dedeupe_files/site_NICtraining.json'
settings_file = 'Sites_NIC_Learnt_settings' if platform.system() == 'Windows'  else '/home/ec2-user/dedeupe_files/site_NIClearned_settings'
site_clustered_dupes = 'adi_site_clustered_dupes.csv' if platform.system() == 'Windows'  else '/home/ec2-user/dedeupe_files/site_clustered_dupes.csv'


app = Flask(__name__)

@app.route('/TestingConnection')
def hello():
    return 'Hello, Testing My Flask Application'


def parseInput(request_data):
    print("==========",request_data)
    file_name = request_data.get("csvPathRequest")
    if file_name:
        # Get the absolute path to the file
        file_path = os.path.abspath(file_name)
        print(file_path)
        return file_path
    return None


def find_duplicates(siteName,
                    status,
                    siteType,
                    latitude,
                    longitude,
                    address1,
                    address2,
                    city,
                    state,
                    country,
                    pin,
                    source,
                    cluster_id,
                    confidence_score):
    # severity = kwargs.get('SEVERITY', 'F')
    # site_name = kwargs.get('siteName', '')
    # status = kwargs.get('status', '')
    # site_type = kwargs.get('siteType', '')
    # latitude = kwargs.get('latitude', '')
    # longitude = kwargs.get('longitude', '')
    # address1 = kwargs.get('address1', '')
    # address2 = kwargs.get('address2', '')
    # city = kwargs.get('city', '')
    # state = kwargs.get('state', '')
    # country = kwargs.get('country', '')
    # pin = kwargs.get('pin', 0)
    # created_by = kwargs.get('createdBy', '')
    # created_at = kwargs.get('createdAt', '')
    # last_modified_at = kwargs.get('lastModifiedAt', '')
    # source = kwargs.get('source', '')
    # cluster_id = kwargs.get('cluster_id', '')
    # confidence_score = kwargs.get('confidence_score', '')

    dupes = {
        "SEVERITY": "F",
        "siteName": siteName,
        "status": status,
        "siteType": siteType,
        "latitude": latitude,
        "longitude": longitude,
        "address1": address1,
        "address2": address2,
        "city": city,
        "state": state,
        "pin": pin,
        "cluster_id": cluster_id,
        "confidence_score": confidence_score,
        "FIELD_NAME": "NA",
    }
    print(dupes)
    return dupes




# def date_range(field_1, field_2):
#   dateOne = datetime.datetime.strptime(field_1 , '%Y-%m-%d')
#   dateTwo = datetime.datetime.strptime(field_2 , '%Y-%m-%d')
#   datediff = dateOne - dateTwo

#   if field_1 and field_2 :
#       if abs(datediff)>366 :
#           return 1
#       else:
#           return 0


def preProcess(column):
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    # column = column.strip().strip('"').strip("'").strip()

    if not column:
        column = None
    return column


def readData(filename):
    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            # print(row)
            clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
            row_id = (row['siteName'])  # This is the main identifier, to act as kind of key
            data_d[row_id] = dict(clean_row)
    return data_d


def get_input_file():
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as fp:
            content = json.load(fp)
            param_list = content["param_list"]
            output_format = content["output_format"]

        with open(sys.argv[2]) as json_data:
            data = json.load(json_data)
            print("\n\n\n\n=====================The received data===============\n\n",data)

        # Convert input data strings to Python Lists and Dictionaries
        param_list_dictionary = json.loads(param_list)
        output_format_dictionary = json.loads(output_format)
        cor_id = output_format_dictionary.get("CORRELATION_ID")

        with open('/tmp/site_ddp_dump.json', 'w') as f:
            json.dump(data, f)

    if len(sys.argv) < 2:  # this block will execute if rule file is run directly
        # file = open("../out_json_05252023_171152.json", "r", encoding='utf-8-sig')
        # input_response = json.load(file)
        # data = input_response["records"]
        cor_id = '1234'

    data_file = open(input_file, 'w')

    # create the csv writer object
    csv_writer = csv.writer(data_file)

    # Counter variable used for writing
    # headers to the CSV file
    count = 0

    for record in data:
        record['cor_id'] = cor_id
        if count == 0:
            # Writing headers of CSV file
            header = record.keys()
            csv_writer.writerow(header)
            count += 1

        # Writing data of CSV file
        csv_writer.writerow(record.values())

    data_file.close()

def process_csv_dedupe(file):
    optp = optparse.OptionParser()
    optp.add_option('-v', '--verbose', dest='verbose', action='count',
                    help='Increase verbosity (specify multiple times for more)'
                    )
    (opts, args) = optp.parse_args()
    log_level = logging.WARNING

    if opts.verbose:
        if opts.verbose == 1:
            log_level = logging.INFO
        elif opts.verbose >= 2:
            log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)

    print('importing data ...', '\n')

    # get_input_file()
    data_d = readData(input_file)
    # print(data_d)

    # print('\n',data_d,'\n')

    if os.path.exists(settings_file):
        # print('reading from', settings_file)
        with open(settings_file, 'rb') as f:
            deduper = dedupe.StaticDedupe(f)
    else:
        fields = [
            # {'field': 'siteName', 'type': 'String'},
            # {'field': 'status', 'type': 'Exact'},
            {'field': 'siteType', 'type': 'Exact'},
            # {'field': 'latitude', 'type': 'String'},
            # {'field': 'longitude', 'type': 'String'},
            {'field': 'address1', 'type': 'String'},
            {'field': 'address2', 'type': 'String'},
            {'field': 'city', 'type': 'String'},
            {'field': 'state', 'type': 'Exact'},
            # {'field': 'country', 'type': 'Exact'},
            {'field': 'pin', 'type': 'Exact'},
        ]

        deduper = dedupe.Dedupe(fields)

        if os.path.exists(training_file):
            print('reading labeled examples from ', training_file, ' \n')
            with open(training_file, 'rb') as f:
                deduper.prepare_training(data_d, f)
        else:
            deduper.prepare_training(data_d)

        print('starting active labeling...', '\n')
        # dedupe.consoleLabel(deduper)
        dedupe.console_label(deduper)

        deduper.train()

        with open(training_file, 'w') as tf:
            # deduper.writeTraining(tf)
            deduper.write_training(tf)

        with open(settings_file, 'wb') as sf:
            # deduper.writeSettings(sf)
            deduper.write_settings(sf)

    print('clustering...', '\n')
    # clustered_dupes = deduper.match(data_d, 0.5)
    # clustered_dupes = deduper.partition(data_d, 0.70)
    clustered_dupes = deduper.partition(data_d, 0.90)

    ####### for writing clustered dupes for testing ############
    with open(site_clustered_dupes, 'w', newline='\n') as clusd:
        csvwriter = csv.writer(clusd)
        csvwriter.writerows(clustered_dupes)
    ############################################################

    # print(f'clustered_dupes \n {clustered_dupes} ')

    # print('# duplicate sets', len(clustered_dupes))
    cluster_cnt = len(clustered_dupes) - 1

    cluster_membership = {}
    cluster_dict = defaultdict(int)
    # print(clustered_dupes)
    for cluster_id, (records, scores) in enumerate(clustered_dupes):
        # print(cluster_id, records, scores)
        # if cluster_dict[cluster_id]<1:
        #     cluster_dict[cluster_id]+=1
        # else:

        for record_id, score in zip(records, scores):
            cluster_membership[record_id] = {
                "Cluster_ID": int(cluster_id),
                "confidence_score": float(score)
            }

    with open(output_file, 'w', newline='') as f_output, open(input_file) as f_input:

        reader = csv.DictReader(f_input)
        fieldnames = ['Cluster_ID', 'confidence_score'] + reader.fieldnames

        writer = csv.DictWriter(f_output, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            row_id = (row['siteName'])  # change this as per key defined above
            try:
                row.update(cluster_membership[row_id])
                writer.writerow(row)

                # print(row)

            except KeyError:
                cluster_cnt = cluster_cnt + 1
                # cluster_membership.a[record_id] = {"Cluster ID": cluster_cnt,"confidence_score": 0 }

                row.update({"Cluster_ID": cluster_cnt, "confidence_score": 0})
                writer.writerow(row)

    result = []
    duplicate_cluster_sets = {}
    duplicates_customers = defaultdict(list)
    with open(output_file, 'r') as data:

        for row in csv.DictReader(data):
            duplicates_customers[row.get('Cluster_ID')].append(
                find_duplicates(
                    siteName=row.get('siteName'),
                    status=row.get('status'),
                    siteType=row.get('siteType'),
                    latitude=row.get('latitude'),
                    longitude=row.get('longitude'),
                    address1=row.get('address1'),
                    address2=row.get('address2'),
                    city=row.get('city'),
                    state=row.get('state'),
                    country=row.get('country'),
                    pin=row.get('pin'),
                    source=row.get('SOURCE'),
                    cluster_id=row.get('Cluster_ID'),
                    confidence_score=row.get('confidence_score')
                )
            )

    for key, val in duplicates_customers.items():
        if len(val) > 1:
            print(val)
            result += val

    duplicates_customers.clear()

    return result





@app.route('/tni/api/sendingCSVPath', methods=['POST'])
def handler_naf():
    '''
    Entry point for naf path
    '''
    start_time = datetime.now()
    paths = []
    response_data = []
    status = "Failed"
    message = ''
    status_code = 200
    input_exceptions = []

    # LOGGER.info('----Path task initiated-----')

    # try:
    request_data = request.json
    print("====================",request_data)
    input_csv = parseInput(request_data)
    result = process_csv_dedupe(input_csv)
    print(result)
    return jsonify(result)
        # inputs = parseInputs(request_data=request_data,
        #                      required_inputs=['nafFile', 'siteHumId', 'clli'])

    #     LOGGER.info(f'Inputs : {inputs}')
    #
    #     validateNafFilePath(naf_file_path=inputs["nafFile"])
    #
    #     LOGGER.info(F'Naf Path Validated')
    #
    #     naf_input_file = open(inputs["nafFile"], "rb")
    #
    #     # input_exceptions = []
    #
    #     paths, response_data = nafPath(clli=inputs["clli"].upper().strip(),
    #                                    site_id=inputs["siteHumId"].upper().strip(),
    #                                    naf_input=BytesIO(naf_input_file.read()),
    #                                    port_mapping=NAF_PORT_MAPPING,
    #                                    input_exceptions=input_exceptions)
    #
    #
    # except Exception as e:
    #     LOGGER.debug(f'exception: {e}')
    #     message, status_code = handleException(e)
    #     pass
    # else:
    #     status = 'OK'
    #
    #     LOGGER.info(
    #         "Returning {} records with status code {} for /naf request with input {} ".format(len(response_data),
    #                                                                                           status_code,
    #                                                                                           json.dumps(inputs,
    #                                                                                                      indent=4)))

    # return buildResponse(correlation_id=request_data["correlationId"],
    #                      start_time=start_time,
    #                      response_data=response_data,
    #                      status=status,
    #                      status_code=status_code,
    #                      message=message,
    #                      input_exceptions=input_exceptions)

if __name__ == '__main__':
    app.run()
