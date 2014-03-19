#!/usr/bin/env python

import argparse
from couchdb.client import Server
import datetime
import csv
import operator
import sys


def _flatten_items(items, sep, prefix):
    _items = []
    for key, value in items:
        _prefix = "{}{}".format(prefix, key)
        if isinstance(value, list):
            _items.extend(_flatten_items(list(enumerate(value)), sep=sep,
                          prefix=_prefix+sep))
        elif isinstance(value, dict):
            _items.extend(_flatten_items(value.items(), sep=sep,
                          prefix=_prefix+sep))
        else:
            _items.append((_prefix, value))
    return _items


def flatten_dict(d, sep='>'):
    return dict(_flatten_items(d.items(), sep=sep, prefix=""))


def days_hours_minutes(td):
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return '{0}:{1}:{2}:{3}'.format(td.days, hours, minutes, seconds)


def analyze_scorecard_times(uid, serial, command, ma, version, start, stages):
    row = []
    row.append(uid)
    row.append(serial)
    row.append(command)
    row.append(ma)
    row.append(version)
    row.append(start)
    stage_types = {}
    max_program_lebs = datetime.timedelta()
    max_erase_lebs = datetime.timedelta()
    max_read_lebs = datetime.timedelta()
    for stage in stages:
        if 'name' not in stage or 'start' not in stage or 'end' not in stage:
            print '{0} does not have an end time for one of it\'s ' \
                'stages'.format(uid)
            continue
        name = stage['name']
        start_time = datetime.datetime.strptime(
            stage['start'], '%Y/%m/%d %H:%M:%S UTC')
        end_time = datetime.datetime.strptime(
            stage['end'], '%Y/%m/%d %H:%M:%S UTC')
        stage_delta = end_time - start_time
        if name == 'program lebs' and stage_delta > max_program_lebs:
            max_program_lebs = stage_delta
        if name == 'erase lebs' and stage_delta > max_erase_lebs:
            max_erase_lebs = stage_delta
        if name == 'read lebs' and stage_delta > max_read_lebs:
            max_read_lebs = stage_delta
        if name not in stage_types.keys():
            stage_types[name] = [stage_delta, 1]
        else:
            delta, count = stage_types[name]
            stage_types[name][0] = stage_types[name][0] + stage_delta
            stage_types[name][1] += 1
    total_test_time = datetime.timedelta()
    p_lebs_avg = 'NA'
    e_lebs_avg = 'NA'
    r_lebs_avg = 'NA'
    for k, v in stage_types.items():
        if k != 'program lebs' or k != 'erase lebs' or k != 'read lebs':
            total_test_time += v[0]
        if k == 'program lebs':
            if v[1] == 1:
                p_lebs_avg = v[0]
            else:
                p_lebs_avg = v[0]/v[1]
        if k == 'erase lebs':
            if v[1] == 1:
                e_lebs_avg = v[0]
            else:
                e_lebs_avg = v[0]/v[1]
        if k == 'read lebs':
            if v[1] == 1:
                r_lebs_avg = v[0]
            else:
                r_lebs_avg = v[0]/v[1]

    row.append(days_hours_minutes(total_test_time))
    if command == 'Provision':
        return row
    zero_td = datetime.timedelta()
    if max_program_lebs == zero_td or max_erase_lebs == zero_td or \
            max_read_lebs == zero_td or p_lebs_avg == 'NA' or \
            e_lebs_avg == 'NA' or r_lebs_avg == 'NA':
        return []
    else:
        max_additional_time_for_pe_cycle = (max_program_lebs * 20) \
            + (max_erase_lebs * 20) + (max_read_lebs * 20)
        if p_lebs_avg != 'NA' and e_lebs_avg != 'NA' and r_lebs_avg != 'NA':
            avg_additional_time_for_pe_cycle = (p_lebs_avg * 20) + \
                (e_lebs_avg * 20) + (r_lebs_avg * 20)
        else:
            avg_additional_time_for_pe_cycle = 'NA'
        row.append(days_hours_minutes(max_program_lebs)
                   if max_program_lebs != datetime.timedelta() else 'NA')
        row.append(days_hours_minutes(max_erase_lebs)
                   if max_erase_lebs != datetime.timedelta() else 'NA')
        row.append(days_hours_minutes(max_read_lebs)
                   if max_read_lebs != datetime.timedelta() else 'NA')
        row.append(days_hours_minutes(total_test_time +
                   max_additional_time_for_pe_cycle))
        row.append(p_lebs_avg)
        row.append(e_lebs_avg)
        row.append(r_lebs_avg)
        if avg_additional_time_for_pe_cycle == 'NA':
            row.append('NA')
        else:
            row.append(days_hours_minutes(total_test_time +
                       avg_additional_time_for_pe_cycle))

    return row


def get_watts(amps, volts):
    return amps * volts


def analyze_watts(doc):
    row = []
    total_max_watts = 0.0
    total_mean_watts = 0.0
    total_median_watts = 0.0
    if 'sensors' not in doc or doc['disposition'] == 'failed' or \
            doc['disposition'] == 'incomplete':
        return row
    try:
        if doc['mcd']['boards'][0]['pmp_nodes'][0]['capabilities']['type'] \
                != 0:
            print 'PMP node 1 is not 0 for document {0}'.format(doc['_id'])
            return row
    except:
        print '{0} can not get pmp node'.format(doc['_id'])

    try:
        voltage_info = doc['sensors']['1']['29']
        amperage_info = doc['sensors']['1']['30']
        if voltage_info['name'] != '12.0V' and voltage['units'] != 'volts':
            print 'Sensor is not 29 for doc id {0}'.format(doc['_id'])
            return row
        if amperage_info['name'] != '12.0V' and amperage['units'] != 'amps':
            print 'Sensor is not 30 for doc id {0}'.format(doc['_id'])
            return row
        total_max_watts = get_watts(amperage_info['max'], voltage_info['max'])
        total_mean_watts = get_watts(amperage_info['mean'],
                                     voltage_info['mean'])
        total_median_watts = get_watts(amperage_info['median'],
                                       voltage_info['median'])
        voltage_samples = voltage_info['samples']
        amperage_samples = amperage_info['samples']

    except:
        try:
            voltage_info = doc['sensors']['power']['1']['29']
            amperage_info = doc['sensors']['power']['1']['30']
            if voltage_info['name'] != '12.0V' and voltage['units'] != 'volts':
                print 'Sensor is not 29 for doc id {0}'.format(doc['_id'])
                return row
            if amperage_info['name'] != '12.0V' and \
                    amperage['units'] != 'amps':
                print 'Sensor is not 30 for doc id {0}'.format(doc['_id'])
                return row
            total_max_watts = get_watts(amperage_info['max'],
                                        voltage_info['max'])
            total_mean_watts = get_watts(amperage_info['mean'],
                                         voltage_info['mean'])
            total_median_watts = get_watts(amperage_info['median'],
                                           voltage_info['median'])
            voltage_samples = voltage_info['samples']
            amperage_samples = amperage_info['samples']
        except:
            print 'Could not get data for doc id {0}'.format(doc['_id'])
            return row

    end = datetime.datetime.strptime(doc['end'],
                                     '%Y/%m/%d %H:%M:%S UTC')
    start = datetime.datetime.strptime(doc['start'],
                                       '%Y/%m/%d %H:%M:%S UTC')
    delta = end - start
    test_time = days_hours_minutes(delta)

    row.append(doc['_id'])
    row.append(doc['serial'])
    row.append(doc['command'])
    row.append(doc['mcd']['pn_ma'] if 'mcd' in doc else 'NA')
    row.append(doc['sugarplum_version'])
    row.append(doc['fixture'])
    row.append(doc['slot'])
    row.append(test_time)
    row.append(total_max_watts)
    row.append(total_mean_watts)
    row.append(total_median_watts)
    row.append(voltage_samples)
    row.append(amperage_samples)
    return row


def main():
    parser = argparse.ArgumentParser(
        description='Inspect and download results from CouchDB')
    parser.add_argument(
        '--db-uri', default='http://<yourserver>:5984/')
    parser.add_argument(
        '--db-name', default='sugarplum')
    parser.add_argument(
        '--max-depth', default=2, type=int,
        help='The maximum depth you wish to traverse in the json document.  '
             'If 0 the entire document will be parsed')
    parser.add_argument(
        '--score-time', default=False, action='store_true')
    parser.add_argument(
        '--analyze-watts', default=False, action='store_true')
    parser.add_argument(
        'serials', metavar='SN', type=str, nargs='+',
        help='Find results for the provided serial numbers.')
    args = parser.parse_args()

    header_types = {}
    server = Server(args.db_uri)
    sp_resultsdb = server[args.db_name]
    flattened_list = []
    csv_files = []
    if args.score_time:
        acsvfile = open('score_time.csv', 'wb')
        awriter = csv.writer(acsvfile)
        awriter.writerow(['_uid', 'serial', 'command', 'MA',
                          'sugarplum_version', 'start time',
                          'test time without P/E cycles',
                          'max_program_lebs', 'max_erase_lebs',
                          'max_read_lebs',
                          'MAX total_test_time with 20 PE cycles',
                          'avg_program_lebs', 'avg_erase_lebs',
                          'avg_read_lebs',
                          'AVG total_test_time with 20 PE cycles'])
    if args.analyze_watts:
        acsvfile = open('analyze_watts.csv', 'wb')
        awriter = csv.writer(acsvfile)
        awriter.writerow(['id', 'serial', 'command', 'MA',
                         'sugarplum_version', 'fixture', 'slot',
                         'test time(Day:Hours:Minutes:Seconds)', 'max_watts',
                         'mean_watts', 'median_watts', 'voltage samples',
                         'amperage samples'])

    for serial in args.serials:
        matches = sp_resultsdb.view('sugarplum/byserial', None,
                                    startkey=[serial, u''],
                                    endkey=[serial, u'\ufff0'],
                                    include_docs='True')
        for match in matches:
            if args.score_time:
                if 'mcd' not in match.doc:
                    continue
                tmp = analyze_scorecard_times(match.doc['_id'],
                                              match.doc['serial'],
                                              match.doc['command'],
                                              match.doc['mcd']['pn_ma'],
                                              match.doc['sugarplum_version'],
                                              match.doc['start'],
                                              match.doc['stages'])
                if tmp:
                    awriter.writerow(tmp)
                continue
            if args.analyze_watts:
                tmp = analyze_watts(match.doc)
                if tmp:
                    awriter.writerow(tmp)
                continue
            flattened = flatten_dict(match)
            flattened_list.append(flattened)
            csv_files.append('_'.join([flattened['doc>command'],
                             str(flattened['doc>schema_version'])]))
    if args.score_time:
        return 0

    for csv_file in csv_files:
        write_header = True
        command, schema = csv_file.split('_')
        with open(csv_file + '.csv', 'wb') as csvfile:
            csv_writer = csv.writer(csvfile)
            for flattened in flattened_list:
                if flattened['doc>command'] == command and \
                        str(flattened['doc>schema_version']) == schema:
                    if args.max_depth > 0:
                        for key in flattened.keys():
                            if key.count('>') > args.max_depth:
                                del flattened[key]
                    else:
                        if write_header:
                            csv_writer.writerow(flattened.keys())
                            write_header = False
                        csv_writer.writerow(flattened.values())

if __name__ == '__main__':
    sys.exit(main())
