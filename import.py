import logging
import datetime	
#import yaml
import csv
import re

# Logger config
log_format = '[%(asctime)s] [%(levelname)s] %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_format)

# Date 
date = datetime.datetime.now()
date_script = str(date.strftime("%y")) + str(date.strftime("%m")) + str(date.strftime("%d"))

# Global variables
inputPath = 'inputs/'
outputPath = 'outputs/'
inputFile = inputPath + 'dataresource_input.csv'
outputFile = outputPath + '1_inpn_insert_' + date_script + '.sql'

# Get data functions
def get_institution(row, institutions, sequences):
    institution = next((x for x in institutions if x['code'] == row[2]), None)
    if institution is None:
        seq = len(institutions) + 1
        institution = {
            'code': row[2],
            'id': str(seq),
            'uid': 'in' + str(seq)
        }
        sequences['institution'] = seq
        institutions.append(institution)


def get_collection(row, collections, sequences):
    collection = next((x for x in collections if x['code'] == row[3]), None)
    if collection is None:
        seq = len(collections) + 1
        collection = {
            'code': row[3],
            'id': str(seq),
            'uid': 'co' + str(seq),
            'institution_code': row[2].replace("'", "''")
        }
        sequences['collection'] = seq
        collections.append(collection)

def get_dataset(row, datasets, sequences):
    dataset = next((x for x in datasets if x['id'] == row[0]), None)
    if dataset is None:
        seq = len(datasets) + 1
        dataset = {
            'id': row[0],
            'name': row[1],
            'uid': 'dr' + str(seq),
            'filename': row[3],
            'institution_code': row[2].replace("'", "''")
        }
        sequences['dataResource'] = seq
        datasets.append(dataset)

def get_datalink(datalinks, collections, institutions, datasets, sequences):
    seq1 = sequences['datalink'] + 1
    seq2 = seq1 +1
    datalink = {
        'dr': datasets[-1]['uid'],     # accéder au dernier élément
        'id_inst': str(seq1),
        'inst': institutions[-1]['uid'],  # accéder au dernier élément
        'id_col': str(seq2),
        'col': collections[-1]['uid']  # accéder au dernier élément
    }
    sequences['datalink'] = seq2
    datalinks.append(datalink)

def get_providermap(providermaps, collections, institutions, sequences):
    seq = len(providermaps) + 1
    providermap = {
        'id': str(seq),
        'collection_code_uid': collections[-1]['id'],     # accéder au dernier élément
        'institution_code_uid': institutions[-1]['id']  # accéder au dernier élément
    }
    sequences['providermap'] = seq
    providermaps.append(providermap)

def get_providercode_c(row, providercodes, sequences):
    providercode = next((x for x in providercodes if x['code'] == row[3]), None)
    if providercode is None:
        seq = len(providercodes) + 1
        providercode = {
            'code': row[3],
            'id': str(seq)
        }
    sequences['providercode'] = seq
    providercodes.append(providercode)


def get_providercode_i(row, providercodes, sequences):
    providercode = next((x for x in providercodes if x['code'] == row[2].replace("'", "''")), None)
    if providercode is None:
        seq = len(providercodes) + 1
        providercode = {
            'code': row[2].replace("'", "''"),
            'id': str(seq),
        }
        sequences['providercode'] = seq
        providercodes.append(providercode)

def get_providercam(row, providercams, providercodes):
    seq = len(providercams) + 1
    collection_codes_id = next((x for x in providercodes if x['code'] == row[3]), None)
    institution_codes_id = next((x for x in providercodes if x['code'] == row[2].replace("'", "''")), None)
    providercam = {
        'provider_code_id': str(seq),
        'provider_codes_id_collection': collection_codes_id['id'],
        'provider_codes_id_institution': institution_codes_id['id']
    }
    providercams.append(providercam)    


# Generate SQL functions
def generate_institutions_sql(institutions):
    insert_tpl = 'INSERT INTO institution (version, date_created, last_updated, latitude, longitude, isalapartner, name, user_last_modified, uid, attributions) VALUES (0, NOW(), NOW(), -1, -1, 0, \'{}\', \'not available\', \'{}\', \'\');\n'
    output_file = outputFile

    with open(output_file, 'a') as sqlfile:
        for institution in institutions:
            sqlfile.write(insert_tpl.format(institution['code'].replace("'", "''"), institution['uid']))

    logging.info('File generated: {}'.format(output_file))


def generate_collections_sql(collections):
    insert_tpl = 'INSERT INTO collection (version, date_created, east_coordinate, isalapartner, last_updated, latitude, longitude, name, north_coordinate, num_records, num_records_digitised, south_coordinate, uid, user_last_modified, west_coordinate, attributions, institution_id) VALUES ( 1, NOW(), -1, 0, NOW(), -1, -1, \'{}\', -1, -1, -1, -1, \'{}\', \'not available\', -1, \'\', (SELECT id FROM institution WHERE name LIKE \'{}\' COLLATE utf8_bin));\n'
    output_file = outputFile

    with open(output_file, 'a') as sqlfile:
        for collection in collections:
            sqlfile.write(insert_tpl.format(collection['code'].replace("'", "''"), collection['uid'], collection['institution_code']))

    logging.info('File generated: {}'.format(output_file))

def generate_datalink_sql(datalinks):
    insert_tpl = 'INSERT INTO data_link (id, version, consumer, provider) VALUES ( \'{}\', 0, \'{}\', \'{}\');\n'
    output_file = outputFile

    with open(output_file, 'a') as sqlfile:
        for datalink in datalinks:
            sqlfile.write(insert_tpl.format(datalink['id_inst'], datalink['inst'], datalink['dr']))
            sqlfile.write(insert_tpl.format(datalink['id_col'], datalink['col'], datalink['dr']))
    logging.info('File generated: {}'.format(output_file))

def generate_datasets_sql(datasets):
    insert_tpl = 'INSERT INTO data_resource (version, date_created, download_limit, filed, gbif_dataset, harvest_frequency, isalapartner, is_shareable_withgbif, last_updated, latitude, longitude, make_contact_public, name, public_archive_available, resource_type, risk_assessment, status, uid, user_last_modified, attributions, connection_parameters, institution_id) VALUES (1, NOW(), 0, 0, 0, 0, 0, 1, NOW(), -1, -1, 1, \'{}\', 0, \'records\', 0, \'identified\', \'{}\', \'not available\', \'\', \'{{"url":"file:////data/ala-collectory/upload/manual/{}.zip","automation":false,"termsForUniqueKey":["occurrenceID"],"strip":false,"incremental":false,"protocol":"DwCA"}}\', (SELECT id FROM institution WHERE name LIKE \'{}\' COLLATE utf8_bin));\n'
    output_file = outputFile

    with open(output_file, 'a') as sqlfile:
        for dataset in datasets:
            sqlfile.write(insert_tpl.format(dataset['name'].replace("'", "''"), dataset['uid'], dataset['filename'], dataset['institution_code']))

    logging.info('File generated: {}'.format(output_file))

def generate_providermap_sql(providermaps):
    insert_tpl = 'INSERT INTO provider_map (id, version, exact, match_any_collection_code, last_updated, date_created, collection_id, institution_id, warning) VALUES (\'{}\', 0, \'\', \'\', NOW(), NOW(), \'{}\', \'{}\', \'NULL\');\n'
    output_file = outputFile

    with open(output_file, 'a') as sqlfile:
        for providermap in providermaps:
            sqlfile.write(insert_tpl.format(providermap['id'], providermap['collection_code_uid'], providermap['institution_code_uid']))
    logging.info('File generated: {}'.format(output_file))

def generate_providercode_sql(providercodes):
    insert_tpl = 'INSERT INTO provider_code (id, version, code) VALUES (\'{}\', 0, \'{}\');\n'
    output_file = outputFile

    with open(output_file, 'a') as sqlfile:
        for providercode in providercodes:
            sqlfile.write(insert_tpl.format(providercode['id'], providercode['code']))
    logging.info('File generated: {}'.format(output_file))

def generate_providercam_sql(providercams):
    insert_tpl_1 = 'INSERT INTO provider_map_provider_code (provider_map_collection_codes_id, provider_code_id, provider_map_institution_codes_id) VALUES (\'{}\', \'{}\', {});\n'
    insert_tpl_2 = 'INSERT INTO provider_map_provider_code (provider_map_collection_codes_id, provider_code_id, provider_map_institution_codes_id) VALUES ({}, \'{}\', \'{}\');\n'
    output_file = outputFile

    with open(output_file, 'a') as sqlfile:
        for providercam in providercams:
            sqlfile.write(insert_tpl_1.format(providercam['provider_code_id'], providercam['provider_codes_id_collection'], 'NULL'))
            sqlfile.write(insert_tpl_2.format('NULL', providercam['provider_codes_id_institution'], providercam['provider_code_id']))
    logging.info('File generated: {}'.format(output_file))
    
def generate_sequences_sql(sequences):
    update_sql = 'UPDATE sequence SET next_id = {} WHERE name = \'{}\';\n'
    output_file = outputFile

    with open(output_file, 'a') as sqlfile:
        sqlfile.write(update_sql.format((sequences['collection'] + 1), 'collection'))
        sqlfile.write(update_sql.format((sequences['institution'] + 1), 'institution'))
        sqlfile.write(update_sql.format((sequences['collection'] + 1), 'dataResource'))
        sqlfile.write(update_sql.format((sequences['datalink'] + 1), 'datalink'))
        sqlfile.write(update_sql.format((sequences['providermap'] + 1), 'providermap'))
        sqlfile.write(update_sql.format((sequences['providercode'] + 1), 'providercode'))
    logging.info('Sequence updated: {}'.format(output_file))

def initiate_sql():
    initiate_sql = 'SET FOREIGN_KEY_CHECKS=0;\nTRUNCATE provider_map_provider_code;\nTRUNCATE provider_code;\nTRUNCATE provider_map;\nTRUNCATE data_resource;\nTRUNCATE collection;\nTRUNCATE institution;\nTRUNCATE data_link;\nALTER TABLE provider_code MODIFY COLUMN code VARCHAR (500);\nSET FOREIGN_KEY_CHECKS=1;\n'
    output_file = outputFile

    with open(output_file, 'a') as sqlfile:
        sqlfile.write(initiate_sql.format())
    logging.info('File initialized: {}'.format(output_file))

# general fuction
def import_file(filepath):
    sequences = { 'collection': 0, 'institution': 0, 'dataResource': 0, 'datalink': 0, 'providermap': 0, 'providercode': 0}
    institutions = []
    collections = []
    datasets = []
    datalinks = []
    providermaps = []
    providercodes = []
    providercams = []

    with open(filepath, newline='') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=';')
        next(csv_reader, None)  # skip the headers
        csv_rows = list(csv_reader)

        for row in csv_rows:
            get_institution(row, institutions, sequences)
            get_collection(row, collections, sequences)
            get_dataset(row, datasets, sequences)
            get_datalink(datalinks, collections, institutions, datasets, sequences)
            get_providermap(providermaps, collections, institutions, sequences)
            get_providercode_i(row, providercodes, sequences)
            get_providercode_c(row, providercodes, sequences)
            get_providercam(row, providercams, providercodes)

        logging.info('Institutions imported: {}'.format(len(institutions)))
        logging.info('Collections imported: {}'.format(len(collections)))
        logging.info('Datasets imported: {}'.format(len(datasets)))
        logging.info('Datalink done: {}'.format(len(datalinks)))
        logging.info('Providermap imported: {}'.format(len(providermaps)))
        logging.info('Providercode imported: {}'.format(len(providercodes)))
        logging.info('Providercam imported: {}'.format(len(providercams)))

    file = open(outputFile, "w")
    file.close()
    initiate_sql()
    generate_institutions_sql(institutions)
    generate_collections_sql(collections)
    generate_datasets_sql(datasets)
    generate_datalink_sql(datalinks)
    generate_providermap_sql(providermaps)
    generate_providercode_sql(providercodes)
    generate_providercam_sql(providercams)
    generate_sequences_sql(sequences)


def main():
    logging.info('Start import')

    import_file(inputFile)

    logging.info('End import')


if __name__ == "__main__":
    main()
