from elasticsearch import Elasticsearch
from openpyxl import load_workbook
from datetime import date
import re
import os
import logging
import requests


# these will not change.
INTERNAL_NOTE = 'It was possible to publish this article open access thanks ' \
                'to a Swiss National Licence with the Publisher.'

EMBARGOS = {
    'gruyter': 2,    # 2015
    'cambridge': 5,  # 2012
    'oxford': 3,     # 2014
    'springer': 5    # 2012
}


PUBLISHER_NORMALIZATIONS = {
    'Akademie Verlag GmbH': 'Akademie Verlag',
    'Blackwell Publishing Ltd': 'Blackwell',
    'Blackwell Science Ltd': 'Blackwell',
    'Blackwell Science Ltd, UK': 'Blackwell',
    'Blackwell Science, Ltd': 'Blackwell',
    'Elsevier B.V.': 'Elsevier',
    'Elsevier Science': 'Elsevier',
    'Elsevier Science B.V.': 'Elsevier',
    'OLDENBOURG WISSENSCHAFTSVERLAG': 'Oldenbourg',
    'Oldenbourg Wissenschaftsverlag GmbH': 'Oldenbourg',
    'R. Oldenbourg Verlag': 'Oldenbourg',
    'The University Chicago Press': 'The University of Chicago Press',
    'University Chicago Press': 'The University of Chicago Press',
    'Walter de Gruyter': 'De Gruyter',
    'Walter de Gruyter GmbH': 'De Gruyter',
    'Walter de Gruyter GmbH & Co. KG': 'De Gruyter',
    'Walter de Gruyter, Berlin / New York': 'De Gruyter'
}

ISSN_FIXES = {
    '14346621': '1434-6621',
    '943': '0943-8610',
    '3005577': '0300-5577',
    '16193997': '0300-5577'
}

EISSN_FIXES = {
    '-': '1756-2651',
    '14374331': '1437-4331',
    '16193997': '1619-3997',
    '3005577': '1619-3997',
}

"""edoc2es call: NationalLicenceEnricher(es=ES, logger=LOGGER, elastic_index=HOST,excel_path='data/unibas.xlsx',
                                                output_path='data_out/', download_pdfs=False)"""


class NationalLicenceEnricher:

    def __init__(self, excel_path='unibas.xlsx', es="", elastic_index='edoc-vmware',
                 elastic_url='http://localhost:9200', download_pdfs=True,
                 download_location='/opt/eprints3/archives/edoc/fulltext/nationallicences/',
                 pdf_location='/opt/eprints3/archives/edoc/fulltext/nationallicences/',
                 output_path='output/', logger=logging.getLogger('natlic')):
        self.download_pdfs = download_pdfs
        self.pdf_location = pdf_location
        self.download_location = download_location
        self.logger = logger
        self.output_path = output_path

        # list of items where the pdfs need to be checked manually.
        # self.wrong_documents = list()

        # items where either the doi or the internal note needs to be added.
        self.matched_items = dict()

        self.elastic_index = elastic_index
        if es:
            self.es = es
        else:
            self.es = Elasticsearch([elastic_url], timeout=300)

        # loads the excel sheet as work book and its first sheet.
        self.work_book = load_workbook(excel_path)
        self.sheet = self.work_book.active
        self.excel_data = self.load_data_from_excel()

        # compiles the lists matched items & wrong documents.
        self.compile_list()

        # save all the changes made to the sheet.
        self.work_book.save(excel_path)
        self.work_book.close()

    def enrich_edocdata(self, EdocLine):
        if EdocLine.line['eprintid'] in self.matched_items:
            record = self.matched_items[EdocLine.line['eprintid']]
            current = EdocLine.line
            # Add doi to ID numbers if not there already.
            if 'id_number' in current:
                has_doi = False
                for number in current['id_number']:
                    # checks if the doi is already there.
                    if number['type'] == 'doi' and number['id'] == record['doi']:
                        has_doi = True
                if not has_doi:
                    current['id_number'].append({'type': 'doi', 'id': record['doi']})
            else:
                current['id_number'] = list()
                current['id_number'].append({'type': 'doi', 'id': record['doi']})
            # Add note to field <Internal Note>
            if 'suggestions' in current:
                if not re.search(INTERNAL_NOTE, current['suggestions']):
                    current['suggestions'] = current['suggestions'] + ' -- ' + INTERNAL_NOTE
            else:
                current['suggestions'] = INTERNAL_NOTE

            # enrich additional information in all cases.
            if record['journal-title'] is not None:
                current['publication'] = record['journal-title']
            if record['issn'] is not None:
                if str(record['issn']) in ISSN_FIXES:
                    current['issn'] = ISSN_FIXES[record['issn']]
                else:
                    current['issn'] = record['issn']
            if record['e_issn'] is not None:
                if str(record['e_issn']) in EISSN_FIXES:
                    current['e_issn'] = EISSN_FIXES[record['e_issn']]
                else:
                    current['e_issn'] = record['e_issn']
            if record['publisher'] is not None:
                if record['publisher'] in PUBLISHER_NORMALIZATIONS:
                    current['publisher'] = PUBLISHER_NORMALIZATIONS[record['publisher']]
                else:
                    current['publisher'] = record['publisher']
            # mre: for easier filtering in ES to create update-XML-Files
            current['update_status'] = "fulltext"

    def compile_list(self):
        """
            Compiles the list of documents which can be imported into edoc.

        :return: @adjusted_record: excel data + eprintid, embargo
        """
        count = 1  # count of the rows in the excel table.
        for record in self.excel_data:
            count += 1
            has_match, match = self.compare_doi(record)
            if not has_match:
                has_match, match = self.compare_title_family_name(record)

            if has_match:
                adjusted_record = record
                adjusted_record['eprintid'] = match['eprintid']

                # update sheet with digi space save path <digispace-path>/<publisher-name>/<file-name>
                self.sheet['AC' + str(count)] = record['source'] + '/' + record['fulltext-url'].split('/')[-1]
                # stores the eprint id inside the excel sheet.
                self.sheet['AD' + str(count)] = match['eprintid']

                has_document = self.check_documents(record, match)
                adjusted_record['has_document'] = has_document

                # When no adequate document was found check if it has an embargo and set it.
                if not has_document:
                    adjusted_record = self.set_embargos(record, match)

                    # lists for importing either with or without embargo
                    with open(self.output_path + date.today().isoformat() + '-edoc-import.txt', 'a',
                              encoding='utf-8') as file:
                        file.write(str(adjusted_record['eprintid']) + '|'
                                   + adjusted_record['security'] + '|'
                                   + adjusted_record['content'] + '|'
                                   + str(adjusted_record['embargo_date']) + '|'
                                   + adjusted_record['local-path'] +  # '|'
                                   # + record['doi'] + used for tests only.
                                   '\n')

                # only enrich this document if the document is missing or the internal note has not been added yet.
                if not re.search(INTERNAL_NOTE, record.get('suggestions', '')) or not has_document:
                    if self.download_pdfs:
                        self.download_pdf(record)
                    self.matched_items[adjusted_record['eprintid']] = adjusted_record

    def set_embargos(self, record, match):
        """
        Adds the embargo date, security option and content to the record.

        :param record:  excel data record
        :param match:   elastic match result
        :return:        Return expanded record.
        """
        publisher = record['source']
        record['local-path'] = self.pdf_location + record['source'] + '/' + record['fulltext-url'].split('/')[-1]
        this_year = date.today().year
        publish_date = int(record['publish-date'])

        record['content'] = 'published'
        record['embargo_date'] = publish_date + EMBARGOS[publisher]

        # publication has to be from before (current year - years of embargo)
        if record['embargo_date'] > this_year:
            record['security'] = 'staffonly'
        else:
            record['security'] = 'public'

        self.logger.info('Item ' + str(match['eprintid']) + ' can be imported with embargo until %s',
                         str(record['embargo_date']))
        return record

    def check_documents(self, record, match) -> bool:
        """
            Checks if the match has a document attached.

        :return:    True -> Document attached
                    False -> No document attached
        """
        if 'documents' in match:
            documents = match['documents']
            self.logger.info('Edoc entry ' + str(match['eprintid']) + ' has already a pdf document.')
            for d in documents:
                # We are only interested when a document has the mime-type pdf.
                # ONLY THE FIRST PDF IS CONSIDERED.
                if d['mime_type'] == 'application/pdf':
                    save_doc = dict()
                    security = d['security']
                    if 'content' in d:
                        content = d['content']
                    else:
                        content = 'UNSPECIFIED'

                    # Public/Published documents are ok.
                    if security == 'public' and content == 'published':
                        return True
                    # Staffonly/Published documents are ok (with Embargo).
                    elif security == 'staffonly' and content == 'published':
                        return True
                    # Otherwise the document can be improved
                    else:
                        save_doc['eprintid'] = match['eprintid']
                        save_doc['doi'] = record['doi']
                        save_doc['fulltext-link'] = record['fulltext-url']
                        save_doc['security'] = security
                        save_doc['content'] = content
                        self.logger.warning('Edoc entry ' + str(match['eprintid']) +
                                            ' has a pdf document which will be replaced.')
                        return False
            return False
        else:
            return False

    def compare_doi(self, record: dict) -> tuple:
        """
            Searches for edoc entries with an exact doi match.

        :param record: a excel data record.
        :return: tuple
                            bool,           -> True for single match, False otherwise
                            result source   -> The source dict of the matched document or None.

        """
        doi_query = {"query": {"bool": {"must": {"match": {"id_number.id.keyword": record['doi']}}}}}
        es_response = self.es.search(body=doi_query, index=self.elastic_index)
        if es_response['hits']['total'] == 1:
            # A single match was found. Forward for further processing.
            self.logger.info('Found match: %s, %s.', es_response['hits']['hits'][0]['_source']['eprintid'],
                             record['doi'])
            return True, es_response['hits']['hits'][0]['_source']
        elif es_response['hits']['total'] > 1:
            # Several matches were found. These are most likely duplicates in edoc and need to be resolved manually.
            # The logging is emailed to fodaba@unibas.ch
            eprint_id_list = [item['_source']['eprintid'] for item in es_response['hits']['hits']]
            self.logger.critical('Found several entries for doi %s. Cannot import with several hits. ' +
                                 'Eprint IDs: %s', record['doi'], eprint_id_list)
            return False, None
        else:
            # No match was found.
            return False, None

    def compare_title_family_name(self, record) -> tuple:
        """
            Searches for edoc entries based on title, family-names

            As a fall back should edoc not have a doi stored.

        :param record: a excel data record.
        :return: True when a match was found, False otherwise.
        """

        title_author_query = {"query": {"bool": {"must": [
            {"match": {"title": {"query": record['title'], "operator": "AND"}}},
            {"match": {"creators.name.family": {"query": record['family-names'], "operator": "OR"}}}
        ]}}}
        es_response = self.es.search(body=title_author_query, index=self.elastic_index)
        if es_response['hits']['total'] == 1:
            # A single match was found. Forward for further processing.
            self.logger.info(
                'Found match in edoc based on title & authors: ' +
                str(es_response['hits']['hits'][0]['_source']['eprintid']))
            return True, es_response['hits']['hits'][0]['_source']
        elif es_response['hits']['total'] > 1:
            # Several matches were found. These are most likely duplicates in edoc and need to be resolved manually.
            # The logging is emailed to fodaba@unibas.ch
            eprint_id_list = [item["_source"]['eprintid'] for item in es_response['hits']['hits']]
            self.logger.critical('Found several entries for titel ' + record['title'] + '. ' +
                                 'This issue needs to be resolved before full texts can be imported.\n\n' +
                                 'Eprints IDs: ' + str(eprint_id_list))
            return False, None
        else:
            # no match - this item will be ignored.
            return False, None

    def download_pdf(self, record):
        path = self.download_location + record['source'] + '/' + record['fulltext-url'].split('/')[-1]
        if not os.path.isfile(path):
            try:
                response = requests.get(record['fulltext-url'])
            except requests.exceptions.RequestException:
                self.logger.exception('Could not download pdf from: ' + record['fulltext-url'])
            else:
                with open(path, 'wb') as file:
                    file.write(response.content)
            self.logger.info('Downloaded full text for ' + record['doi'] + '. ' +
                             'Saved file in ' + path)

    def load_data_from_excel(self):
        """
        Loads all the relevant fields from the unibas.xlsx file into a list of dictionaries

        Do not change this.

        Requires authors to be divided by semi-colon and names divided by comma.

        :return: all_data: A list of dictionaries with keys:
                    -> doi, url-doi, fulltext-url, title, family-names, publish-date, publisher
        """
        all_data = list()
        for row in self.sheet:
            # ignore the first line...
            if row[3].value == 'doi':
                continue
            element = dict()
            element['doi'] = row[3].value
            element['url-doi'] = row[4].value
            element['fulltext-url'] = row[5].value
            element['title'] = row[6].value

            # stores family names of authors.
            element['family-names'] = ''

            # Requires authors to be divided by semi-colon and names divided by comma.
            authors = row[8].value.split(';')
            for author in authors:
                family = author.split(',')[0]
                element['family-names'] += family.strip() + ' '

            # data for enrichment of edoc records.
            element['journal-title'] = row[10].value
            element['publisher'] = row[12].value  # publisher listed in citation.
            element['issn'] = row[18].value
            element['e_issn'] = row[19].value

            # date of publication to determine embargo
            element['publish-date'] = row[9].value

            # source publisher -> determine embargo
            element['source'] = row[22].value
            # element['comment'] = row[27].value # comment for internal note. Currently imported statically.
            all_data.append(element)

        return all_data


if __name__ == '__main__':
    logging.basicConfig(filename='output/logs/national-licence-enrichment-' + date.today().isoformat() + '.log',
                        level=logging.WARNING, filemode='w')

    NationalLicenceEnricher(pdf_location='output/pdfs/', download_location='output/pdfs/')
