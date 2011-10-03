# -*- coding: utf-8 -*-

'''
Created on 10-08-2011

@author: Bartek
'''

import csv
import os

from data_wrapper import CsvFile, CsvData, Data
from csv_mapper.mapper import HeaderTransformer
from csv_mapper.mapper import MappingsSet
from decoders import TerytDecoder, JSTDecoder, ParDecoder


        

class Decoder:
    def __init__(self, filename):
        self.file = open(filename, 'rb')
        self.dzialy = {}
        self.rozdzialy = {}
        self.paragrafy = {}
        self.cyfry = {}
        self.state = ''
        self.build()
        self.undecoded = {}
        
    def __del__(self):
        self.file.close()
        
    def starts_with_number(self, line):
        first = line.partition(' ')[0]
        return first.isdigit()
    
    def is_empty(self, line):
        first = line.partition(' ')[0]
        return first == ''
    
    def is_separator(self, line):
        return line in ['D:', 'R:', 'P:', 'C:']
    
    def to_next_state(self):
        if self.state == '':
            self.state = 'D'
        elif self.state == 'D':
            self.state = 'R'
        elif self.state == 'R':
            self.state = 'C'
        elif self.state == 'C':
            self.state = 'P'
        else:
            raise RuntimeError
        
    def try_to_add(self, lines):
        if len(lines) == 0:
            return
        number = lines[0].partition(' ')[0]
        code = ' '.join(lines).partition(' ')[2]
        self.add(number, code)
        
    def add(self, key, value):
        if self.state == 'D':
            self.dzialy[key] = value
        elif self.state == 'R':
            self.rozdzialy[key] = value
        elif self.state == 'C':
            self.cyfry[key] = value
        elif self.state == 'P':
            self.paragrafy[key] = value
        else:
            raise RuntimeError
        
    def build(self):
        last_lines = []
        for row in self.file:
            row = row.rpartition('\r\n')[0]
            if self.is_empty(row) and len(last_lines) > 0:
                self.try_to_add(last_lines)
                last_lines = []
            elif self.starts_with_number(row):
                self.try_to_add(last_lines)
                last_lines = [row]
            elif self.is_separator(row):
                self.to_next_state()
            else:
                last_lines.append(row)
        
        # add last line
        self.try_to_add(last_lines)
                
    def print_collection(self, coll_dict):
        keys = []
        for key in coll_dict.iterkeys():
            keys.append(key)
        keys.sort()
        for key in keys:
            print key, '->', coll_dict[key]
        
    def show(self):
        print 'Dzialy:'
        self.print_collection(self.dzialy)
        print 'Rozdzialy:'
        self.print_collection(self.rozdzialy)
        print 'Cyfry:'
        self.print_collection(self.cyfry)
        print 'Paragrafy:'
        self.print_collection(self.paragrafy)
        
    def decode(self, key, coll):
        try:
            if coll == u'Dział klasyfikacji budżetowej':
                return self.dzialy[key]
            elif coll == u'Rozdział klasyfikacji budżetowej':
                return self.rozdzialy[key]
            elif coll == u'Finansowanie paragrafu - 4 cyfra paragrafu':
                return self.cyfry[key]
            elif coll == u'Paragraf klasyfikacji budżetowej':
                return self.paragrafy[key]
            else:
                return None
        except KeyError:
            self.undecoded[key + coll] = True
            return None
            
        
    def get_decoded_fields(self):
        return [
            u'Dział klasyfikacji budżetowej',
            u'Rozdział klasyfikacji budżetowej',
            u'Finansowanie paragrafu - 4 cyfra paragrafu',
            u'Paragraf klasyfikacji budżetowej'
        ]
    
    def get_undecoded_values(self):
        undecoded_list = []
        for key in self.undecoded.iterkeys():
            undecoded_list.append(key)
        return undecoded_list
    
    def clean_undecoded(self):
        self.undecoded = {}
        

class CsvTransformer:
    def __init__(self, csvdata, decoder, header_transformer):
        self.data = csvdata
        self.decoder = decoder
        self.htransformer = header_transformer
        self.build_trans_list()
        
    def build_trans_list(self):
        self.header_list = []
        for key in self.data.get_header():
            self.header_list.append(key)
            
    def get_transformed_header(self):
        transformed_header = []
        fname = self.htransformer.get_filename_key(self.data.get_filename())
        for key in self.data.get_header():
            replaced_key = self.htransformer.replace(key, fname)
            transformed_header.append(replaced_key)
        return transformed_header
        
            
    def transform_row(self, row):
        for col in self.decoder.get_decoded_fields():
            if col in row:
                decoded_value = self.decoder.decode(row[col], col) 
                if decoded_value is not None:
                    row[col] = decoded_value
        return row
    
    def transform_row_to_list(self, row):
        list = []
        for col in self.header_list:
            list.append(row[col])
        return list
    
    def transform_file(self, new_filename):
        self.data.build()
        rows = self.data.get_rows()
        new_rows = []
        header = self.get_transformed_header()
        i = 0
        for row in rows:
            #print i
            i += 1
            transformed_row = self.transform_row(row)
            new_rows.append(self.transform_row_to_list(transformed_row))
        
        new_file = open(new_filename, 'w')
        new_file.write(';'.join(header) + '\n')
        
        i = 0
        for new_row in new_rows:
            row = [str(field) for field in new_row]
            line = ';'.join(row) + '\n'
            new_file.write(line)
        new_file.close()
        
    def transform_file_lazy(self, new_filename):
        new_file = open(new_filename, 'w')
        
        header = self.get_transformed_header()
        cp_header = ';'.join(header).decode('cp1250')
        new_file.write(cp_header + '\n')
        #new_file.write(';'.join(header) + '\n')
        row = self.data.get_next_row()
        while row is not None:
            transformed_row = self.transform_row(row)
            new_row = self.transform_row_to_list(transformed_row)
            row = [str(field) for field in new_row]
            line = ';'.join(row) + '\n'
            new_file.write(line)
            row = self.data.get_next_row()
        new_file.close()
        
        
    def set_new_data(self, csvdata):
        self.data = csvdata
        self.build_trans_list()
        

class OldToNewFormat:
    
    """Transformes file in the old format to the new one:
    - removes IDD, IDSPRAW, UWAGI, STATUS(for Rb27s and Rb28s)
    - moves ID_JST after PT, renames to KODMSWIA and changes its value
    """
    
    def __init__(self, filename):
        """Reads data from specified file.
        
        Arguments:
        filename -- name of file to change
        """
        csv_file = CsvFile(filename, delim=';')
        self.data = CsvData(csv_file)
        self.name = filename
        self.status_exists = 'STATUS' in self.data.get_header()
    
    def change_file(self, fname=None):
        """Transforms file to new format and saves it.
        
        Arguments:
        fname -- name of transformed file, if None, then generated from
                 input file's name
        """
        new_header = self.change_header(self.data.get_header())
        new_rows = []
        
        row = self.data.get_next_row(row_type='list')
        while row:
            new_row = self.change_row(row)
            new_rows.append(new_row)
            row = self.data.get_next_row(row_type='list')
        
        if fname is None:
            fname = self.name[:-4] + '_mod.csv'
        
        save_data = Data([new_header] + new_rows, fname)
        save_data.save(quoting=csv.QUOTE_NONE)
    
    def change_header(self, header):
        """Returns new header with unnecessary fields removed: IDD, IDSPRAW,
        UWAGI[, STATUS]; moves ID_JST after PT and renames it to KODMSWIA.
        
        Arguments:
        header -- header that should be changed
        """
        new_header = header[:]
                
        if self.status_exists:
            to_remove = [15, 12, 2, 1, 0]
        else:
            to_remove = [12, 2, 1, 0]
        
        for i in to_remove:
            del new_header[i]
        
        new_header.insert(6, 'KODMSWIA')
        return new_header
    
    def change_row(self, row):
        """Returns changed row.
        Removes unnecessary fields(the same as change_header) and
        moves value from ID_JST to KODMSWIA and changes it:
        if it's związek JST, copies it without the first digit,
        otherwise changes it to ''
        
        Arguments:
        row -- data row to change
        """
        new_row = row[:]
        mswia_code = new_row[2]
        
        if self.status_exists:
            to_remove = [15, 12, 2, 1, 0]
        else:
            to_remove = [12, 2, 1, 0]
        
        for i in to_remove:
            del new_row[i]
        
        if mswia_code.startswith('4'):
            mswia_code = mswia_code[1:]
        else:
            mswia_code = ''
        
        new_row.insert(6, mswia_code)
            
        return new_row
        

class TTPDecoder:
    
    """Decodes teryt, jst name and paragraph 4."""
    
    def __init__(self, teryt_file, jst_file, par_file):
        """Initiates TTPDecoder by creating all single decoders.
        
        Arguments:
        teryt_file -- name of file with TERYT codes
        jst_file -- name of file with jst codes
        par_file -- name of file with paragraphs
        """
        self.teryt_decoder = TerytDecoder(teryt_file)
        self.jst_decoder = JSTDecoder(jst_file)
        self.par_decoder = ParDecoder(par_file)
        self.header_transform_dict = {
            u'Kod województwa wg GUS': u'Województwo',
            u'Kod powiatu wg GUS': u'Powiat',
            u'Kod gminy wg GUS': u'Gmina',
            u'Typ gminy': u'Typ jednostki',
            u'Finansowanie paragrafu - 4 cyfra paragrafu': u'Finansowanie'
        }
        
    def decode_file(self, source_filename, result_filename):
        """Decodes given file.
        
        Arguments:
        source_filename -- name of file to decode
        result_filename -- name of file to save decoded data
        """
        csv_file = CsvFile(source_filename, delim=';', quote='"')
        csv_data = CsvData(csv_file)
        
        new_header = self.decode_header(csv_data.get_header())
        
        new_rows = []
        row = csv_data.get_next_row(row_type='list')
        i = 0
        while row:
            i += 1
            changed_row = row[:]
            is_jst = row[4] in ['z', 'Z']
            
            changed_row[1] = self.teryt_decoder.get_name(row[1])
            if changed_row[1] is None:
                print i
            if is_jst:
                changed_row[2] = self.teryt_decoder.get_name(row[1] + row[2])
                if changed_row[2] is None:
                    print i
                changed_row[3] = self.jst_decoder.get_name(row[6][1:]) # decoder has xyz, file has 0xyz
                if changed_row[3] is None:
                    print i
                changed_row[4] = u'Związek JST'
            else:
                type = self.teryt_decoder.get_type(row[1])
                if row[2] != '00':
                    changed_row[2] = self.teryt_decoder.get_name(row[1] + row[2])
                    if changed_row[2] is None:
                        print i
                    type = self.teryt_decoder.get_type(row[1] + row[2])
                else:
                    changed_row[2] = ''
                    
                if row[3] != '00':
                    changed_row[3] = self.teryt_decoder.get_name(row[1] + row[2] + row[3])
                    if changed_row[3] is None:
                        print i
                    type = self.teryt_decoder.get_type(row[1] + row[2] + row[3])
                else:
                    changed_row[3] = ''
                changed_row[4] = type
            
            self.clean_row(changed_row)
            new_rows.append(changed_row)
            row = csv_data.get_next_row(row_type='list')
        
        csv_file.close()
        
        
        new_data = Data([new_header] + new_rows, result_filename)
        new_data.save()
        
        
    def decode_header(self, header):
        """Decodes fields in header, returns changed header.
        
        Arguments:
        header -- header of file
        """
        changed_header = []
        for field in header:
            try:
                changed_header.append(self.header_transform_dict[field])
            except KeyError:
                changed_header.append(field)
        
        self.clean_row(changed_header)
        
        return changed_header
        
    
    def clean_row(self, row):
        """Removes unnecessary fields from row."""
        del row[6]
        del row[5]
        
        return row
                

if __name__ == '__main__':
    """all_files = ['Rb27s_csv.csv',
                 'Rbnds_csv.csv',
                 'Rbn_csv.csv', 'Rbn4_csv.csv', 'Rbnpg_csv.csv',
                 'Rb28s_csv.csv', 'Rb28sd_csv.csv',
                 'Rbz_csv.csv', 'Rbzkp_csv.csv', 'Rbzpg_csv.csv',
                 'Rbzzu_csv.csv'
    ]"""
    """all_files = ['Rb27s_csv.csv',
                 'Rbnds_csv.csv',
                 'Rbn_csv.csv', 'Rbn4_csv.csv', 'Rbnpg_csv.csv',
                 'Rb28s_csv.csv',
                 'Rbz_csv.csv', 'Rbzkp_csv.csv', 'Rbzpg_csv.csv',
                 'Rbzzu_csv.csv'
    ]"""
    """all_files = ['Rb27s_csv.csv',
                 'Rbnds_csv.csv',
                 'Rbn_csv.csv',
                 'Rb28s_csv.csv',
                 'Rbz_csv.csv', 'Rbzkp_csv.csv',
                 'Rbzzu_csv.csv'
    ]"""
    """all_files = ['Rb27s_csv.csv',
                 'Rbnds_csv.csv',
                 'Rbn_csv.csv',
                 'Rb28s_csv.csv',
                 'Rbz_csv.csv', 'Rbzkp_csv.csv',
                 'Rbzzu_csv.csv', 'Rbzpw_csv.csv',
                 'Rbzkz_csv.csv'
    ]"""
    """all_files = ['Rb27s_csv.csv',
                 'Rbnds_csv.csv',
                 'Rbn_csv.csv',
                 'Rb28s_csv.csv',
                 'Rbz_csv.csv'
    ]"""
    """all_files = ['Rb27s_csv.csv',
                 'Rbnds_csv.csv',
                 'Rb28s_csv.csv',
                 'Rbz_csv.csv', 'Rbzkp_csv.csv',
                 'Rbzpw_csv.csv', 'Rbzzu_csv.csv',
                 'Rbzkz_csv.csv'
    ]"""
    """all_files = ['old_Rb27s_csv.csv',
                 'old_Rbnds_csv.csv',
                 'old_Rb28s_csv.csv',
                 'old_Rbz_csv.csv', 'old_Rbzkp_csv.csv',
                 'old_Rbzpw_csv.csv', 'old_Rbzzu_csv.csv',
                 'old_Rbzkz_csv.csv'
    ]"""
    all_files = ['old_Rb27s_csv.csv',
                 'old_Rbnds_csv.csv',
                 'old_Rb28s_csv.csv',
                 'old_Rbz_csv.csv'
    ]
    
    old_format = True
    
    decoder_d = Decoder('klasyfikacja_2004_09_20_mod_d.txt')
    decoder_w = Decoder('klasyfikacja_2004_09_20_mod_w.txt')
    base_dir = '2004'
    jst_dict_name = 'slownik_jst_2011_max_ost.csv'
    
    mapping_set = MappingsSet('mapowanie_stare_csv.txt')
    header_transformer = HeaderTransformer(mapping_set)
    
    if old_format:
        print 'Step 0'
        new_filenames = []
        for filename in all_files:
            source_path = os.path.join(base_dir, 'source', filename)
            format_changer = OldToNewFormat(source_path)
            changed_filename = filename.lstrip('old_')
            dest_path = os.path.join(base_dir, 'source', changed_filename)
            format_changer.change_file(dest_path)
            new_filenames.append(changed_filename)
            
        all_files = new_filenames

    print 'Step 1'
    for filename in all_files:
        source_path = os.path.join(base_dir, 'source', filename)
        if filename in ['Rb27s_csv.csv', 'Rb28s_csv.csv']:
            dest_path = os.path.join(base_dir, 'step1', filename)
        else:
            dest_path = os.path.join(base_dir, 'step2', filename)
        header_transformer.transform_file(source_path, dest_path, debug=True)
    
    
    print 'Step 2'
    for filename in ['Rb27s_csv.csv', 'Rb28s_csv.csv']:
        source_path = os.path.join(base_dir, 'step1', filename)
        dest_path = os.path.join(base_dir, 'step2', filename)
        
        csvfile = CsvFile(source_path, delim=';')
        data = CsvData(csvfile)
        if filename == 'Rb27s_csv.csv':
            decoder = decoder_d
        elif filename == 'Rb28s_csv.csv':
            decoder = decoder_w
        csv_transformer = CsvTransformer(data, decoder, header_transformer)
        csv_transformer.transform_file_lazy(dest_path)
        print 'Step 2, undecoded values: ', decoder.get_undecoded_values()
    
    
    print 'Step 3'

    #jst_dict_path = os.path.join(base_dir, 'source', jst_dict_name)
    jst_dict_path = jst_dict_name
    ttp_decoder = TTPDecoder('TERYT.csv', jst_dict_path, 'par4.csv')
    for filename in all_files:
        source_path = os.path.join(base_dir, 'step2', filename)
        dest_path = os.path.join(base_dir, 'target', filename)
        ttp_decoder.decode_file(source_path, dest_path)
    
    print 'Done'
