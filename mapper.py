# -*- coding: utf-8 -*-

'''
Created on 09-08-2011

@author: Bartek
'''

import csv
import os

class Mapping:
    def __init__(self, fname, dict):
        map = {}
        for key, value in dict.iteritems():
            map[key] = value
        self.filename = fname
        self.map = map
        
    def get_val(self, key):
        if key in self.map:
            return self.map[key]
        else:
            return None
        
class MappingsSet:
    def __init__(self, filename):
        mapping_file = open(filename, 'rb')
        dialect = csv.Sniffer().sniff(mapping_file.read(1024))
        mapping_file.seek(0)
        self.csv_reader = csv.reader(mapping_file, dialect)
        self.maps = {}
        self.get_all_maps()
        mapping_file.close()
        
    def create_next_map(self, fname, values_dict):
        new_mapping = Mapping(fname, values_dict)
        return new_mapping
    
    def add_map(self, map):
        self.maps[map.filename] = map
        
    def get_all_maps(self):
        vals_dict = {}
        fname = ''
        
        for row in self.csv_reader:
            pair = tuple(row)
                
            if pair[0] == 'fname':
                fname = pair[1]
            elif pair[0] == 'end':
                new_map = self.create_next_map(fname, vals_dict)
                self.add_map(new_map)
                vals_dict = {}
            else:
                vals_dict[pair[0]] = pair[1]
                
    def show_map(self, fname):
        map = self.get_map(fname)
        if map is not None:
            for key, val in map.map.iteritems():
                print key, '==>', val
        else:
            print 'Nie ma pliku o nazwie ', fname
        
    def show_all_maps(self):
        for fname in self.maps.iterkeys():
            self.show_map(fname)
            
    def show_all_files(self):
        print 'All files:'
        for fname in self.maps.iterkeys():
            print fname
            
    def get_map(self, fname):
        if fname in self.maps:
            return self.maps[fname]
        else:
            return None
        
    def get_common_map(self):
        return self.get_map('All')
            
            
class HeaderTransformer:
    def __init__(self, mappings_set):
        self.mset = mappings_set
        
    def get_filename_key(self, filename):
        part_name = os.path.split(filename)[1]
        return part_name.split('_')[0]
    
    def transform_files(self, filenames):
        for filename in filenames:
            self.transform_file(filename)
        
    def transform_file(self, filename, target_filename=None, debug=False, dial=False):
        fname = self.get_filename_key(filename)
        file = open(filename, 'rb')
        
        if dial:
            dialect = csv.Sniffer().sniff(file.read(1024))
            file.seek(0)
            csv_reader = csv.reader(file, dialect)
        else:
            csv_reader = csv.reader(file, delimiter=';', quotechar='"')
        partial_name = os.path.split(filename)[1] # get rid of path, get name
        new_header = self.transform_header(csv_reader, partial_name, debug)
        
        if not target_filename:
            target_filename = fname + '_csv_ok.csv'
        new_file = open(target_filename, 'wb')
        cp_header = new_header.decode('cp1250')
        new_file.write(cp_header + '\n')
        
        for row in csv_reader:
            cp_row = (';'.join(row) + '\n').decode('cp1250')
            new_file.write(cp_row)
        
        file.close()
        new_file.close()
        
    def transform_header(self, csv_reader, fname, debug=False):
        csv_row = csv_reader.next()
        map_name = fname.split('_')[0]
        return self.transform(csv_row, map_name, debug)
        
    def transform(self, row, fname, debug=False):
        transformed = []
        map = self.mset.get_map(fname)        
        for name in row:
            mapped_name = self.replace(name, fname, debug)
            transformed.append(mapped_name)
            if row.count(name) > 1:
                print 'BLAD, REP, key=%s, fname=%s' % (name, fname)
        return ';'.join(transformed)
    
    def replace(self, key, fname, debug=False):
        map = self.mset.get_map(fname)
        if map is not None:
            value = map.get_val(key)
            if value is None:
                common_map = self.mset.get_common_map()
                value = common_map.get_val(key)
            
            if value is None:
                if key != 'REGON' and debug:
                    print 'BLAD, key=%s, fname=%s' % (key, fname)
                return key
            else:
                return value 
        else:
            if key != 'REGON' and debug:
                print 'BLAD, key=%s, fname=%s' % (key, fname)
            return key
         

if __name__ == '__main__':
    mapping_set = MappingsSet('mapowanie_csv.txt')
    transformer = HeaderTransformer(mapping_set)
    filenames = ['Rb27s_csv.csv',
                 'Rbnds_csv.csv',
                 'Rbn_csv.csv', 'Rbn4_csv.csv', 'Rbnpg_csv.csv',
                 'Rb28s_csv.csv', 'Rb28sd_csv.csv',
                 'Rbz_csv.csv', 'Rbzkp_csv.csv', 'Rbzpg_csv.csv',
                 'Rbzzu_csv.csv'
    ]
    transformer.transform_files(filenames)
    