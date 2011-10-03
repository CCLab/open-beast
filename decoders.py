# -*- coding: utf-8 -*-

'''
Created on 08-09-2011
'''

from data_wrapper import CsvFile, CsvData

class TerytDecoder:
    
    """Decodes teryt codes."""

    def __init__(self, teryt_file_name):
        """
        Initiates teryt decoder using csv_file with teryt codes.
        
        Arguments:
        teryt_file_name - name of csv file containing TERYT codes
        """
        csv_file = CsvFile(teryt_file_name, delim=';', quote='"')
        data = CsvData(csv_file)
        data.build(rows_type='list')
        self.codes = {}
        for row in data.get_rows():
            if not self.is_important(row[4]):
                continue
            woj, pow, gm = row[1], row[2], row[3]
            name, type = row[5], row[6]
            if woj not in self.codes:
                self.codes[woj] = {'name': name, 'type': type, 'pows': {}}
            if pow != '00':
                powiats = self.codes[woj]['pows']
                if pow not in powiats:
                    powiats[pow] = {'name': name, 'type': type, 'gms': {}}
                if gm != '00':
                    gmins = powiats[pow]['gms']
                    gmins[gm] = {'name': name, 'type': type}
        csv_file.close()
                    
    def get_unit(self, code):
        """Returns unit specified by teryt code. If decoder
        is unable to find such a unit, returns None.
        
        Arguments:
        code -- teryt code of unit
        """
        try:
            woj_code, pow_code, gm_code = code[:2], None, None
            if len(code) >= 4:
                pow_code = code[2:4]
                if len(code) >= 6:
                    gm_code = code[4:6]
            
            obj = self.codes[woj_code]
            if pow_code:
                obj = obj['pows'][pow_code]
                if gm_code:
                    obj = obj['gms'][gm_code]
            
            return obj
        except KeyError as e:
            obj = self.check_old(code)
            if obj:
                print 'TerytDecoder, old code: ', code
            else:
                print 'TerytDecoder, full_code: ', code, e
                return None
    
    def get_name(self, code):
        """Returns name of unit specified by teryt code. If decoder
        is unable to find such a unit, returns None.
        
        Arguments:
        code -- teryt code of unit
        """
        unit = self.get_unit(code)
        if unit:
            return unit['name']
        else:
            return None
    
    def get_type(self, code):
        """Returns type of unit specified by teryt code. If decoder
        is unable to find such a unit, returns None.
        
        Arguments:
        code -- teryt code of unit
        """
        unit = self.get_unit(code)
        if unit:
            return unit['type']
        else:
            return None
        
    def is_important(self, type):
        """Checks unit's type and returns True if this code should be saved.
        Otherwise it is 'miasto' or 'obszar wiejski'.
        
        Arguments:
        type -- type of unit
        """
        return type not in ['4', '5']
    
    
    def check_old(self, code):
        """Return teryt code for old teritorial unit. If such a unit does not
        exist, returns None.        
        
        Arguments:
        code -- teryt code of unit
        """
        olds = {
            '0263': {'name': 'Wałbrzych', 'type': 'miasto na prawach powiatu'},
            '060608': {'name': 'Rejowiec', 'type': 'gmina wiejska'}
        }
        try:
            return olds[code]
        except KeyError:
            return None
    
    
class JSTDecoder:
    
    """Decodes names of JST."""
    
    def __init__(self, file_name):
        """
        Initiates jst codes decoder using csv_file with jst codes.
        Omitts lines that does not describe jst.
        
        Arguments:
        file_name - name of csv file containing jst codes
        """
        
        csv_file = CsvFile(file_name, delim=';', quote='"', enc='cp1250')
        data = CsvData(csv_file)
        data.build(rows_type='list')
        self.codes = {}
        for row in data.get_rows():
            type = row[4]
            if type != 'z' and type != 'Z':
                continue
            
            code = row[9]
            name = row[0]
            self.codes[code] = name
    
    def get_name(self, code):
        """Returns name of jst for given code. If their is no jst connected
        with the code, then None is returned.
        
        Arguments:
        code -- code of jst
        """
        try:
            return self.codes[code]
        except KeyError as e:
            print 'JSTDecoder', e
            return None


class ParDecoder:
    
    """Decodes the fourth figure of paragraph."""
    
    def __init__(self, file_name):
        """Initiates decoder using file describing meaning of that figure.
        
        Arguments:
        file_name -- name of file with description of the fourth figure
        """
        
        csv_file = CsvFile(file_name, delim=';', quote='"')
        data = CsvData(csv_file)
        codes = {}
        for row in data.get_rows():
            codes[row[0]] = row[1]
    
    def get_descr(self, par):
        """Returns descr of fourth figure of paragraph.
        If given argument is not correct, returns None.
        
        Arguments:
        par -- the fourth figure of paragraph
        """
        try:
            return self.codes[par]
        except KeyError as e:
            print 'ParDecoder', e
            return None
