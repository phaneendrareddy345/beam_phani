
#Author - Phani
#Version  Date
#   1    2022-02-09


'How to filter data from pcollections using map and filter method'

import apache_beam as beam

def is_phani(l):
    return l['name']=='phani'

with beam.Pipeline() as p:
    outp=(p|beam.Create([{'name':'phani','age':'21'},{'name':'isu','age':'18'}]))
    
    filtered_date = outp | beam.Map(lambda l:l['name']=='isu')

    "Filter is a method which takes another to check accross the pcollections --> Filter(fun()) or Filter(lambda row : row['fld_name']=='fld_value')"

    ftd = outp | beam.Filter(lambda l: l['name']=='phani')

    "to select fields apply map --> selected_flds= inp | beam.Map(lambda row:row[fld_name])"

    select = outp | beam.Map(lambda l: l['age'])

    select | beam.Map(print)

    fil_isu = outp | beam.Filter(lambda l : l['name']=='isu')

    #fil_isu | beam.Map(print)

    #ftdd = outp | beam.Map(lambda l:return l['name']=='phani')
    
    #filtered_date | beam.Map(print)

    #ftd | beam.Map(print)

    #ftd.beam.DataFrame.io.to_csv('\Users\Lenovo\OneDrive\Documents\outp.csv')

    