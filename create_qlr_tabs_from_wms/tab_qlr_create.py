"""
Creates TAB and QLR files from a image service WMS.

Python 34\
"""

import os
import glob
import re

data = r"C:\lists\*.csv"

def ensure_dir(file_path):
    if not os.path.exists(file_path):
        os.makedirs(file_path)

for f in glob.glob(data):
    # service name
    list_basename = os.path.splitext(os.path.basename(f))[0]
    pattern = r'_(1=1|product_type_=_4|\d{8})'
    service = re.sub(pattern, '', list_basename)

    print(f"\nWriting QLR/TABs for {service}...")

    # set directories
    tab_out = rf"C:\....\TAB_{service}\TAB"
    qlr_out = rf"C:\...\QLR_{service}\QLR"

    # templates
    xml_template = rf"C:\...\templates\XML-Template_{service}.xml"
    tab_template = rf"C:\...\TAB-Template_{service}.TAB"
    qlr_template = rf"C:\...\QLR-TEMPLATE_{service}.qlr"

    # check output directories exist, if not create them
    for x in tab_out, qlr_out:
        ensure_dir(x)

     # list of projects
    data_list = set(line.strip() for line in open(f))

    # write files
    qlr_count, tab_count = 0, 0

    for item in data_list:
        project = item.replace(',', "")
        with open(xml_template, 'r') as infile:
            xml_write = infile.read().replace('PROJECT_NAME', project)
            with open(os.path.join(tab_out, str(project + ".xml")), 'w') as outfile:
                outfile.writelines(xml_write)
                #print(str(project + ".xml") + " saved...")
        with open(tab_template, 'r') as infile:
            tab_write = infile.read().replace(os.path.basename(xml_template), project + ".xml")
            with open(os.path.join(tab_out, str(project + ".tab")), 'w') as outfile:
                outfile.writelines(tab_write)
                tab_count += 1
                #print(str(project + ".tab") + " saved...")
        with open(qlr_template, 'r') as infile:
            qlr_write = infile.read().replace('PROJECT_NAME', project)
            with open(os.path.join(qlr_out, str(project + ".qlr")), 'w') as outfile:
                outfile.writelines(qlr_write)
                qlr_count += 1
                #print(str(project + ".qlr") + " saved...")

    print(f"\n{service} complete. Total TABs written {tab_count}, total QLRs written {qlr_count}")









