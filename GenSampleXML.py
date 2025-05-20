#!/usr/bin/python3
# -*- coding: utf-8 -*-

# *************************************************************************
#    > File Name: GenSampleXML.py
#    > Author: xlzh
#    > Mail: xiaolongzhang2015@163.com
#    > Created Time: 2025年03月14日 星期五 16时06分15秒
# *************************************************************************

import sys
import math
from xml.dom import minidom
import threading
import queue
from GSA2XMLProSam import MysqlUtils
from GSA2XMLProSam import generate_biosample, generate_description


def generate_sample_action(sample_list, start_idx, end_idx, main_doc, result_queue):
    """
    generate sample action by sample_acc
    """
    n_process = 0
    end_idx = end_idx if end_idx < len(sample_list) else len(sample_list)
    mysql_con = MysqlUtils()
    thread_result_list = []

    for idx in range(start_idx, end_idx):
        if n_process % 1000 == 0:
            sys.stderr.write(f"[Scope: {start_idx}-{end_idx}] {n_process}/{end_idx - start_idx} ...\n")

        sample_acc = sample_list[idx]
        sample_with_attrs = mysql_con.select_sample_with_attrs_by_sample_accession(sample_acc)
        if not sample_with_attrs:
            sys.stderr.write(f"[Error: generate_sample_action] sample {sample_acc} is not found!\n")
            continue

        sample_user_info = mysql_con.select_contact_by_sam_accession(sample_acc)
        sample_related_prj = mysql_con.select_sample_related_project(sample_acc)

        sample_action = generate_biosample(
            target_db="BioSample",
            spuid=sample_with_attrs['accession'] + ": " + sample_with_attrs['name'],
            sample_package=sample_with_attrs['package'],
            sample_taxonname=sample_with_attrs['taxon_name'],
            sample_user_info=sample_user_info,
            sample_related_prj=sample_related_prj['accession'],
            sample_with_attr=sample_with_attrs,
            main_doc=main_doc
        )

        thread_result_list.append(sample_action)
        n_process += 1

    # put all the sample action into the thread-safe result_queue
    for sample_action in thread_result_list:
        result_queue.put(sample_action)

    # close the mysql connection
    mysql_con.close()


def generate_sample_xml(sample_list: list, xml_path: str, n_thread=4):
    main_doc = minidom.Document()
    submission = main_doc.createElement('Submission')
    result_queue = queue.Queue()

    # Add description for the bio-project
    description = generate_description(
        'BioSample',
       '2025-03-14',
       'Yanqing',
       'Wang',
        'wangyanqing@big.ac.cn',
       'China National Center for Bioinformation',
        main_doc
    )
    submission.appendChild(description)

    # Generate sample action for each sample in the list
    n_item = len(sample_list)
    sub_size = math.ceil(n_item / n_thread)
    idx_list = [(i, i + sub_size) for i in range(0, n_item, sub_size)]

    thread_list = []
    for idx in idx_list:
        thread = threading.Thread(target=generate_sample_action,
                                  args=(sample_list, idx[0], idx[1], main_doc, result_queue))
        thread.start()
        thread_list.append(thread)

    # waiting for all the threads finish
    for thread in thread_list:
        thread.join()

    # add the sample action into submission
    sys.stderr.write("[*] add the sample action to the submission!\n")
    while not result_queue.empty():
        sample_action = result_queue.get()
        submission.appendChild(sample_action)

    main_doc.appendChild(submission)

    # Write the project to an XML file
    sys.stderr.write("[*] write the sample action to output XML file!\n")
    xml_fp = open(xml_path, 'w')
    main_doc.writexml(xml_fp, indent='\t', addindent='\t', newl='\n', encoding="utf-8")
    xml_fp.close()


def read_sample_accession(file_name: str) -> list:
    sample_list = []

    file_fp = open(file_name, 'r')
    for line in file_fp:
        if line.startswith('accession'):
            continue

        if not line.startswith('SAMC'):
            sys.stderr.write(f"[Error:read_sample_accession] wrong sample accession of {line.rstrip()}\n")
            sys.exit(-1)

        sample_list.append(line.rstrip())

    return sample_list


def main():
    args = sys.argv

    # if len(args) != 4:
    #     sys.stderr.write("usage: python3 GenSampleXML.py <sample_accession_list> <output_xml_file> <thread_num>\n")
    #     sys.exit(-1)

    args = ['', '/Users/xlzh/Project/NgdcProject/011DdbjShare/Data/sample_accession20250512.txt', 'GsaSample.xml', 16]

    sample_list = read_sample_accession(args[1])
    xml_path = args[2]
    n_thread = int(args[3])

    generate_sample_xml(sample_list, xml_path, n_thread)


if __name__ == '__main__':
    main()

