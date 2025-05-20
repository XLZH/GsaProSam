#!/usr/bin/python3
# -*- coding: utf-8 -*-

# *************************************************************************
#    > File Name: GenCraXML.py
#    > Author: xlzh
#    > Mail: xiaolongzhang2015@163.com
#    > Created Time: 2025年05月16日 星期五 19时17分45秒
# *************************************************************************

import sys
import threading
from xml.dom import minidom
import os.path as op
import math

from GSA2XMLProSam import MysqlUtils
from GSA2XMLProSam import generate_submitter


def read_cra_accession(file_name: str, xml_dir: str) -> list:
    """
    Reads a list of CRA accession from a specified file.
    Note: skip the accession if the XML existed in the xml directory
    """
    cra_list = []

    file_fp = open(file_name, 'r')
    for line in file_fp:
        if line.startswith('accession'):
            continue

        if not line.startswith('CRA'):
            sys.stderr.write(f"[Error:read_cra_accession] wrong CRA accession of {line.rstrip()}\n")
            sys.exit(-1)

        cra_acc = line.rstrip()
        xml_path = op.join(xml_dir, f"{cra_acc}.xml")
        if op.exists(xml_path):  # skip existed xml
            continue

        cra_list.append(line.rstrip())

    return cra_list


def generate_cra_description(cra_acc, prj_acc, cra_dict, user_dict, main_doc):
    descriptor_element = main_doc.createElement("Descriptor")

    comment_element = main_doc.createElement("Comment")
    comment_element.appendChild(main_doc.createTextNode(f"GSA to DRA Data submission. GSA accession: {cra_acc}"))
    descriptor_element.appendChild(comment_element)

    title_element = main_doc.createElement("Title")
    title_str = cra_dict['title'] if cra_dict['title'] else 'missing0'
    title_element.appendChild(main_doc.createTextNode(title_str))
    descriptor_element.appendChild(title_element)

    desc_element = main_doc.createElement("Description")
    desc_str = cra_dict['description'] if cra_dict['description'] else 'missing'
    desc_element.appendChild(main_doc.createTextNode(desc_str))
    descriptor_element.appendChild(desc_element)

    submitter_element = generate_submitter(user_dict, main_doc)
    descriptor_element.appendChild(submitter_element)

    submit_element = main_doc.createElement("SubmissionDate")
    date_str = cra_dict['submit_time']
    date_str = date_str.strftime("%Y-%m-%d") if date_str else 'missing'
    submit_element.appendChild(main_doc.createTextNode(date_str))
    descriptor_element.appendChild(submit_element)

    release_element = main_doc.createElement("ReleaseDate")
    date_str = cra_dict['release_time'].strftime("%Y-%m-%d")
    release_element.appendChild(main_doc.createTextNode(date_str))
    descriptor_element.appendChild(release_element)

    identifier_element = main_doc.createElement("Identifier")
    spuid_element = main_doc.createElement("SPUID")
    spuid_element.setAttribute("spuid_namespace", "NGDC")
    spuid_element.setAttribute("db", "GSA")
    spuid_element.appendChild(main_doc.createTextNode(cra_acc))
    identifier_element.appendChild(spuid_element)
    descriptor_element.appendChild(identifier_element)

    attr_ref_element = main_doc.createElement("AttributeRefId")
    attr_ref_element.setAttribute("name", "BioProject")
    ref_element = main_doc.createElement("RefId")
    spuid_element = main_doc.createElement("SPUID")
    spuid_element.setAttribute("spuid_namespace", "NGDC")
    spuid_element.setAttribute("db", "BioProject")
    spuid_element.appendChild(main_doc.createTextNode(prj_acc))
    ref_element.appendChild(spuid_element)
    attr_ref_element.appendChild(ref_element)
    descriptor_element.appendChild(attr_ref_element)

    return descriptor_element


def generate_experiment_action(run, main_doc):
    """ generate experiment action for the given Run
    """
    action_element = main_doc.createElement("Action")

    add_data_element = main_doc.createElement("AddData")
    add_data_element.setAttribute("target_object", "Experiment")

    attr_list = [
        'title',
        'instrument_model',
        'library_source',
        'library_selection',
        'library_strategy',
        'library_name',
        'insert_size',
        'library_construction_protocol',
        'library_layout'
    ]

    for attr in attr_list:
        attr_element = main_doc.createElement("Attribute")
        attr_element.setAttribute("name", attr)
        attr_element.appendChild(main_doc.createTextNode(run['formatted_attrs'][attr]))
        add_data_element.appendChild(attr_element)

    # generate the attribute ref id
    attr_ref_element = main_doc.createElement("AttributeRefId")
    attr_ref_element.setAttribute("name", "BioSample")

    ref_id_element = main_doc.createElement("RefId")
    spuid_element = main_doc.createElement("SPUID")
    spuid_element.setAttribute("spuid_namespace", "NGDC")
    spuid_element.setAttribute("db", "BioSample")
    spuid_element.appendChild(main_doc.createTextNode(run['sam_acc']))
    ref_id_element.appendChild(spuid_element)

    attr_ref_element.appendChild(ref_id_element)
    add_data_element.appendChild(attr_ref_element)

    # identifier
    identifier_element = main_doc.createElement("Identifier")
    spuid_element = main_doc.createElement("SPUID")
    spuid_element.setAttribute("spuid_namespace", "NGDC")
    spuid_element.setAttribute("db", "GSA")
    spuid_element.appendChild(main_doc.createTextNode(run['exp_acc']))
    identifier_element.appendChild(spuid_element)
    add_data_element.appendChild(identifier_element)

    action_element.appendChild(add_data_element)
    return action_element


def generate_run_action(run, main_doc):
    """ generate run action for the given Run
    """
    action_element = main_doc.createElement("Action")

    add_files_element = main_doc.createElement("AddFiles")
    add_files_element.setAttribute("target_object", "Run")

    # title of the run
    title_element = main_doc.createElement("Title")
    title_element.appendChild(main_doc.createTextNode(run['sam_title']))
    add_files_element.appendChild(title_element)

    # file type of the run (only fastq and bam is allowed)
    type_map = {1: 'FASTQ', 2: 'BAM'}
    file_type_element = main_doc.createElement("FileType")
    file_type_element.appendChild(main_doc.createTextNode(type_map[run['run_data_type']]))
    add_files_element.appendChild(file_type_element)

    # file of the run
    for rdf in run['file_list']:
        file_element = main_doc.createElement("File")
        file_element.setAttribute("file_path", rdf['rdf_archive_file_name'])
        md5_element = main_doc.createElement("MD5Checksum")
        md5_element.appendChild(main_doc.createTextNode(rdf['rdf_md5']))
        file_element.appendChild(md5_element)
        add_files_element.appendChild(file_element)

    # attribute ref id
    attr_ref_element = main_doc.createElement("AttributeRefId")
    attr_ref_element.setAttribute("name", "Experiment")

    ref_id_element = main_doc.createElement("RefId")
    spuid_element = main_doc.createElement("SPUID")
    spuid_element.setAttribute("spuid_namespace", "NGDC")
    spuid_element.setAttribute("db", "GSA")
    spuid_element.appendChild(main_doc.createTextNode(run['exp_acc']))
    ref_id_element.appendChild(spuid_element)

    attr_ref_element.appendChild(ref_id_element)
    add_files_element.appendChild(attr_ref_element)

    # identifier
    identifier_element = main_doc.createElement("Identifier")
    spuid_element = main_doc.createElement("SPUID")
    spuid_element.setAttribute("spuid_namespace", "NGDC")
    spuid_element.setAttribute("db", "GSA")
    spuid_element.appendChild(main_doc.createTextNode(run['run_acc']))
    identifier_element.appendChild(spuid_element)
    add_files_element.appendChild(identifier_element)

    action_element.appendChild(add_files_element)
    return action_element


def generate_cra_xml(cra_acc, xml_dir, mysql_con):
    """ generate the XML for the given CRA accession
        NOTE: we assume the give cra_acc is valid (existed and released)
    """
    main_doc = minidom.Document()
    submission = main_doc.createElement('Submission')

    # Add description for the CRA
    cra_dict = mysql_con.select_cra_by_cra_accession(cra_acc)
    user_dict = mysql_con.select_contact_by_cra_accession(cra_acc)
    project = mysql_con.select_bioproject_by_cra_accession(cra_acc)
    description = generate_cra_description(
        cra_acc=cra_acc,
        prj_acc=project['accession'],
        cra_dict=cra_dict,
        user_dict=user_dict,
        main_doc=main_doc
    )
    submission.appendChild(description)

    sample_exp_run = mysql_con.select_biosample_experiment_run_by_cra_accession(cra_acc)
    if not sample_exp_run:
        sys.stderr.write(f"[warning:generate_cra_xml] there is no valid run for {cra_acc}!\n")
        return -1

    exp_action_list, run_action_list = [], []

    for run in sample_exp_run:
        exp_action = generate_experiment_action(run, main_doc)
        exp_action_list.append(exp_action)
        run_action = generate_run_action(run, main_doc)
        run_action_list.append(run_action)

    # add the experiment and run action into submission
    for exp_action in exp_action_list:
        submission.appendChild(exp_action)

    for run_action in run_action_list:
        submission.appendChild(run_action)

    # add the submission into the main document
    main_doc.appendChild(submission)

    # write the xml into the given dir
    xml_fp = open(op.join(xml_dir, f'{cra_acc}.xml'), 'w')
    main_doc.writexml(xml_fp, indent='\t', addindent='\t', newl='\n', encoding="utf-8")
    return 0


def generate_cra_xml_single(cra_list, xml_dir):
    """ generate the cra xml file with single thread
    """
    mysql_con = MysqlUtils()
    n_item = len(cra_list)
    item_idx = 1

    for cra_acc in cra_list:
        sys.stdout.write(f"[{item_idx}/{n_item}] generate project action for {cra_acc} ...\n")
        generate_cra_xml(cra_acc, xml_dir, mysql_con)
        item_idx += 1

    mysql_con.close()


def generate_cra_xml_thread(cra_list, xml_dir, start_idx, end_idx):
    """ generate the cra xml within the thread
    """
    n_process = 0
    end_idx = end_idx if end_idx < len(cra_list) else len(cra_list)
    mysql_con = MysqlUtils()

    for idx in range(start_idx, end_idx):
        cra_acc = cra_list[idx]
        sys.stderr.write(f"[{start_idx}-{end_idx}] {cra_acc} ({n_process}/{end_idx - start_idx}) ...\n")
        generate_cra_xml(cra_acc, xml_dir, mysql_con)
        n_process += 1

    mysql_con.close()


def generate_cra_xml_parallel(cra_list, xml_dir, n_thread):
    """ generate the cra xml file with n threads
    """
    n_item = len(cra_list)
    sub_size = math.ceil(n_item / n_thread)
    idx_list = [(i, i + sub_size) for i in range(0, n_item, sub_size)]

    thread_list = []
    for idx in idx_list:
        thread = threading.Thread(target=generate_cra_xml_thread, args=(cra_list, xml_dir, idx[0], idx[1]))
        thread.start()
        thread_list.append(thread)

    # waiting for all threads finish
    for thread in thread_list:
        thread.join()


def main():
    args = sys.argv

    if len(args) != 4:
        sys.stderr.write("usage: python3 GenCraXML.py <cra_accession_list> <output_dir> <thread>\n")
        sys.exit(-1)

    # args = ['', '/Users/xlzh/Project/NgdcProject/011DdbjShare/Data/cra_accession20250512.txt', 'GsaCraXML', 4]

    cra_list = read_cra_accession(args[1], args[2])
    xml_dir = args[2]
    n_thread = int(args[3])

    if n_thread > 1:
        generate_cra_xml_parallel(cra_list, xml_dir, n_thread)

    else:
        generate_cra_xml_single(cra_list, xml_dir)


def debug_fun():
    mysql_con = MysqlUtils()
    cra_acc = 'CRA000095'
    xml_dir = "XML"

    generate_cra_xml(cra_acc, xml_dir, mysql_con)

    mysql_con.close()
    print('hello world')


if __name__ == '__main__':
    main()
