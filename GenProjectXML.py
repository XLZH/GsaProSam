#!/usr/bin/python3
# -*- coding: utf-8 -*-

# *************************************************************************
#    > File Name: GenProjectXML.py
#    > Author: xlzh
#    > Mail: xiaolongzhang2015@163.com
#    > Created Time: 2025年03月13日 星期四 20时59分35秒
# *************************************************************************

import sys
from xml.dom import minidom
from GSA2XMLProSam import MysqlUtils
from GSA2XMLProSam import generate_bioproject, generate_description


def generate_project_action(prj_acc: str, mysql_con: MysqlUtils, main_doc, submission):
    """
    Generates a project action element for a given project accession and appends it to the submission element.

    This function retrieves project-related information from a MySQL database using the provided project accession.
    It then constructs a project action element using the retrieved data and appends it to the submission element
    of the XML document.

    Parameters:
    -----------
    prj_acc : str
        The project accession number (e.g., 'PRJCA021730') for which the project action is to be generated.

    mysql_con : MysqlUtils
        An instance of the MysqlUtils class used to interact with the MySQL database and retrieve project-related data.

    main_doc : xml.dom.minidom.Document
        The main XML document object to which the project action element will be appended.

    submission : xml.dom.minidom.Element
        The submission element of the XML document to which the generated project action element will be appended.

    Returns:
    --------
    None
        The function does not return any value. It modifies the XML document by appending the generated project action
        element to the submission element.
    """
    project = mysql_con.select_bioproject_by_prj_accession(prj_acc)
    project_sample_scope = mysql_con.select_sample_scope_name_by_prj_accession(prj_acc)
    prj_data_type = mysql_con.select_prj_data_type_by_prj_accession(prj_acc)
    prj_taxon_name = mysql_con.select_taxon_name_by_prj_accession(prj_acc)
    prj_user_info = mysql_con.select_contact_by_prj_accession(prj_acc)
    prj_publication = mysql_con.select_project_publication(prj_acc)
    prj_grant = mysql_con.select_project_grants(prj_acc)

    prj_action = generate_bioproject(
        target_db="BioProject",
        prj_accn=project['accession'],
        prj_title=project['title'],
        prj_description=project['description'],
        prj_release_date=project['release_time'].strftime("%Y-%m-%d"),
        prj_user_info=prj_user_info,
        prj_sample_scope=project_sample_scope,
        prj_data_type=prj_data_type,
        prj_taxon_name=prj_taxon_name['taxonomy'],
        prj_publication=prj_publication,
        prj_grant=prj_grant,
        main_doc=main_doc
    )
    submission.appendChild(prj_action)


def generate_project_xml(prj_list: list, xml_path: str):
    """
    Generates an XML file containing project information for a list of project accessions.

    This function creates an XML document that includes a submission element and a description
    for the BioProject. It then iterates over the list of project accessions, generating project
    actions for each one and appending them to the submission element. Finally, the XML document
    is written to the specified file path.

    Parameters:
    -----------
    prj_list : list
        A list of project accession numbers (e.g., ['PRJCA021730', 'PRJCA021731']). Each accession
        number should start with 'PRJC'.

    xml_path : str
        The file path where the generated XML document will be saved. The path should include the
        file name and extension (e.g., 'XML/PRJCA021730.xml').

    Returns:
    --------
    None
        The function does not return any value. It writes the generated XML content to the specified
        file and closes the MySQL connection.
    """
    main_doc = minidom.Document()
    submission = main_doc.createElement('Submission')
    mysql_con = MysqlUtils()

    # Add description for the bio-project
    description = generate_description(
        'BioProject',
       '2025-03-17',
       'Yanqing',
       'Wang',
        'wangyanqing@big.ac.cn',
       'China National Center for Bioinformation',
        main_doc
    )
    submission.appendChild(description)

    # Generate project action for each project in the list
    n_item = len(prj_list)
    item_idx = 1

    for prj in prj_list:
        sys.stdout.write(f"[{item_idx}/{n_item}] generate project action for {prj} ...\n")
        generate_project_action(prj, mysql_con, main_doc, submission)
        item_idx += 1

    main_doc.appendChild(submission)

    # Write the project to an XML file
    xml_fp = open(xml_path, 'w')
    main_doc.writexml(xml_fp, indent='\t', addindent='\t', newl='\n', encoding="utf-8")
    mysql_con.close()


def read_project_accession(file_name: str) -> list:
    """
    Reads a list of project accession numbers from a specified file.

    This function opens a file and reads each line, expecting it to contain a project
    accession number that starts with 'PRJC'. If a line does not start with 'PRJC',
    an error message is printed to stderr and the program exits with a status code of -1.
    Valid project accession numbers are stripped of any trailing whitespace and added
    to a list, which is returned at the end.

    Parameters:
    -----------
    file_name : str
        The path to the file containing the project accession numbers. Each line in the
        file should contain a single project accession number.

    Returns:
    --------
    list
        A list of project accession numbers read from the file.

    Raises:
    -------
    SystemExit
        If any line in the file does not start with 'PRJC', the function will print an
        error message and exit the program with a status code of -1.
    """
    prj_list = []

    file_fp = open(file_name, 'r')
    for line in file_fp:
        if line.startswith('accession'):
            continue

        if not line.startswith('PRJC'):
            sys.stderr.write(f"[Error:read_project_accession] wrong project accession of {line.rstrip()}\n")
            sys.exit(-1)

        prj_list.append(line.rstrip())

    return prj_list


def main():
    # args = sys.argv
    #
    # if len(args) != 3:
    #     sys.stderr.write("usage: python3 GenProjectXML.py <project_accession_list> <output_xml_file>\n")
    #     sys.exit(-1)

    args = ['', '/Users/xlzh/Project/NgdcProject/011DdbjShare/Data/project_accession20250820.txt', 'GsaProject20250820.xml']

    prj_list = read_project_accession(args[1])
    xml_path = args[2]

    # prj_list = ['PRJCA000902']
    # xml_path = "GsaProject.xml"

    generate_project_xml(prj_list, xml_path)


if __name__ == '__main__':
    main()

