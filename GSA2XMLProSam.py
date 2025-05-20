#!/usr/bin/env python
# coding: utf-8
import re
import datetime
import logging
import json
import pymysql
from Config import GSA_DB

logging.basicConfig(level=logging.INFO, format="[%(levelname)s %(asctime)s] %(message)s")


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


def is_valid_pubmed(pubmed_id: str):
    """ check whether the pubmed id is valid
    """
    pattern = r'^\d{5,8}$'
    return bool(re.fullmatch(pattern, pubmed_id))


def is_valid_doi(doi_id: str):
    """ check whether the doi is valid
    """
    pattern = r'^10\.\d{4,9}/[-._;()/:A-Z0-9]+$'
    return bool(re.fullmatch(pattern, doi_id, re.IGNORECASE))


class MysqlUtils(object):
    def __init__(self):
        self.host = GSA_DB["host"]
        self.port = GSA_DB["port"]
        self.username = GSA_DB["username"]
        self.password = GSA_DB["password"]
        self.dbname = GSA_DB["dbname"]
        self.charsets = "utf8mb3"
        self.unique_relevance = None

        try:
            self.con = pymysql.Connect(
                host=self.host,
                port=int(self.port),
                user=self.username,
                passwd=self.password,
                db=self.dbname,
                charset=self.charsets
            )
            # 获得数据库的游标
            self.cursor = self.con.cursor(cursor=pymysql.cursors.DictCursor)  # 开启事务
            # logging.info("Get cursor successfully")
        except Exception as e:
            logging.info("Can not connect databse {}\nReason:{}".format(self.dbname, e))

    def close(self):
        if self.con:
            self.con.commit()
            self.con.close()
            logging.info("Close database {} successfully".format(self.dbname))
        else:
            logging.info("DataBase doesn't connect,close connectiong error;please check the db config.")

    def fetchOne(self):
        self.con.ping(reconnect=True)
        data = self.cursor.fetchone()
        return data

    def fetchAll(self):
        self.con.ping(reconnect=True)
        data = self.cursor.fetchall()
        return data

    def excute(self, sql, args=None):
        if args is None:
            logging.debug(sql)
            self.con.ping(reconnect=True)
            self.cursor.execute(sql)
            return self.cursor.rowcount
        else:
            logging.debug(sql)
            logging.debug(str(args))
            self.con.ping(reconnect=True)
            self.cursor.execute(sql, args)
            return self.cursor.rowcount

    def excute_insert(self, sql, myargs):
        logging.info(sql)
        self.con.ping(reconnect=True)
        self.cursor.execute(sql, myargs)
        return self.cursor.rowcount

    def commit(self):
        self.con.commit()

    def select_contact_by_prj_accession(self, prj):
        sql = "select first_name,middle_name,last_name,email,organization from submitter where submitter.submitter_id=(select submitter_id from project where project.accession=(%s));"
        self.excute(sql, prj)
        return self.fetchOne()

    def select_contact_by_cra_accession(self, cra):
        sql = "select first_name,middle_name,last_name,email,organization from submitter where submitter.submitter_id=(select submitter_id from cra where cra.accession=(%s));"
        self.excute(sql, cra)
        res = self.fetchOne()
        return res

    def select_contact_by_sam_accession(self, sam):
        sql = "select first_name,middle_name,last_name,email,organization from submitter where submitter.submitter_id=(select submitter_id from sample where sample.accession=(%s));"
        self.excute(sql, sam)
        return self.fetchOne()

    def select_sample_related_project(self, sam):
        sql = "SELECT accession FROM project WHERE project.prj_id = (SELECT prj_id FROM sample WHERE sample.accession=(%s));"
        self.excute(sql, sam)
        return self.fetchOne()

    def select_bioproject_by_prj_accession(self, prj):
        sql = "select * from project WHERE project.accession = (%s);"
        self.excute(sql, prj)
        return self.fetchOne()

    def select_bioproject_by_cra_accession(self, cra):
        sql = "select * from project WHERE project.prj_id = (select prj_id from cra WHERE cra.accession = (%s));"
        self.excute(sql, cra)
        return self.fetchOne()

    def select_cra_by_cra_accession(self, cra):
        sql = "select * from cra WHERE cra.accession = (%s);"
        self.excute(sql, cra)
        return self.fetchOne()

    def select_sample_scope_name_by_prj_accession(self, prj):
        sql = "select s.sample_scope_name from prj_sample_scope s WHERE s.sample_scope_id = (select p.sample_scope_id from project p WHERE p.accession = (%s)); "  # .format(country.strip().strip("'").strip())
        self.excute(sql, prj)
        res = self.fetchOne()
        sra_scope_name = ["Monoisolate", "Multiisolate", "Multispecies", "Environment", "Synthetic"]

        if res['sample_scope_name'] in sra_scope_name:
            return "e" + res['sample_scope_name']
        elif res['sample_scope_name'] == 'Single cell':
            return "eSingleCell"
        else:
            return "eOther"

    def select_prj_data_type_by_prj_accession(self, prj):
        sql = "select x.data_type_name from prj_data_type x WHERE x.data_type_id IN (select d.data_type_id from pro_data_type d WHERE d.prj_id =  (select prj_id from project WHERE project.accession = (%s)));"  # .format(country.strip().strip("'").strip())
        self.excute(sql, prj)
        res = self.fetchAll()
        logging.debug("select_prj_data_type_by_cra_accession:{}".format(res))
        if res is None:
            return []

        data_type_mapping = {  # (xlzh: 20250509) re-map the table
            'Genome sequencing and assembly': 'Genome sequencing and assembly',
            'Raw sequence reads': 'Raw sequence reads',
            'Genome sequencing': 'Genome sequencing',
            'Whole genome sequencing': 'Genome sequencing',  # different mapping
            'Genome sequencing': 'Genome sequencing',
            'Clone ends': 'Clone ends',
            'Epigenomics': 'Epigenomics',
            'Exome': 'Exome',
            'Map': 'Map',
            'Metabonomics': 'Metabolome',  # only ngdc and ddbj have this field
            'Metagenome': 'Metagenome',
            'Metagenomic assembly': 'Metagenomic assembly',
            'Phenotype or Genotype': 'Phenotype or Genotype',
            'Proteome': 'Proteome',
            'Random survey': 'Random survey',
            'Targeted loci cultured': 'Targeted loci cultured',
            'Targeted loci environmental': 'Targeted loci environmental',
            'Targeted Locus (Loci)': 'Targeted Locus (Loci)',
            'Transcriptome or Gene expression': 'Transcriptome or Gene expression',
            'Variation': 'Variation'
        }
        formated_dt = []
        hasOther = False
        for x in res:
            if x['data_type_name'].strip() in data_type_mapping.keys():
                formated_dt.append({"data_type_name": data_type_mapping[x['data_type_name'].strip()]})
            else:
                hasOther = True
        if hasOther:
            formated_dt.append({"data_type_name": "Other"})

        return formated_dt

    def select_taxon_name_by_prj_accession(self, prj):
        sql = "SELECT taxonomy FROM project WHERE accession=(%s);"
        self.excute(sql, prj)
        res = self.fetchOne()
        logging.debug("select_taxon_name_by_cra_accession:{}".format(res))
        if not res:
            return None

        return res

    def select_taxon_name_by_taxonid(self, taxid):
        sql = "SELECT name_txt FROM taxon_name WHERE taxon_name.tax_id = (%s) and taxon_name.name_class = 'scientific name'"
        self.excute(sql, taxid)
        res = self.fetchOne()
        return res

    def select_sample_with_attrs_by_sample_accession(self, sam_acc):
        sql = "select * from sample WHERE sample.accession = (%s);"
        self.excute(sql, sam_acc)

        sample_obj = self.fetchOne()
        if sample_obj is None:
            return None

        # sample_process_dict = {x['sample_id']: x for x in list(samples_list)}
        sample_type_mapping = {"1": "sample_attr_pathogen_clinical_host_associated",
                               "2": "sample_attr_pathogen_environmental_food_other",
                               "3": "sample_attr_microbe",
                               "4": "sample_attr_model_animal",
                               "5": "sample_attr_human",
                               "6": "sample_attr_plant",
                               "7": "sample_attr_virus",
                               "8": "sample_attr_metagenome_environmental",
                               "9": "sample_attr_mimsme_human_gut",
                               "10": "sample_attr_mimsme_soil",
                               "11": "sample_attr_mimsme_water"}

        package_mapping = {"1": "Pathogen.cl.1.0",
                           "2": "Pathogen.env.1.0",
                           "3": "Microbe.1.0",
                           "4": "Model.organism.animal.1.0",
                           "6": "Plant.1.0",
                           "7": "Virus.1.0",
                           "8": "Metagenome.environmental.1.0",
                           "9": "MIMS.me.human-gut.6.0",
                           "10": "MIMS.me.soil.6.0",
                           "11": "MIMS.me.water.6.0"}  ##2023年10.26把package换成了6.0

        # add at 20250423 for Non-human species with human package
        sample_type_id = str(sample_obj['sample_type_id'])
        if sample_type_id not in package_mapping:
            return None

        sample_type_table_name = sample_type_mapping[str(sample_obj['sample_type_id'])]
        sample_id = sample_obj['sample_id']

        # query attrs in sample_attr_tables
        sql2 = "select * from  {} as t  WHERE t.sample_id = '{}';".format(sample_type_table_name, sample_id)
        self.excute(sql2)
        sample_attr = self.fetchOne()
        if sample_attr is None:  # 20250319, there is no sample attr for the sample_id
            sample_attr = {}

        bad_keys = ['sample_id', 'type', 'taxon_id', "attribute_id", "geographic_location", "latitude_longitude"]
        for key in bad_keys:
            if key in sample_attr:
                sample_attr.pop(key)

        sample_obj['attrs'] = sample_attr
        sample_obj['package'] = package_mapping[str(sample_obj['sample_type_id'])]

        # query taxon id
        sql_taxon = "select k.name_txt from taxon_name k WHERE k.tax_id = (%s) and k.name_class ='scientific name';"
        self.excute(sql_taxon, sample_obj['taxon_id'])
        sample_taxon_name = self.fetchOne()
        sample_obj['taxon_name'] = sample_taxon_name
        return sample_obj

    def generate_project_relevance(self):
        if self.unique_relevance:
            return self.unique_relevance

        # generate the unique relevance
        sql = "select distinct(relevance) from project;"
        self.excute(sql)
        project_relevance = self.fetchAll()
        gsa2sra_project_relevance = {}

        for i in project_relevance:
            project_relevance_key = str(i["relevance"]).strip().capitalize()
            relevance_name = str(
                i["relevance"]).strip().replace(" ", "").upper()
            if relevance_name == "AGRICULTURAL":
                gsa2sra_project_relevance[project_relevance_key] = "Agricultural"
            elif relevance_name == "MEDICAL":
                gsa2sra_project_relevance[project_relevance_key] = "Medical"
            elif relevance_name == "INDUSTRIAL":
                gsa2sra_project_relevance[project_relevance_key] = "Industrial"
            elif relevance_name == "ENVIRONMENTAL":
                gsa2sra_project_relevance[project_relevance_key] = "Environmental"
            elif relevance_name == "EVOLUTION":
                gsa2sra_project_relevance[project_relevance_key] = "Evolution"
            elif relevance_name == "MODELORGANISM":
                gsa2sra_project_relevance[project_relevance_key] = "ModelOrganism"
            elif relevance_name == "OTHER":
                gsa2sra_project_relevance[project_relevance_key] = "Other"
            else:
                gsa2sra_project_relevance[project_relevance_key] = "Other"

        self.unique_relevance = gsa2sra_project_relevance
        return gsa2sra_project_relevance

    # (xlzh: 20250509) add publication tag for xml file
    def select_project_publication(self, prj_acc):
        sql = """
        SELECT 
            pub.publication_id, pub.pubmed_id, pub.doi, pub.article_title, pub.journal_title, pub.year, pub.month 
        FROM 
            publication pub 
        WHERE 
            is_deleted_by_user=0 AND prj_id = (SELECT prj_id FROM project WHERE project.accession = (%s)) 
        GROUP BY 
            pub.pubmed_id, pub.doi;
        """
        self.excute(sql, prj_acc)
        res = self.fetchAll()
        if res is None:  # there is no publication for the project
            return []

        # convert the None or Null fields
        valid_publication = []

        for prj_pub in res:
            if not is_valid_pubmed(prj_pub['pubmed_id']) and not is_valid_doi(prj_pub['doi']):  # both field is missing
                continue

            target_field = ['article_title', 'journal_title', 'year', 'month']
            for field in target_field:
                if prj_pub[field]:  # the filed is not missing
                    continue

                prj_pub[field] = 'missing'

            valid_publication.append(prj_pub)

        return valid_publication

    # (xlzh: 20250509) add grants tag for xml file
    def select_project_grants(self, prj_acc):
        sql = """
        SELECT 
            g.grant_ID, g.agency, g.agency_abbr, g.grant_title
        FROM 
            prj_grants g
        INNER JOIN 
            pro_grants pg ON g.grants_id = pg.grants_id
        WHERE 
            pg.prj_id = (SELECT prj_id FROM project WHERE project.accession = (%s));
        """
        self.excute(sql, prj_acc)
        res = self.fetchAll()
        if not res:  # there is no publication for the project
            return []

        valid_list = []

        for grant in res:
            if not grant['grant_ID']:  # skip items without grantID
                continue

            target_field = ['agency', 'agency_abbr', 'grant_title']
            for field in target_field:
                if grant[field]:  # the filed is not missing
                    continue

                grant[field] = 'missing'

            valid_list.append(grant)

        return valid_list

    # (xlzh: 20250517) generate run for CRA submission
    def select_biosample_experiment_run_by_cra_accession(self, cra):
        # 20230928 添加了and run_data_type_id in (1,2)，这样只保证交换fq和bam，也只转这部分得元信息
        sql = """
        SELECT 
            %s AS cra_acc,
            rdf.run_file_id AS rdf_run_file_id, 
            rdf.run_file_name AS rdf_run_file_name, 
            rdf.archived_file_name AS rdf_archive_file_name,
            rdf.md5 AS rdf_md5, 
            ser.* 
        FROM 
            run_data_file AS rdf 
        JOIN (
            SELECT 
                r.exp_id AS run_exp_id,
                r.run_id AS run_run_id, 
                r.accession AS run_acc, 
                r.run_data_type_id AS run_data_type,
                r.alias AS run_alias,
                se.* 
            FROM 
                run r 
            JOIN (
                SELECT 
                    e.*,
                    s.accession AS sam_acc,
                    s.prj_id AS sam_prj_id, 
                    s.sample_id AS sam_sample_id, 
                    s.`name` AS sam_name, 
                    s.title AS sam_title,
                    s.taxon_id AS sam_taxon_id, 
                    s.sample_type_id AS sam_sample_type_id, 
                    s.public_description AS sam_public_description 
                FROM 
                    sample AS s 
                JOIN (
                    SELECT 
                        experiment.accession AS exp_acc,
                        experiment.prj_id AS exp_prj_id, 
                        experiment.cra_id AS exp_cra_id, 
                        experiment.sample_id AS exp_sample_id, 
                        experiment.exp_id AS exp_exp_id, 
                        experiment.selection_id AS exp_selection_id, 
                        experiment.platform_id AS exp_platform_id, 
                        experiment.strategy_id AS exp_strategy_id, 
                        experiment.source_id AS exp_source_id, 
                        experiment.lib_design AS exp_lib_design, 
                        experiment.lib_layout AS exp_lib_layout, 
                        experiment.lib_name AS exp_lib_name,
                        experiment.lib_insert_size AS exp_lib_insert,
                        experiment.title AS exp_title 
                    FROM  
                        experiment  
                    WHERE 
                        experiment.cra_id = (SELECT cra_id FROM cra WHERE cra.accession = %s)
                ) AS e ON s.sample_id = e.exp_sample_id
            ) AS se ON r.exp_id = se.exp_exp_id
        ) AS ser ON rdf.run_id = ser.run_run_id 
        WHERE 
            status = 10 AND run_data_type IN (1, 2);
        """
        self.excute(sql, (cra, cra))
        res = self.fetchAll()

        if res is None:
            return []

        # res is not None
        crr_dict = {}

        for rdf_dict in res:
            run_name = rdf_dict['run_acc']
            if run_name not in crr_dict:
                crr_dict[run_name] = {'file_list': []}

            rdf_info = {}
            for key, value in rdf_dict.items():
                if key in {'rdf_archive_file_name', 'rdf_md5'}:  # add the item into file_list
                    rdf_info[key] = value

                elif not key.startswith('rdf'):
                    crr_dict[run_name][key] = value

            crr_dict[run_name]['file_list'].append(rdf_info)

        # add formatted_attrs for storing attrs of CRA
        for crrid in crr_dict:
            run_dict = crr_dict[crrid]
            run_dict['formatted_attrs'] = {}

            # experiment title
            exp_title = run_dict['exp_title']
            run_dict['formatted_attrs']['title'] = exp_title if exp_title else 'missing'

            # instrument model
            sql_instrument = 'select exp_platform.platform_name from exp_platform WHERE exp_platform.platform_id =(%s);'
            self.excute(sql_instrument, run_dict['exp_platform_id'])
            sql_instrument_res = self.fetchOne()
            run_dict['formatted_attrs']['instrument_model'] = sql_instrument_res['platform_name']

            # library_source
            sql_source = 'select exp_lib_source.source_name from exp_lib_source WHERE exp_lib_source.source_id =(%s);'
            self.excute(sql_source, run_dict['exp_source_id'])
            sql_source_res = self.fetchOne()
            run_dict['formatted_attrs']['library_source'] = sql_source_res['source_name']

            # library_selection
            sql_selection = 'select exp_lib_selection.selection_name from exp_lib_selection WHERE exp_lib_selection.selection_id =(%s);'
            self.excute(sql_selection, run_dict['exp_selection_id'])
            selection_res = self.fetchOne()
            run_dict['formatted_attrs']['library_selection'] = selection_res['selection_name']

            # library_strategy
            sql_strategy = 'select exp_lib_strategy.strategy_name from exp_lib_strategy WHERE exp_lib_strategy.strategy_id =(%s);'
            self.excute(sql_strategy, run_dict['exp_strategy_id'])
            strategy_res = self.fetchOne()
            run_dict['formatted_attrs']['library_strategy'] = strategy_res['strategy_name']

            # library_name
            lib_name = run_dict['exp_lib_name']
            run_dict['formatted_attrs']['library_name'] = lib_name if lib_name else 'missing'

            # library_protocol
            lib_protocol = run_dict['exp_lib_design']
            run_dict['formatted_attrs']['library_construction_protocol'] = lib_protocol if lib_protocol else 'missing'

            # insert size
            insert_size = run_dict['exp_lib_insert']
            run_dict['formatted_attrs']['insert_size'] = str(insert_size) if insert_size else 'missing'

            # library layout
            layout_map = {'1': 'SINGLE', '2': 'PAIRED'}
            layout_flag = run_dict['exp_lib_layout']
            run_dict['formatted_attrs']['library_layout'] = layout_map[layout_flag] if layout_flag else 'missing'

        return crr_dict.values()


def generate_description(share_type, release_date, first_name, last_name, email, organization, main_doc):
    first_name_element = main_doc.createElement('First')
    first_name_element.appendChild(main_doc.createTextNode(first_name))

    last_name_element = main_doc.createElement('Last')
    last_name_element.appendChild(main_doc.createTextNode(last_name))

    name_element = main_doc.createElement("Name")
    name_element.appendChild(first_name_element)
    name_element.appendChild(last_name_element)

    contact_element = main_doc.createElement("Contact")
    contact_element.appendChild(name_element)
    contact_element.setAttribute("email", email)

    organization_name_element = main_doc.createElement("Name")
    organization_name_element.appendChild(main_doc.createTextNode(organization))

    organization_element = main_doc.createElement("Organization")
    organization_element.setAttribute("role", "owner")
    organization_element.setAttribute("type", "institute")
    organization_element.appendChild(organization_name_element)
    organization_element.appendChild(contact_element)

    comment_element = main_doc.createElement("Comment")
    comment_element.appendChild(main_doc.createTextNode(f"NGDC to DDBJ data transfer: {share_type}"))
    hold_element = main_doc.createElement("Hold")
    hold_element.setAttribute("release_date", release_date)

    description_element = main_doc.createElement("Description")
    description_element.appendChild(comment_element)
    description_element.appendChild(organization_element)
    description_element.appendChild(hold_element)

    return description_element


def generate_action_tree(target_db, spuid, main_doc):
    action = main_doc.createElement("Action")
    adddata = main_doc.createElement("AddData")
    adddata.setAttribute("target_db", target_db)
    action.appendChild(adddata)
    data = main_doc.createElement("Data")
    data.setAttribute("content_type", "xml")
    adddata.appendChild(data)
    XmlContent = main_doc.createElement("XmlContent")
    data.appendChild(XmlContent)

    Identifier = main_doc.createElement("Identifier")

    SPUID = main_doc.createElement("SPUID")
    SPUID.setAttribute("spuid_namespace", "NGDC")
    SPUID.appendChild(main_doc.createTextNode(spuid))
    Identifier.appendChild(SPUID)
    adddata.appendChild(Identifier)

    return [action, adddata, data, XmlContent]


def generate_submitter(user_info, main_doc):
    submitter = main_doc.createElement('Submitter')

    # add username element
    first_name_element = main_doc.createElement('First')
    first_name_element.appendChild(main_doc.createTextNode(user_info['first_name']))
    middle_name_element = main_doc.createElement('Middle')
    middle_name_element.appendChild(main_doc.createTextNode(user_info['middle_name']))
    last_name_element = main_doc.createElement('Last')
    last_name_element.appendChild(main_doc.createTextNode(user_info['last_name']))

    name_element = main_doc.createElement("Name")
    name_element.appendChild(first_name_element)
    name_element.appendChild(middle_name_element)
    name_element.appendChild(last_name_element)

    # add email element
    email_element = main_doc.createElement("Email")
    email_element.appendChild(main_doc.createTextNode(user_info['email']))

    # add organization element
    organization_element = main_doc.createElement("Organization")
    organization_element.appendChild(main_doc.createTextNode(user_info['organization']))

    # add the child elements to submitter
    submitter.appendChild(organization_element)
    submitter.appendChild(email_element)
    submitter.appendChild(name_element)

    return submitter


# create  bioproject
def generate_bioproject(
        target_db,
        prj_accn,
        prj_title,
        prj_description,
        prj_release_date,
        prj_user_info,
        prj_sample_scope,
        prj_data_type,
        prj_taxon_name,
        prj_publication,
        prj_grant,
        main_doc):

    prjaction, prjadddata, prjdata, prjxmlcontent = generate_action_tree(target_db, prj_accn, main_doc)
    Project = main_doc.createElement("Project")
    Project.setAttribute("schema_version", "2.0")
    prjxmlcontent.appendChild(Project)

    # project id
    ProjectID = main_doc.createElement("ProjectID")
    SPUID = main_doc.createElement("SPUID")
    SPUID.setAttribute("spuid_namespace", "NGDC")
    SPUID.appendChild(main_doc.createTextNode(str(prj_accn).strip()))
    ProjectID.appendChild(SPUID)
    Project.appendChild(ProjectID)

    # descriptor
    Descriptor = main_doc.createElement("Descriptor")

    DesTitle = main_doc.createElement("Title")
    DesTitle.appendChild(main_doc.createTextNode(str(prj_title).strip()))
    Descriptor.appendChild(DesTitle)

    DesDescription = main_doc.createElement("Description")
    DesDescriptionP = main_doc.createElement("p")
    DesDescriptionP.appendChild(main_doc.createTextNode(str(prj_description).strip()))
    DesDescription.appendChild(DesDescriptionP)
    Descriptor.appendChild(DesDescription)

    DesExternalLink = main_doc.createElement("ExternalLink")
    DesExternalLinkRUL = main_doc.createElement("URL")
    DesExternalLinkRUL.appendChild(
        main_doc.createTextNode("https://ngdc.cncb.ac.cn/bioproject/browse/" + str(prj_accn).strip()))
    DesExternalLink.appendChild(DesExternalLinkRUL)
    Descriptor.appendChild(DesExternalLink)

    # (xlzh: 20250509) publication
    for prj_pub in prj_publication:
        DesPublication = main_doc.createElement("Publication")

        if is_valid_pubmed(prj_pub['pubmed_id']):  # there has pubmed ID for the publication
            DesPublication.setAttribute("id", prj_pub['pubmed_id'])
            db_type = 'ePubmed'

        else:  # DOI
            DesPublication.setAttribute("id", prj_pub['doi'])
            db_type = 'eDOI'

        # add reference tag
        DesReference = main_doc.createElement("Reference")
        RefTitle = main_doc.createElement("Title")
        RefTitle.appendChild(main_doc.createTextNode(prj_pub['article_title']))
        DesReference.appendChild(RefTitle)

        JournalTitle = main_doc.createElement("Journal")
        JournalTitle.appendChild(main_doc.createTextNode(prj_pub['journal_title']))
        DesReference.appendChild(JournalTitle)

        Year = main_doc.createElement("Year")
        Year.appendChild(main_doc.createTextNode(prj_pub['year']))
        DesReference.appendChild(Year)

        Month = main_doc.createElement("Month")
        Month.appendChild(main_doc.createTextNode(prj_pub['month']))
        DesReference.appendChild(Month)
        DesPublication.appendChild(DesReference)

        # add dbType tag
        dbType = main_doc.createElement("DbType")
        dbType.appendChild(main_doc.createTextNode(db_type))
        DesPublication.appendChild(dbType)
        Descriptor.appendChild(DesPublication)

    # (xlzh: 20250110) add grant tag
    for grant in prj_grant:
        DesGrant = main_doc.createElement("Grant")
        DesGrant.setAttribute("GrantId", grant['grant_ID'])
        Title = main_doc.createElement("Title")
        Title.appendChild(main_doc.createTextNode(grant['grant_title']))
        DesGrant.appendChild(Title)

        Agency = main_doc.createElement("Agency")
        Agency.setAttribute("abbr", grant['agency_abbr'])
        Agency.appendChild(main_doc.createTextNode(grant['agency']))
        DesGrant.appendChild(Agency)
        Descriptor.appendChild(DesGrant)

    # (xlzh: 20250509) Relevance tag is omitted by DDBJ
    Project.appendChild(Descriptor)

    # ProjectType
    ProjectType = main_doc.createElement("ProjectType")
    ProjectTypeSubmission = main_doc.createElement("ProjectTypeSubmission")
    ProjectTypeSubmission.setAttribute("sample_scope", prj_sample_scope)

    # (xlzh: 20250509) get taxonomy from project and only select the first one
    if prj_taxon_name:
        taxon_name = prj_taxon_name.split(',')[0]
        Organism = main_doc.createElement("Organism")
        OrganismName = main_doc.createElement("OrganismName")
        OrganismName.appendChild(main_doc.createTextNode(taxon_name))
        Organism.appendChild(OrganismName)
        ProjectTypeSubmission.appendChild(Organism)

    ProjectDataType = main_doc.createElement("ProjectDataType")
    for dt in prj_data_type:
        DataType = main_doc.createElement("DataType")
        DataType.appendChild(main_doc.createTextNode(dt['data_type_name']))
        ProjectDataType.appendChild(DataType)

    ProjectTypeSubmission.appendChild(ProjectDataType)
    ProjectType.appendChild(ProjectTypeSubmission)
    Project.appendChild(ProjectType)

    # Add Release data and submitter
    ProjectRelease = main_doc.createElement("ReleaseDate")
    ProjectRelease.appendChild(main_doc.createTextNode(prj_release_date))
    prjadddata.appendChild(ProjectRelease)

    Submitter = generate_submitter(prj_user_info, main_doc)
    prjadddata.appendChild(Submitter)

    return prjaction


# generate biosample
def generate_biosample(
        target_db,
        spuid,
        sample_package,
        sample_taxonname,
        sample_user_info,
        sample_related_prj,
        sample_with_attr,
        main_doc):

    sam_action, sam_adddata, sam_data, sam_xmlcontent = generate_action_tree(target_db, spuid, main_doc)
    BioSample = main_doc.createElement("BioSample")
    BioSample.setAttribute("schema_version", "2.0")
    sam_xmlcontent.appendChild(BioSample)

    # sample id
    SampleId = main_doc.createElement("SampleId")
    SPUID = main_doc.createElement("SPUID")
    SPUID.setAttribute("spuid_namespace", "NGDC")
    SPUID.appendChild(main_doc.createTextNode(spuid))
    SampleId.appendChild(SPUID)
    BioSample.appendChild(SampleId)

    # Descriptor
    Descriptor = main_doc.createElement("Descriptor")
    Title = main_doc.createElement("Title")
    Title.appendChild(main_doc.createTextNode(sample_with_attr['name']))
    Descriptor.appendChild(Title)
    BioSample.appendChild(Descriptor)

    # Organism ##20240105 检测如果是metagenome类型，并且不是以metagenome结尾，需要修改为metagenome
    Organism = main_doc.createElement("Organism")
    OrganismName = main_doc.createElement("OrganismName")
    if (int(sample_with_attr['sample_type_id']) in [8, 9, 10, 11, 12] and
            str(sample_taxonname['name_txt']).endswith("metagenome") is False):

        if int(sample_with_attr['sample_type_id']) == 8:
            OrganismName.appendChild(main_doc.createTextNode("metagenome"))
        elif int(sample_with_attr['sample_type_id']) == 9:
            OrganismName.appendChild(
                main_doc.createTextNode("human gut metagenome"))
        elif int(sample_with_attr['sample_type_id']) == 10:
            OrganismName.appendChild(main_doc.createTextNode("soil metagenome"))
        elif int(sample_with_attr['sample_type_id']) == 11:
            OrganismName.appendChild(main_doc.createTextNode("water metagenome"))
    elif int(sample_with_attr['sample_type_id']) in [3]:  ## 20240105 microbe类型，不能包含metagenome Microbiota等
        if (str(sample_taxonname['name_txt']).endswith("metagenome") is True or
                str(sample_taxonname['name_txt']) in ["Microbiota", "Eukaryota", "unclassified sequences"]):
            OrganismName.appendChild(
                main_doc.createTextNode("Bacteria"))
        else:
            OrganismName.appendChild(
                main_doc.createTextNode(sample_taxonname['name_txt']))
    else:
        OrganismName.appendChild(
            main_doc.createTextNode(sample_taxonname['name_txt']))
    Organism.appendChild(OrganismName)
    BioSample.appendChild(Organism)

    # Package
    Package = main_doc.createElement("Package")
    Package.appendChild(main_doc.createTextNode(sample_package))
    BioSample.appendChild(Package)

    # attribute
    Attributes = main_doc.createElement("Attributes")
    GSA2NCBI_attr_mapping = {
        "cultivar": "cultivar",
        "biomaterial_provider": "biomaterial_provider",
        "tissue": "tissue",
        "age": "age",
        "cell_line": "cell_line",
        "cell_type": "cell_type",
        "collected_by": "collected_by",
        "collection_date": "collection_date",
        "culture_collection": "culture_collection",
        "dev_stage": "dev_stage",
        "disease": "disease",
        "disease_stage": "disease_stage",
        "genotype": "genotype",
        "growth_protocol": "growth_protocol",
        "height_length": "height_or_length",
        "isolation_source": "isolation_source",
        "latitude_longitude": "lat_lon",
        "phenotype": "phenotype",
        "population": "population",
        "specimen_voucher": "specimen_voucher",
        "treatment": "treatment",
        "isolate": "isolate",
        "strain": "strain",
        "host_organism_id": "host",  # 2022年8月4日 update  host_taxid -> host
        "lab_host": "lab_host",
        "geographic_location": "geo_loc_name",
        "altitude": "altitude",
        "depth": "depth",
        "host_tissue_sampled": "host_tissue_sampled",
        "identified_by": "identified_by",
        "passage_history": "passage_history",
        "sample_size": "samp_size",
        "serotype": "serotype",
        "serovar": "serovar",
        "subgroup": "subgroup",
        "subtype": "subtype",
        "host_disease": "host_disease",
        "host_age": "host_age",
        "host_description": "host_description",
        "host_disease_outcome": "host_disease_outcome",
        "host_disease_stage": "host_disease_stage",
        "host_health_state": "host_health_state",
        "host_subject_id": "host_subject_id",
        "pathotype": "pathotype",
        "breed": "breed",
        "birth_date": "birth_date",
        "birth_location": "birth_location",
        "breed_history": "breeding_history",
        "breed_method": "breeding_method",
        "cell_subtype": "cell_subtype",
        "death_date": "death_date",
        "health_state": "health_state",
        "storage_conditions": "store_cond",
        "stud_book_number": "stud_book_number",
        "elevation": "elev",
        "agrochemical_additions": "agrochem_addition",
        "aluminium_saturation": "al_sat",
        "aluminium_saturation_method": "al_sat_meth",
        "annual_seasonal_precipitation": "annual_season_precpt",
        "annual_seasonal_temperature": "annual_season_temp",
        "crop_rotation": "crop_rotation",
        "current_vegetation": "cur_vegetation",
        "current_vegetation_method": "cur_vegetation_meth",
        "drainage_classification": "drainage_class",
        "extreme_event": "extreme_event",
        "extreme_salinity": "extreme_salinity",
        "fao_classification": "fao_class",
        "fire": "fire",
        "flooding": "flooding",
        "heavy_metals": "heavy_metals",
        "heavy_metals_method": "heavy_metals_meth",
        "horizon": "horizon",
        "horizon_method": "horizon_meth",
        "links_additional_analysis": "link_addit_analys",
        "link_classification_information": "link_class_info",
        "link_climate_information": "link_climate_info",
        "local_classification": "local_class",
        "local_classification_method": "local_class_meth",
        "microbial_biomass": "microbial_biomass",
        "microbial_biomass_method": "microbial_biomass_meth",
        "miscellaneous_parameter": "misc_param",
        "ph": "ph",
        "ph_method": "ph_meth",
        "pooling_dna_extracts": "pool_dna_extracts",
        "previous_land_use": "previous_land_use",
        "previous_land_use_method": "previous_land_use_meth",
        "profile_position": "profile_position",
        "salinity_method": "salinity_meth",
        "sieving": "sieving",
        "slope_aspect": "slope_aspect",
        "soil_type": "soil_type",
        "slope_gradient": "slope_gradient",
        "soil_type_method": "soil_type_meth",
        "texture": "texture",
        "texture_method": "texture_meth",
        "tillage": "tillage",
        "total_n_method": "tot_n_meth",
        "total_nitrogen": "tot_nitro",
        "total_organic_carbon_method": "tot_org_c_meth",
        "total_organic_carbon": "tot_org_carb",
        "water_content_soil": "water_content_soil",
        "water_content_soil_method": "water_content_soil_meth",
        "reference_biomaterial": "ref_biomaterial",
        "sample_collection_device": "samp_collect_device",
        "sample_material_processing": "samp_mat_process",
        "source_material_identifiers": "source_material_id",
        "description": "description",
        "chemical_administration": "chem_administration",
        "ethnicity": "ethnicity",
        "gastrointestinal_tract_disorder": "gastrointest_disord",
        "host_mass_index": "host_body_mass_index",
        "host_product": "host_body_product",
        "host_temperature": "host_body_temp",
        "host_diet": "host_diet",
        "host_family_relationship": "host_family_relationship",
        "host_genotype": "host_genotype",
        "host_height": "host_height",
        "host_last_meal": "host_last_meal",
        "host_occupation": "host_occupation",
        "host_phenotype": "host_phenotype",
        "host_pulse": "host_pulse",
        "host_total_mass": "host_tot_mass",
        "medication_code": "ihmc_medication_code",
        "liver_disorder": "liver_disord",
        "medical_history_performed": "medic_hist_perform",
        "organism_count": "organism_count",
        "perturbation": "perturbation",
        "salinity": "salinity",
        "sample_storage_duration": "samp_store_dur",
        "sample_storage_location": "samp_store_loc",
        "sample_storage_temperature": "samp_store_temp",
        "special_diet": "special_diet",
        "mating_type": "mating_type",
        "alkalinity": "alkalinity",
        "alkyl_diethers": "alkyl_diethers",
        "aminopeptidase_activity": "aminopept_act",
        "ammonium": "ammonium",
        "atmospheric_data": "atmospheric_data",
        "bacterial_production": "bac_prod",
        "bacterial_respiration": "bac_resp",
        "bacterial_carbon_production": "bacteria_carb_prod",
        "biomass": "biomass",
        "bishomohopanol": "bishomohopanol",
        "bromide": "bromide",
        "calcium": "calcium",
        "carbon_nitrogen_ratio": "carb_nitro_ratio",
        "chloride": "chloride",
        "chlorophyll": "chlorophyll",
        "conductivity": "conduc",
        "density": "density",
        "diether_lipids": "diether_lipids",
        "dissolved_carbon_dioxide": "diss_carb_dioxide",
        "dissolved_hydrogen": "diss_hydrogen",
        "dissolved_inorganic_carbon": "diss_inorg_carb",
        "dissolved_inorganic_nitrogen": "diss_inorg_nitro",
        "dissolved_inorganic_phosphorus": "diss_inorg_phosp",
        "dissolved_organic_carbon": "diss_org_carb",
        "dissolved_organic_nitrogen": "diss_org_nitro",
        "dissolved_oxygen": "diss_oxygen",
        "downward_par": "down_par",
        "fluorescence": "fluor",
        "glucosidase_activity": "glucosidase_act",
        "light_intensity": "light_intensity",
        "magnesium": "magnesium",
        "mean_friction_velocity": "mean_frict_vel",
        "mean_peak_friction_velocity": "mean_peak_frict_vel",
        "n_alkanes": "n_alkanes",
        "nitrate": "nitrate",
        "nitrite": "nitrite",
        "nitrogen": "nitro",
        "organic_carbon": "org_carb",
        "organic_matter": "org_matter",
        "organic_nitrogen": "org_nitro",
        "oxygenation_status": "oxy_stat_samp",
        "particulate_organic_carbon": "part_org_carb",
        "particulate_organic_nitrogen": "part_org_nitro",
        "petroleum_hydrocarbon": "petroleum_hydrocarb",
        "phaeopigments": "phaeopigments",
        "phosphate": "phosphate",
        "phospholipid_fatty_acid": "phosplipid_fatt_acid",
        "photon_flux": "photon_flux",
        "potassium": "potassium",
        "pressure": "pressure",
        "primary_production": "primary_prod",
        "redox_potential": "redox_potential",
        "silicate": "silicate",
        "sodium": "sodium",
        "soluble_reactive_phosphorus": "soluble_react_phosp",
        "sulfate": "sulfate",
        "sulfide": "sulfide",
        "suspended_particulate_matter": "suspend_part_matter",
        "tidal_stage": "tidal_stage",
        "total_depth_water_column": "tot_depth_water_col",
        "total_dissolved_nitrogen": "tot_diss_nitro",
        "total_inorganic_nitrogen": "tot_inorg_nitro",
        "total_particulate_carbon": "tot_part_carb",
        "total_phosphorus": "tot_phosp",
        "water_current": "water_current",
        "sex": "sex",
        "host_sex": "host_sex",
        "sample_volume_weight_dna_extraction": "samp_vol_we_dna_ext",

        # add additional attribute in 2025-03-24 by XiaolongZhang
        "host_sex_id": 'host_sex',
        "current_land_use_id": "cur_land_use",
        "environment_biome": "env_broad_scale",
        "environment_feature": "env_local_scale",
        "environment_material": "env_medium",
        "oxygenation_status_id": "oxy_stat_samp",
        "oxygenation_status": "oxy_stat_samp",
        "relationship_oxygen_id": "rel_to_oxygen",
        "salinity": "samp_salinity",
        "sample_volume_weight": "samp_vol_we_dna_ext"
    }

    GSA2NCBI_attr_sex_mapping = {
        "1": "male",
        "2": "female",
        "3": "neuter",
        "4": "hermaphrodite",
        "5": "not determined",
        "6": "missing",
        "7": "not applicable",
        "8": "not collected"
    }
    # 20240110 增加tillage的映射
    tillage_mapping = {
        "1": "drill",
        "2": "cutting disc",
        "3": "ridge till",
        "4": "strip tillage",
        "5": "zonal tillage",
        "6": "chisel",
        "7": "tined",
        "8": "mouldboard",
        "9": "disc plough"
    }

    # 20250324 增加current_land_use_id映射表 by XiaolongZhang
    land_use_mapping = {
        "1": "cities",
        "2": "farmstead",
        "3": "industrial areas",
        "4": "roads/railroads",
        "5": "rock",
        "6": "sand",
        "7": "gravel",
        "8": "mudflats",
        "9": "salt flats",
        "10": "badlands",
        "11": "permanent snow or ice",
        "12": "saline seeps",
        "13": "mines/quarries",
        "14": "oil waste areas",
        "15": "small grains",
        "16": "row crops",
        "17": "vegetable crops",
        "18": "horticultural plants (e.g. tulips)",
        "19": "marshlands (grass, sedges, rushes)",
        "20": "tundra (mosses, lichens)",
        "21": "rangeland",
        "22": "pastureland (grasslands used for livestock grazing)",
        "23": "hayland",
        "24": "meadows (grasses, alfalfa, fescue, bromegrass, timothy)",
        "25": "shrub land (e.g. mesquite, sage-brush, creosote bush, shrub oak, eucalyptus)",
        "26": "successional shrub land (tree saplings, hazels, sumacs, chokecherry, shrub dogwoods, blackberries)",
        "27": "shrub crops (blueberries, nursery ornamentals, filberts)",
        "28": "vine crops (grapes)",
        "29": "conifers (e.g. pine, spruce, fir, cypress)",
        "30": "hardwoods (e.g. oak, hickory, elm, aspen)",
        "31": "intermixed hardwood and conifers",
        "32": "tropical (e.g. mangrove, palms)",
        "33": "rainforest (evergreen forest receiving >406 cm annual rainfall)",
        "34": "swamp (permanent or semi-permanent water body dominated by woody plants)",
        "35": "crop trees (nuts, fruit, christmas trees, nursery trees)"
    }

    # 20250324 增加oxygenation_status_id映射表, 1->aerobe, 2->anaerobe
    oxygenation_status_mapping = {
        "1": "aerobe",
        "2": "anaerobe"
    }

    # 20250324 增加relationship_oxygen_id映射表
    relationship_oxygen_mapping = {
        "1": "aerobe",
        "2": "anaerobe",
        "3": "facultative",
        "4": "microaerophilic",
        "5": "microanaerobe",
        "6": "obligate aerobe",
        "7": "obligate anaerobe"
    }

    mysqlutils = MysqlUtils()

    # 20240104 修复package中标签重复的问题
    if sample_package in ["Metagenome.environmental.1.0", "Microbe.1.0"]:
        if sample_with_attr['attrs']["host_organism_id"] is None and sample_with_attr['attrs'][
            "isolation_source"] is not None:
            del sample_with_attr['attrs']["host_organism_id"]
        elif sample_with_attr['attrs']["isolation_source"] is None and sample_with_attr['attrs'][
            "host_organism_id"] is not None:
            del sample_with_attr['attrs']["isolation_source"]
        else:
            del sample_with_attr['attrs']["isolation_source"]
    elif sample_package in ["Pathogen.cl.1.0"]:
        if sample_with_attr['attrs']["isolate"] is None and sample_with_attr['attrs']["strain"] is not None:
            del sample_with_attr['attrs']["isolate"]
        elif sample_with_attr['attrs']["strain"] is None and sample_with_attr['attrs']["isolate"] is not None:
            del sample_with_attr['attrs']["strain"]
        else:
            del sample_with_attr['attrs']["strain"]
    elif sample_package in ["MIMS.me.human-gut.6.0"]:
        if sample_with_attr['attrs']["host_organism_id"] != 9606:
            sample_with_attr['attrs']["host_organism_id"] = 9606
    elif sample_package in ["Virus.1.0"]:
        if sample_with_attr['attrs']["host_organism_id"] is None and sample_with_attr['attrs']["lab_host"] is not None:
            del sample_with_attr['attrs']["host_organism_id"]
        elif sample_with_attr['attrs']["lab_host"] is None and sample_with_attr['attrs'][
            "host_organism_id"] is not None:
            del sample_with_attr['attrs']["lab_host"]
        else:
            del sample_with_attr['attrs']["lab_host"]

    for k, v in sample_with_attr['attrs'].items():
        NCBIattrName = GSA2NCBI_attr_mapping.get(k, None)
        if not NCBIattrName or not v or not str(v).strip():
            continue

        attr = main_doc.createElement("Attribute")
        attr.setAttribute("attribute_name", NCBIattrName)
        attr_key = str(k).strip().lower()
        this_attr = "missing"

        if attr_key == "collection_date":
            if isinstance(v, str):
                try:
                    this_attr = v.split(" ")[0]
                except:
                    this_attr = "missing"
            elif isinstance(v, datetime.datetime):
                this_attr = v.strftime("%Y-%m-%d")

        elif attr_key == "age":
            pattern = r'^(\d+(?:\.\d+)?)\s*(?:(second|minute|hour|day|week|month|year)s?)?$'
            try:
                age_tuple = re.findall(pattern, str(v))[0]
                if age_tuple[1] == "":  # e.g. '12345' -> ('12345', '')
                    this_unit = sample_with_attr['attrs'].get("age_unit", None)
                    if this_unit is not None:
                        this_attr = str(v) + " " + this_unit
                else:  # e.g. '12345 years' -> ('12345', 'year')
                    this_attr = age_tuple[0] + " " + age_tuple[1]
            except IndexError:
                this_attr = "missing"

        elif attr_key == "host_age":
            pattern = r'^(\d+(?:\.\d+)?)\s*(?:(second|minute|hour|day|week|month|year)s?)?$'
            try:
                age_tuple = re.findall(pattern, str(v))[0]
                if age_tuple[1] == "":
                    this_unit = sample_with_attr['attrs'].get("host_age_unit", None)
                    if this_unit is not None:
                        this_attr = str(v) + " " + this_unit
                else:
                    this_attr = age_tuple[0] + " " + age_tuple[1]
            except IndexError:
                this_attr = "missing"

        elif attr_key == "host_organism_id":  # 2022年8月4日 这里在查一下表，将taxid 转换taxname
            this_unit = sample_with_attr['attrs'].get('host_organism_id', None)
            if this_unit and this_unit > 0:
                taxon_name = mysqlutils.select_taxon_name_by_taxonid(v)
                this_attr = str(taxon_name['name_txt'])

        # 20231110:virus中的这个字段有特殊的校验规则，GSA的都不符合，因此替换为missing https://www.insdc.org/submitting-standards/controlled-vocabulary-culturecollection-qualifier/
        elif attr_key == "culture_collection":
            this_attr = "missing"

        # 20231110:virus中的这个字段有特殊的校验规则，GSA的都不符合，因此替换为missing http://www.insdc.org/controlled-vocabulary-specimenvoucher-qualifier
        elif attr_key == "specimen_voucher":
            this_attr = "missing"

        elif attr_key == "tillage":
            this_attr = tillage_mapping.get(str(v), "missing")

        # sex,规则是，把数字替换成后面的字
        # 1 male
        # 2 female
        # 3 neuter
        # 4 hermaphrodite
        # 5 not determined
        # 6 missing
        # 7 not applicable
        # 8 not collected
        elif attr_key in {"sex", "host_sex", "host_sex_id"}:  # 20250324 增加host_sex的映射
            this_attr = GSA2NCBI_attr_sex_mapping.get(str(v), "missing")

        # add additional attribute for the bio-sample at 20250324 by XiaolongZhang
        elif attr_key == "current_land_use_id":
            this_attr = land_use_mapping.get(str(v), "missing")

        elif attr_key in {"oxygenation_status_id", "oxygenation_status"}:
            this_attr = oxygenation_status_mapping.get(str(v), "missing")

        elif attr_key == "relationship_oxygen_id":
            this_attr = relationship_oxygen_mapping.get(str(v), "missing")

        else:
            this_attr = str(v)

        if this_attr != 'missing':  # skip the attribute that has value of 'missing'
            innerT = main_doc.createTextNode(this_attr)
            attr.appendChild(innerT)
            Attributes.appendChild(attr)

    BioSample.appendChild(Attributes)

    # add sample related bio-project
    RelatedProject = main_doc.createElement("RelatedProject")
    ProjectID = main_doc.createElement("ProjectID")
    SPUID = main_doc.createElement("SPUID")
    SPUID.setAttribute("spuid_namespace", "NGDC")
    SPUID.appendChild(main_doc.createTextNode(sample_related_prj))
    ProjectID.appendChild(SPUID)
    RelatedProject.appendChild(ProjectID)
    sam_adddata.appendChild(RelatedProject)

    # Add release data and submitter
    SampleRelease = main_doc.createElement("ReleaseDate")
    release_date = sample_with_attr['release_time'].strftime("%Y-%m-%d")
    SampleRelease.appendChild(main_doc.createTextNode(release_date))
    sam_adddata.appendChild(SampleRelease)

    Submitter = generate_submitter(sample_user_info, main_doc)
    sam_adddata.appendChild(Submitter)

    return sam_action

