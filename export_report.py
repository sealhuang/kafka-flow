# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import json
from configparser import ConfigParser


def get_report_gallery(config_file='./report_gallery.config'):
    """Read report gallery config file."""
    config = ConfigParser()
    config.read(config_file)

    return config

def export_pdfs(report_type, base_dir):
    """Export and rename pdf files."""
    # user data dir
    data_dir = os.path.join(base_dir, 'user_data')
    if not os.path.exists(data_dir):
        print('The directory `%s/user_data` does not exists.'%(report_type))
        return None
    # pdf dir
    pdf_dir = os.path.join(base_dir, 'pdfs')
    if not os.path.exists(pdf_dir):
        print('The directory `%s/pdfs` does not exists.'%(report_type))
        return None

    # export dir
    export_dir = os.path.join(os.path.curdir, '%s_exports'%(report_type))
    if not os.path.exists(export_dir):
        os.makedirs(export_dir, mode=0o755)

    # get json and pdf files
    json_list = os.listdir(data_dir)
    pdf_list = os.listdir(pdf_dir)

    for json_file in json_list:
        try:
            jf = os.path.join(data_dir, json_file)
            with open(jf) as f:
                user_data = json.load(f)
        except:
            print('Error while reading file %s'%(jf))
            continue

        # get ticket id
        ticket_id = user_data['ticketID']
        # get user info
        user_vals = []
        if ('school' in user_data) and user_data['school']:
            user_vals.append(user_data['school'])
        if ('grade' in user_data) and ('class' in user_data) and \
            user_data['grade'] and user_data['class']:
            user_vals.append('-'.join([user_data['grade'], user_data['class']]))
        else:
            if 'grade' in user_data and user_data['grade']:
                user_vals.append(user_data['grade'])
            if 'class' in user_data and user_data['class']:
                user_vals.append(user_data['class'])
        if 'name' in user_data and user_data['name']:
            user_vals.append(user_data['name'])

        # remove blanks
        user_vals = [item.replace(' ', '-') for item in user_vals]

        prefix_ = 'report_'+ticket_id+'_'
        for pf in pdf_list:
            if pf.startswith(prefix_):
                ts = pf.split('.')[0].split('_')[-1]
                efile = os.path.join(export_dir,'_'.join(user_vals+[ts])+'.pdf')
                sfile = os.path.join(pdf_dir, pf)
                cmd_str = 'cp %s %s'%(sfile, efile)
                #print(cmd_str)
                os.system(cmd_str)

def export_pdfs_1(base_dir, ticket_id, new_pdf):
    """Export and rename pdf files."""
    # pdf dir
    pdf_dir = os.path.join(base_dir, 'pdfs')
    if not os.path.exists(pdf_dir):
        print('The directory `%s/pdfs` does not exists.'%(report_type))
        return None

    # get pdf files
    pdf_list = os.listdir(pdf_dir)

    prefix_ = 'report_'+ticket_id+'_'
    for pf in pdf_list:
        if pf.startswith(prefix_):
            ts = pf.split('.')[0].split('_')[-1]
            efile = new_pdf+'_'+ts+'.pdf'
            sfile = os.path.join(pdf_dir, pf)
            cmd_str = 'cp %s %s'%(sfile, efile)
            #print(cmd_str)
            os.system(cmd_str)


if __name__ == '__main__':
    # get report config info
    report_config_file = os.path.join(os.path.curdir, 'report_gallery.config')
    report_gallery = get_report_gallery(report_config_file)

    #-- report-type based
    ## user input
    #report_type = 'mathDiagnosisK8_v1'

    #if report_type not in report_gallery:
    #    print('Error! Not find report type named %s.'%(report_type))
    #else:
    #    report_cfg = report_gallery[report_type]

    #    # rename and export pdf files 
    #    export_pdfs(report_type, report_cfg['base_dir'])

    #-- local-file based
    local_dir = os.path.join(os.path.curdir, 'local_msgs', 'baxueyuan_20201027')
    export_dir = os.path.join(local_dir, 'exports')
    if not os.path.exists(export_dir):
        os.makedirs(export_dir, mode=0o755)

    user_info_file = os.path.join(local_dir, 'user_info.csv')
    user_info = open(user_info_file).readlines()
    user_info = [line.strip().split(',') for line in user_info]
    for line in user_info:
        report_type = line[0]
        ticket_id = line[1]
        user_school = line[2].replace(' ', '')
        user_grade = line[3].replace(' ', '')
        user_class = line[4].replace(' ', '')
        user_name = line[5].replace(' ', '')
        new_pdf = os.path.join(
            export_dir,
            '%s_%s_%s_%s'%(user_school, user_grade, user_class, user_name),
        )

        if report_type not in report_gallery:
            print('Error! Not find report type named %s.'%(report_type))
        else:
            report_cfg = report_gallery[report_type]

            # rename and export pdf files
            export_pdfs_1(report_cfg['base_dir'], ticket_id, new_pdf)


