# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-11 14:17:25
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-11 15:36:26

DC_LABEL_NAME = 'label'

class Labeller(object):

    @staticmethod
    def label(unlabelled_data_path, labelled_data_path):
        reader = csv.reader(codecs.open(unlabelled_data_path, 'r'))
        # print reader.fieldnames
        header = next(reader)
        dataset = []
        for row in reader:
            # print info
            os.system('cls' if os.name == 'nt' else 'clear')
            print '#'*50
            print '# Post Features'
            print '#'*50
            for i in range(1, len(header)):
                print header[i]+':', row[header.index(header[i])]
            print '#'*50
            print '# Post Content'
            print '#'*50
            print row[0]
            print '#'*50
            
            # ask user to label
            user_defined_label = raw_input('Please enter the label: (others: 1, massage: 2, escort: 3, job_ads: 4)\n')
            print 'You enter:', user_defined_label
            print '#'*50

            # load dict for data
            data = {DC_LABEL_NAME: user_defined_label}
            for field in header:
                data.setdefault(field, row[header.index(field)])
            dataset.append(data)

        with open(labelled_data_path, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=[DC_LABEL_NAME]+header)
            writer.writeheader()
            for data in dataset:
                writer.writerow(data)


if __name__ == '__main__':
    from loader import *
    Labeller.label(DC_DEFAULT_UNLABELLED_DATA_FILEPATH, DC_DEFAULT_LABELLED_DATA_FILEPATH)
