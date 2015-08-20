#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division
import pandas as pd
import numpy as np


def create_headstart_files(working_folder, number_of_papers=None):
    df = pd.read_json(working_folder+"/ads_data.json")
    df.reader_ids = df.reader_ids.astype(str)
    df.sort("readers", ascending=False, inplace=True)

    if not number_of_papers:
        number_of_papers = len(df.index)

    # List of reader id's
    readers = []
    count = 0
    for idx, row in df.iterrows():
        if count == number_of_papers:
            break
        readers.append(row['reader_ids'][1:-1].split(";"))
        count += 1

    # Create metadata.csv
    print("*** Writing metadata.csv")
    metadata = df.iloc[0:number_of_papers]
    print(metadata.describe())
    metadata['id'] = range(1, number_of_papers+1)
    metadata.to_csv(working_folder + "/metadata.csv", sep=",", index=False, encoding="utf8")

    # Co-occurence matrix
    # cooc = np.zeros((len(readers)+1, len(readers)+1), dtype=int)

    # Adjacency list of co-reads
    output = []

    print("*** Creating adjacency list of co-reads")
    max_iter = sum(range(1, len(readers)))
    count = 1
    for idx1, list1 in enumerate(readers, start=1):
        for idx2, list2 in enumerate(readers, start=1):
            if idx2 > idx1:
                print("{} out of {}".format(count, max_iter))
                count += 1
                co_read = len(set(list1) & set(list2))
                if co_read != 0:
                    output.append([idx1, idx2, co_read])
                    output.append([idx2, idx1, co_read])

                    # cooc[idx2, idx1] = co_read
                    #     cooc[idx1, idx2] = co_read
                    # elif idx1 == idx2:
                    #     cooc[idx1, idx1] = 0

    # for i in range(1,len(readers)+1):
    # cooc[0, i] = i
    #     cooc[i, 0] = i

    output = np.array(output)
    np.savetxt(working_folder + "/cooc.csv", output, delimiter=",")

    print("Sparsity: {}".format(len(output)/2/len(readers)**2))

if __name__ == '__main__':
    folder = "E:/Work/Know-Center/CrawlingFramework/files/cs_dl/2015-04-28_18-26-06/2015-04-28_18-30-33/2015-05-26_17-44-44"
    create_headstart_files(folder, number_of_papers=50)