# -*- coding: utf-8 -*-
import os
from pathlib import Path
import cchardet as chardet
import pandas as pd
from datetime import date
from data_treatment import DataTreatment


class ReadFiles:
    """
    Class responsible for reading, cleaning and saving csv files following pre accorded format patterns;
    """

    def __init__(self):
        self.csv_path = "C:\\Users\\Kiy\\Documents\\PycharmProjects\\Teste-Pmweb\\data_clearence_natal2021\\raw_csv_files\\"
        self.dataframes_path = "C:\\Users\\Kiy\\Documents\\PycharmProjects\\Teste-Pmweb\\data_clearence_natal2021\\treated_csv_files\\"
        self.data_treatment = DataTreatment()
        self.concat_dataframes = list()

    def get_encoding(self, file):
        """
        Method responsible for getting the encoding of a csv file
        :param file: csv file to be opened
        :return: string corresponded to the file's encoding
        """
        # task1
        filepath = self.csv_path + file
        filename = Path(filepath)
        # must read the file as binary so the 'chardet' function can detect its encoding
        try:
            read_bytes = filename.read_bytes()
            detect = chardet.detect(read_bytes)
        except UnicodeDecodeError:
            print(f"Unidecode error on file '{file}', trying with different encoding")
        else:
            encoding = detect['encoding']
            confidence = detect['confidence']
            if confidence < 0.8:
                print(f"WARNING - the 'chardet' function confidence percentage that the correct encoding is \
                      '{encoding}' is below 80%, double check the output!")
            else:
                print(f"File '{file}' encoding is {encoding} with a confidence percentage of "
                      f"{'{:.2%}'.format(confidence)}!\n")
                return encoding

    def process_csv(self):
        """
        Method responsible for reading csv files, processing and redirecting to different methods of this class;
        :return:
        """
        # task2
        file_dir = os.listdir(self.csv_path)  # returns a list of files inside the respective directory
        for file in file_dir:
            print(f"Starting process to read and clean '{file}' file\n")
            encoding = self.get_encoding(file)
            filepath = self.csv_path + file
            chunksize = 1000

            # the line below will be responsible for reading the full csv into chunks of 1,000 lines
            chunk_reader = pd.read_csv(filepath, encoding=encoding, chunksize=chunksize)
            for i, chunk in enumerate(chunk_reader):
                try:
                    df_filename = f'natal2021_{date.today().strftime("%d%m%Y")}_part({i+1}).csv'
                    df = self.data_treatment.data_cleaning(chunk, df_filename)
                    if len(self.concat_dataframes) == 0:
                        self.concat_dataframes.append(df)
                    else:
                        df_concat = pd.concat([self.concat_dataframes[0], df])
                        self.concat_dataframes.pop(0)
                        self.concat_dataframes.append(df_concat)

                except Exception as error:
                    print(f"Something went wrong while reading chunk -> {i}")
                    raise error

            if len(self.concat_dataframes) > 0 and self.concat_dataframes is not None:
                print("Data was cleaned and treated successfully, starting saving process\n")
                self.save_csv()

            else:
                print("No formatted data to be saved!\n")

    def save_csv(self):
        """
        Method responsible for saving the treated dataframe into a new csv file
        :return: print confirming if the save was successful or not
        """
        try:
            # task6
            filepath = self.dataframes_path + 'treated_natal2021.csv'
            self.concat_dataframes[0].to_csv(filepath, index=False, encoding='utf-8')
            print("Saved the treated csv file successfully\n")
            print(f"File saved on path -> {filepath}\n")

        except Exception as error:
            print(f"\nSomething went wrong at the 'save_csv' method")
            raise error


if __name__ == "__main__":
    ReadFiles().process_csv()

