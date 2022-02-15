# -*- coding: utf-8 -*-
import re


class DataTreatment:
    """
    Class responsible for treating dataframe data coming from ReadFiles class;
    """

    def data_cleaning(self, df, df_filename):
        """
        Method responsible for convert all columns to string and remove leading and trailing spaces;
        It also works as the 'main' method of the class, redirecting the df to other static methods;
        :param df: dataframe coming from the ReadFiles class
        :param df_filename: filename corresponded to the file that created the dataframe
        :return: treated datafame in its final version, ready to be saved
        """
        df_copy = df.copy()
        columns = list(df.columns)
        # task3
        for column in columns:
            df_copy[column] = df_copy[column].astype(dtype=str)  # convert all columns to str type
            df_copy[column] = df_copy[column].str.strip()  # removes all leading and trailing spaces

        df_copy = self.create_city_ascii(df_copy, df_filename)
        df_copy = self.phone_number_treatment(df_copy, df_filename)

        return df_copy

    @staticmethod
    def create_city_ascii(df_copy, df_filename):
        """
        This method is responsible for creating a new column named 'CITY_ASCII', which is a copy of the 'CITY' column
        but without special_character, numbers and lowercase_characters; It also replaces all non 'ascii'
        compatibility characters with their equivalents
        :param df_copy: dataframe coming from the "data_cleaning" method
        :param df_filename: filename corresponded to the file that created the dataframe
        :return: df_copy dataframe with its respective new column
        """
        try:
            # task4
            df_copy["CITY_ASCII"] = df_copy['CITY']
            df_copy['CITY_ASCII'] = df_copy['CITY_ASCII'] = df_copy['CITY_ASCII'].str.normalize('NFKD') \
                                                                                 .str.encode('ascii', 'ignore') \
                                                                                 .str.decode('utf-8') \
                                                                                 .str.upper()
            # removes special character but the hyphen
            df_copy['CITY_ASCII'] = df_copy['CITY_ASCII'].map(lambda x: re.sub(r'[^a-zA-Z.\d\s-]+', '', x))

        except Exception as error:
            print(f"Something went wrong while creating the 'CITY_ASCII' column on file -> {df_filename}")
            raise error

        return df_copy

    @staticmethod
    def phone_number_treatment(df_copy, df_filename):
        """
        This method is responsible for removing non numeric characters from the 'PHONE' column
        :param df_copy: dataframe coming from the "data_cleaning" method
        :param df_filename: filename corresponded to the file that created the dataframe
        :return :df_copy dataframe with its respective treated column
        """
        try:
            # task5
            df_copy['PHONE'] = df_copy['PHONE'].map(lambda x: re.sub('[^0-9]', '', x))

        except Exception as error:
            print(f"Something went wrong while treating the 'PHONE' column on file -> {df_filename}")
            raise error

        return df_copy
