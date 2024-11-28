import pandas as pd
import dask.dataframe as dd
import argparse


class MissingValueError(Exception):
    pass


class GoldenRecord:
    def __init__(self, csv_path):
        self.df = self.open_and_filter_csv(csv_path)
        self.list_columns = self.df.columns

    def open_and_filter_csv(self, csv_path):
        '''В столбцах ниже типы данных перемешаны, потому указываем, чтоб по умолчанию
        все превращал в object'''

        dtype = {
            'addr_area': 'object',
            'addr_body': 'object',
            'addr_flat': 'object',
            'addr_house': 'object',
            'addr_loc': 'object',
            'addr_zip': 'object',
            'contact_tg': 'object',
            'contact_other': 'object',
            'contact_vc': 'object',
            'fin_loan_begin_dt': 'object',
            'fin_loan_end_dt': 'object'
        }

        # Преобразование в pandas
        df = dd.read_csv(csv_path, dtype=dtype, low_memory=False).drop_duplicates().compute()

        # Удаление продуктов, товаров
        df = df[df['client_first_name'].str.isdigit() == False]

        # Преобразование в нижний регистр
        string_cols = df.select_dtypes(include='object').columns
        df[string_cols] = df[string_cols].apply(self.to_lowercase)

        # Применяем функцию для заполнения ФИО
        df = df.apply(self.fill_client_fio, axis=1)

        return df

    def to_lowercase(self, series):
        return series.str.lower()

    def fill_client_fio(self, row):
        if not pd.isna(row['client_first_name']) and not pd.isna(row['client_middle_name']) and not pd.isna(
                row['client_last_name']):
            full_name = f"{row['client_last_name']} {row['client_first_name']} {row['client_middle_name']}"
            if full_name != row['client_fio_full']:
                row['client_fio_full'] = full_name
        elif not pd.isna(row['client_fio_full']):
            names = row['client_fio_full'].split()
            row['client_last_name'], row['client_first_name'], row['client_middle_name'] = (
                names[0],
                names[1],
                ' '.join(names[2:]) if len(names) > 2 else None
            )
        return row

    def process_dataframe(self):
        result = pd.DataFrame(columns=self.list_columns)

        duplicates = self.df[self.df.duplicated('client_fio_full', keep=False)]
        list_index = list(duplicates['client_fio_full'].index)
        subset = self.df.drop(list_index)
        subset_na = self.df[self.df['client_fio_full'].isna()]

        result = pd.concat([result, subset])
        result = pd.concat([result, subset_na])

        # Группируем строки по email
        grouped = duplicates.groupby("client_fio_full").agg({
            i: 'first' for i in self.list_columns if i != 'client_fio_full'
        }).reset_index('client_fio_full')
        result = pd.concat([result, grouped])
        return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Добро пожаловать GoldRecordEXE! Введите -h или --help для получения списка команд.'
    )

    parser.add_argument('--send', action='store',
                        help='Отправляет путь csv-файла, из которого будет получена \"Золотая запись\".')
    parser.add_argument('--save', action='store',
                        help='Сохраняет \"Золотую запись\" в качестве csv-файла по указанному пути.')
    args = parser.parse_args()

    result = None

    if args.send:
        print('Загрузка началась!')
        golden_record = GoldenRecord(args.send)
        try:
            result = golden_record.process_dataframe()
            print(result)
        except Exception as e:
            print('В процессе проведения анализа данных произошла ошибка:', e)
        else:
            print('\n\"Золотая запись\" успешно получена!')

    if args.save:
        if result is not None:
            print('Сохраняем файл.')
            try:
                result.to_csv(args.save, encoding='utf-8')
            except FileNotFoundError:
                raise FileNotFoundError('Путь не найден.')
            else:
                print('Файл успешно сохранён!')
        else:
            raise MissingValueError('Данные отсутствуют.')