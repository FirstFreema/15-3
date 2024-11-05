from engine_HH import HH
import json


class DataReceiver:

    def __init__(self, employers: list):
        self.employers = employers

    def get_employers(self):
        employers_hh = []
        for employer in self.employers:
            employers_hh.append(HH(employer).get_employer())
        return employers_hh

    def get_vacancies(self):
        vacancies_list = []
        employers_hh = self.get_employers()
        for i in range(len(employers_hh)):
            vacancies_list.append(HH(self.employers[i]).get_vacancies(employers_hh[i]['id']))
        return vacancies_list

    @staticmethod
    def normalize_vacancies(vacancies):
        vacancies_list_all = []
        for vacancy_emp in vacancies:
            vacancies_list_all.extend(vacancy_emp)
        return vacancies_list_all

    @staticmethod
    def normalize_salary(normalized_vacancies):
        for vacancy in normalized_vacancies:
            if vacancy['salary_from'] is None and vacancy['salary_to'] is None:
                vacancy['salary_from'] = 0
                vacancy['salary_to'] = 0
            elif vacancy['salary_from'] is None:
                vacancy['salary_from'] = 0
            elif vacancy['salary_to'] is None:
                vacancy['salary_to'] = 0
        return normalized_vacancies

    @staticmethod
    def save_data_to_json(filename, data):
        with open(f'{filename}.json', 'w', encoding='utf-8') as fp:
            json.dump(data, fp, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    employers = ['skyeng', 'skillbox', 'лаборатория касперского', 'lesta games', 'Вконтакте', 'LG Electronics Inc.',
                 'SberTech', 'YADRO', 'Доктор Веб']
    my_data_receiver = DataReceiver(employers)
    my_employers = my_data_receiver.get_employers()
    my_vacancies = my_data_receiver.get_vacancies()
    my_normalized_vacancies = my_data_receiver.normalize_vacancies(my_vacancies)
    my_normalized_vacancies = my_data_receiver.normalize_salary(my_normalized_vacancies)
    my_data_receiver.save_data_to_json(r'data/employers', my_employers)
    my_data_receiver.save_data_to_json(r'data/vacancies', my_normalized_vacancies)
