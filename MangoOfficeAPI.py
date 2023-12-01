import time
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Union

from Exceptions import InvalidParameterType, CustomMangoException
from requests import post, HTTPError
from json import dumps

import pandas as pd


class MangoRequest:
    def __init__(
            self,
            api_key: str,
            api_salt: str,
            url: str
    ):
        self.__api_key = api_key
        self.__api_salt = api_salt
        self.__url = url

    @staticmethod
    def __filter_params(
            params: dict
    ) -> dict:
        params = {
            "start_date": params.get('start_date', None),
            "end_date": params.get('end_date', None),
            "user_ids": params.get('user_ids', None),
            "group_ids": params.get('group_ids', None),
            "context_type": params.get('context_type', None),
            "context_status": params.get('context_status', None),
            "recall_status": params.get('recall_status', None),
            "search_string": params.get('search_string', None),
            "ext_params": params.get('ext_params', None),
            "ext_fields": params.get('ext_fields', None),
            "limit": params.get('limit', None),
            "offset": params.get('offset', None)
        }

        filtered_params = {k: v for k, v in params.items() if v is not None}

        valid_types = {
            "start_date": str,
            "end_date": str,
            "user_ids": list,
            "group_ids": list,
            "context_type": list,
            "context_status": int,
            "recall_status": int,
            "search_string": str,
            "ext_params": int,
            "ext_fields": list,
            "limit": str,
            "offset": str
        }

        final_params = {}
        for key, value in filtered_params.items():
            if isinstance(value, valid_types[key]):
                final_params[key] = value
            else:
                raise InvalidParameterType(key, valid_types.get(key))

        return final_params

    @staticmethod
    def __get_df(list_elements: list) -> pd.DataFrame:
        unique_keys = set()
        for entry in list_elements:
            unique_keys.update(entry.keys())

        df_columns = list(unique_keys)
        df = pd.DataFrame(list_elements, columns=df_columns)

        return df

    def __get_sign(
            self,
            json_data: dict
    ) -> sha256:
        json_str = dumps(json_data, separators=(',', ':'), sort_keys=True)

        return sha256((self.__api_key + json_str + self.__api_salt).encode()).hexdigest()

    def __send_post_request(
            self,
            json_data: dict,
            endpoint: str
    ) -> dict:
        payload = {
            'vpbx_api_key': self.__api_key,
            'sign': self.__get_sign(json_data=json_data),
            'json': dumps(json_data, separators=(',', ':'), sort_keys=True)
        }

        response = post(
            url=self.__url + endpoint,
            data=payload
        )
        response.raise_for_status()

        return response.json()

    def __get_statistic_calls_key(
            self,
            params: dict,
            endpoint: str = 'vpbx/stats/calls/request/'
    ) -> Union[str, HTTPError]:
        json_data = {key: value for key, value in params.items() if value is not None}

        try:
            key = self.__send_post_request(json_data=json_data, endpoint=endpoint).get('key', None)
            return key
        except HTTPError:
            raise

    def get_statistic_calls(
            self,
            start_date: str,
            end_date: str,
            user_ids: list[int] = None,
            group_ids: list[int] = None,
            context_type: list[int] = None,
            context_status: int = None,
            recall_status: int = None,
            search_string: str = None,
            ext_params: int = None,
            ext_fields: list[str] = None,
            limit: str = "100",
            offset: str = "0",
            endpoint: str = 'vpbx/stats/calls/result/',
            max_retries: int = 5,
            retry_delay: int = 30
    ):
        if recall_status:
            if not context_type == 1 and not context_status == 0:
                raise CustomMangoException(f'Поля context_type, context_status и recall_status'
                                           f' заполняются по следующему правилу: поле recall_status проставляется'
                                           f' только если context_type = 1 context_status = 0, '
                                           f'в остальном любые комбинации.')
        if int(limit) not in [1, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000]:
            raise CustomMangoException(f"Параметр limit находится не в списке допустимых значений.\nСписок допустимых "
                                       f"значений: 1, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000")

        start_datetime = datetime.strptime(start_date, "%d.%m.%Y %H:%M:%S")
        end_datetime = datetime.strptime(end_date, "%d.%m.%Y %H:%M:%S")
        if (end_datetime - start_datetime) > timedelta(days=30):
            raise CustomMangoException("Разница между датами превышает один месяц")

        all_list_elements = []
        additional_data = []

        while True:
            try:
                params = self.__filter_params(
                    params={
                        'start_date': start_date,
                        'end_date': end_date,
                        'user_ids': user_ids,
                        'group_ids': group_ids,
                        'context_type': context_type,
                        'context_status': context_status,
                        'recall_status': recall_status,
                        'search_string': search_string,
                        'ext_params': ext_params,
                        'ext_fields': ext_fields,
                        'limit': limit,
                        'offset': offset,
                    }
                )
            except InvalidParameterType as e:
                raise InvalidParameterType(e.param_name, e.expected_type)

            try:
                key = self.__get_statistic_calls_key(params)
            except HTTPError as e:
                if len(all_list_elements) > 0:
                    print(f'Возникла ошибка на сервере: {e}\nПовтор итерации..')
                    time.sleep(10)
                    break
                raise HTTPError(e.args)

            json_data = {
                "key": key
            }

            retries = 0
            while retries < max_retries:
                try:
                    res = self.__send_post_request(json_data=json_data, endpoint=endpoint)
                except HTTPError as e:
                    if len(all_list_elements) > 0:
                        print(f'Возникла ошибка на сервере: {e}\nПовтор итерации..')
                        time.sleep(10)
                        break
                    raise HTTPError(e.args)

                status = res.get('status', None)
                result = res.get('result', None)

                if result != 1000:
                    raise CustomMangoException(f"Ошибка выполнения запроса. Код ошибки: {result}")

                if status == 'complete':
                    data_list = res.get('data', [])
                    if data_list and len(data_list) > 0:
                        data = data_list[0]
                        list_elements = data.get('list', [])
                    else:
                        list_elements = []

                    if not list_elements:
                        return (
                            self.__get_df(list_elements=all_list_elements),
                            all_list_elements,
                            self.__get_df(list_elements=additional_data)
                        )
                    elif len(list_elements) < int(limit):
                        all_list_elements.extend(list_elements)
                        period = data.get('period', None)
                        total_talks_duration = data.get('total_talks_duration', 0)
                        total_calls_duration = data.get('total_calls_duration', 0)
                        total_calls_count = data.get('total_calls_count', 0)

                        additional_data.append({
                            "period": period,
                            "total_talks_duration": total_talks_duration,
                            "total_calls_duration": total_calls_duration,
                            "total_calls_count": total_calls_count
                        })
                        offset = str(int(offset) + len(list_elements))
                        print(f'Элементов получено: {len(list_elements)}. Всего элементов: {len(all_list_elements)}\n'
                              f'Переход к следующей итерации со сдвигом {offset}')
                        break
                    else:
                        all_list_elements.extend(list_elements)
                        offset = str(int(offset) + int(limit))
                        print(f'Элементов получено: {len(list_elements)}. Всего элементов: {len(all_list_elements)}\n'
                              f'Переход к следующей итерации со сдвигом {offset}')
                        break

                elif status == 'request' or status == 'work':
                    print(f'API Mango Office выполняет обработку данных..\n'
                          f'Ожидание {retry_delay} сек. до следующей попытки получения данных. '
                          f'Осталось попыток {max_retries - retries}')
                    time.sleep(retry_delay)
                    retries += 1
                    continue

                elif status == 'error' or status == 'not-found':
                    raise CustomMangoException(f"Ошибка выполнения запроса: {status}")

                else:
                    break
