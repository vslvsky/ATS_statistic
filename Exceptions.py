class InvalidParameterType(Exception):
    def __init__(self, param_name, expected_type):
        self.param_name = param_name
        self.expected_type = expected_type
        super().__init__(f"Неправильный тип у параметра: '{param_name}'. Ожидаемый тип: {expected_type}")


class CustomMangoException(Exception):
    def __init__(self, message="Ошибка выполнения запроса к Mango Office API"):
        self.message = message
        super().__init__(self.message)

