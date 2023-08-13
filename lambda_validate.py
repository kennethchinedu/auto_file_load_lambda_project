#validating if lamda function is working locally
from lambda_function import lambda_handler

response = lambda_handler(None, None)

print(response)