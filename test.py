import requests

response = requests.get('https://www.capterra.com/360-degree-feedback-software/' , headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36"
        })

print(response.content.decode())