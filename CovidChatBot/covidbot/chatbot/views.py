from django.shortcuts import render
from bs4 import BeautifulSoup
import requests

# Create your views here.
def home(request):
    return render(request,'chatbot/index.html')

def symptoms(request):
    return render(request,'chatbot/symptoms.html')

def prevention(request):
    return render(request,'chatbot/prevention.html')

def faq(request):
    return render(request,'chatbot/FAQ.html')

def footer(request):
    return render(request,'chatbot/footer.html')

def indexie(request):
    url = 'https://www.irishtimes.com'
    soup = requests.get(url + '/news/health/coronavirus')
    soup = BeautifulSoup(soup.text)
    headlines = {hl.find('span', class_='h2').text:url + hl['href'] for hl in soup.find_all('a', class_='gtm-event')[4:5]}
    return render(request,'chatbot/indexie.html', {'headlines': headlines})