from django.shortcuts import render

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
    return render(request,'chatbot/indexie.html')
