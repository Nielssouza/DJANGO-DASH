from django.shortcuts import render

def index(request):
    return render(request, 'dashboard/index.html')

def pib_mundial(request):
    return render(request, 'dashboard/pib_mundial.html')
