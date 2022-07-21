from django.http import JsonResponse
from django.shortcuts import render,redirect
from django_elasticsearch_dsl_drf.filter_backends import (
    CompoundSearchFilterBackend,FilteringFilterBackend)
from django_elasticsearch_dsl_drf.viewsets import DocumentViewSet
from django.http.response import JsonResponse
from numpy import append
from rest_framework.parsers import JSONParser 
from .models import *
from .documents import *
from .serializers import *

import pandas as pd
from pymongo import MongoClient
from pymongo.operations import InsertOne
import json
from django.conf import settings
from django.core.files.storage import FileSystemStorage
import subprocess
import requests

def preparingData(uploaded_file_url):
    ds=pd.read_json("../LinkedinDjango"+uploaded_file_url)
    subroles={    'art':['fine art','printing','arts and crafts','performing arts'],
    'media':["media production", "motion pictures and film", "telecommunications", "photography", "newspapers", "internet", "entertainment", "writing and editing", "online media", "broadcast media", "animation", "publishing"],
'design':["graphic design", "design"],
'education':["higher education", "research", "education management", "primary/secondary education", "professional training & coaching", "libraries", "e-learning"],
'engineering':["aviation & aerospace", "civil engineering", "machinery", "mechanical or industrial engineering", "electrical/electronic manufacturing", "automotive", "airlines/aviation", "industrial automation", "railroad manufacture", "nanotechnology"],
'entertainment':["apparel & fashion", "cosmetics", "music", "wine and spirits", "gambling & casinos", "fishery", "restaurants", "food production", "leisure, travel, & tourism"],
'environment':["oil & energy", "environmental services", "maritime", "renewables & environment"],
'finance':["financial services", "banking", "investment banking"],
'health':["medical practice", "hospitality", "hospital & health care", "biotechnology", "pharmaceuticals", "medical devices", "health, wellness and fitness", "chemicals", "alternative medicine", "mental health care", "veterinary", "sports", "philanthropy", "tobacco"],
'human_resources':["staffing and recruiting", "human resources"],
'IT':["information technology and services", "computer software", "program development", "warehousing", "information services", "computer games", "computer hardware", "computer & network security", "computer networking", "wireless"],
'legal':["law practice", "legal services", "law enforcement", "judiciary", "executive office", "alternative dispute resolution", "think tanks", "political organization"],
'management':["non-profit organization management", "government administration", "investment management", "management consulting"],
'marketing':["marketing and advertising", "market research"],
'operations':["outsourcing/offshoring", "logistics and supply chain", "building materials", "defense & space", "import and export", "mining & metals"],
'products':["consumer goods", "glass, ceramics, & concrete", "plastics", "paper & forest products", "textiles", "packaging and containers", "furniture", "sporting goods", "dairy", "luxury goods & jewelry"],
'public_relations':["government relations", "public relations and communications", "international affairs", "public safety", "public policy"],
'real_estate':["construction", "real estate", "museums and institutions", "architecture & planning", "commercial real estate"],
'sales':["business supplies and equipment", "wholesale", "accounting", "retail", "consumer electronics", "venture capital & private equity", "capital markets"],
'services':["events services", "transportation/trucking/railroad", "insurance", "consumer services", "facilities services", "farming", "recreational facilities and services", "military", "package/freight delivery", "civic & social organization", "security and investigations", "religious institutions", "utilities", "translation and localization", "individual & family services", "semiconductors", "supermarkets", "fund-raising", "ranching", "shipbuilding"],
'trades':["international trade and development"]
         }
    selectedLanguage=[]
    for i in range(0,len(ds)):
        languages = ds['languages'][i]
        proficiencies=[lg['proficiency'] for lg in languages]

        if not languages:
            selectedLanguage.append(None)
        else:
            for lg in languages:
                try:
                    if lg['proficiency']==max(proficiencies):
                        maxlan=lg['name']
                        selectedLanguage.append(maxlan)
                        break

                except TypeError:
                    maxlan=languages[0]['name']
                    selectedLanguage.append(maxlan)
                    break
    ds['languages']=selectedLanguage

    new_job_title_sub_role=[]
    for i in range(0,len(ds)):
        sr=ds['job_title_sub_role'][i]
        r=ds['job_title_role'][i]
        ind=ds['industry'][i]
        if sr :
            new_job_title_sub_role.append(sr)
        else:
            if r :
                new_job_title_sub_role.append(r)
            else:
                if not ind :
                    new_job_title_sub_role.append(None)
                else:
                    new_job_title_sub_role.append(ind)

    ds['job_title_sub_role']=new_job_title_sub_role

    #estimate the job_role based on keywords of sub_role
    new_job_title_role=[]
    listvalues=[]
    for v in subroles.values():
            listvalues+=v
    for i in range(0,len(ds)):
        sr=ds['job_title_sub_role'][i]
        r=ds['job_title_role'][i]
        if r :
            new_job_title_role.append(r)
        else:
            if not sr :
                new_job_title_role.append(None)
            else:
                if sr not in listvalues:
                    new_job_title_role.append(sr)
                else:
                    for key, value in subroles.items():
                        if sr in value :
                            new_job_title_role.append(key)
                    
                        
    ds['job_title_role']=new_job_title_role
    df=ds[['full_name','gender','job_title','job_title_role','job_title_sub_role','location_country','location_continent','emails','languages']]
    df['job_title_role']=df['job_title_role'].str.replace(' ', '_')
    myclient = MongoClient("mongodb+srv://soumaya:soumaya1Atlas@cluster0.y9xab.mongodb.net/test?retryWrites=true&w=majority")
    db = myclient["LinkedinDB"]
    col = db["LinkedinApp_data"]
    col.drop_indexes()
    data = json.loads(df.to_json(orient='records'))
    col.bulk_write([ InsertOne(item) for item in data])


def prepareFile(uploaded_file_url):
    with open("../LinkedinDjango"+uploaded_file_url,'r', encoding="utf8") as contents:
        save = contents.read()
        save=",\n".join(save.splitlines())
    with open("../LinkedinDjango"+uploaded_file_url,'w', encoding="utf8") as contents:
        contents.write("[")
        contents.write(save)
    with open("../LinkedinDjango"+uploaded_file_url,'a', encoding="utf8") as contents:
        contents.write("]")

def getJobSubRole(request):
    job_role=request.GET.get('job_role')
    list_job_title_sub_role=set()
    all_emails=list()
    r =requests.get('http://127.0.0.1:8000/search_emails/?job_title_role='+str(job_role).lower())
    r_dictionary= r.json()
    for item in r_dictionary['results']:
        list_job_title_sub_role.add(item['job_title_sub_role'])
        list_emails=list()
        for email in item['emails']:
            list_emails.append(email['address'])
        all_emails.append(list_emails)

    response_data={
        'list_job_title_sub_role':sorted(list(list_job_title_sub_role)),
        'all_emails':all_emails,
        'data':r_dictionary['results']
    } 
    return JsonResponse(response_data)

def getCountry(request):
    continent=request.GET.get('continent')
    list_countries=set()
    r =requests.get('http://127.0.0.1:8000/search_emails/?location_continent='+str(continent))
    r_dictionary= r.json()
    for item in r_dictionary['results']:
        list_countries.add(item['location_country'])
    response_data={
        'list_countries':sorted(list(list_countries), key=lambda x: (x is None, x))
    } 
    return JsonResponse(response_data)

def getResults(request):
    gender=request.GET.get('gender') if request.GET.get('gender') else ' '
    job_role=request.GET.get('job_role') if request.GET.get('job_role') else ' '
    job_sub_role=request.GET.get('job_sub_role') if request.GET.get('job_sub_role') else ' '
    continent=request.GET.get('continent') if request.GET.get('continent') else ' '
    country=request.GET.get('country') if request.GET.get('country') else ' '
    language=request.GET.get('language') if request.GET.get('language') else ' '
    all_emails=list()
    r =requests.get('http://127.0.0.1:8000/search_emails/?gender='+str(gender)+'&job_title_role='+str(job_role).lower()+'&job_title_sub_role='+str(job_sub_role).lower()+'&location_continent='+str(continent)+'&location_country='+str(country)+'&languages='+str(language))
    r_dictionary= r.json()
    print(r_dictionary)
    # for item in r_dictionary['results']:
    #     list_countries.add(item['location_country'])
    #     list_emails=list()
    #     for email in item['emails']:
    #         list_emails.append(email['address'])
    #     all_emails.append(list_emails)
    response_data={
        # 'list_countries':sorted(list(list_countries), key=lambda x: (x is None, x))
    } 
    return JsonResponse(response_data)


def index(request):
    if request.method == 'POST' and request.FILES['datafile']:
        myfile = request.FILES['datafile']
        fs = FileSystemStorage()
        filename = fs.save(myfile.name, myfile)
        uploaded_file_url = fs.url(filename)
        prepareFile(uploaded_file_url)
        preparingData(uploaded_file_url)
        subprocess.run("python manage.py search_index --rebuild",shell=True, input='y'.encode())
    
    data = Data.objects.all()
    job_title_role = Data.objects.all().values_list('job_title_role', flat=True).distinct()
    list_job_title_role=set()
    list_language=set()
    # job_title_roles=DataDocument.search().source('job_title_role')

    # for hit in job_title_roles.scan(): 
    #     list_job_title_role.add(hit.job_title_role)
    r =requests.get('http://127.0.0.1:8000/search_emails/')
    r_dictionary= r.json()
    for item in r_dictionary['results']:
        list_job_title_role.add(item['job_title_role']) 
        list_language.add(item['languages']) 
    context = {
        'data': data,
        'list_job_title_role': sorted(list_job_title_role, key=lambda x: (x is None, x)), 
        'list_language': sorted(list_language, key=lambda x: (x is None, x)),
    }
 
    return render(request, 'index.html', context=context)

    # # Generate counts of some of the main objects
    # num_instances = BookInstance.objects.all().count()

    # # Available books (status = 'a')
    # num_instances_available = BookInstance.objects.filter(status__exact='a').count()

    # # The 'all()' is implied by default.
    # num_authors = Author.objects.count()


# def simple_upload(request):
#     if request.method == 'POST' and request.FILES['datafile']:
#         myfile = request.FILES['datafile']
#         print('file',myfile)
#         fs = FileSystemStorage()
#         filename = fs.save(myfile.name, myfile)
#         uploaded_file_url = fs.url(filename)
#         return render(request, 'index.html', {
#         'uploaded_file_url': uploaded_file_url
#         })
#     return render(request, 'index.html')

# def insertToDB(request, file):
#     #read file from input html
#     ds=pd.read_json(file)
#     df=ds[['full_name','gender','industry','job_title','location_country','location_continent','emails','languages']]
#     myclient = MongoClient("mongodb+srv://soumaya:soumaya1Atlas@cluster0.y9xab.mongodb.net/test?retryWrites=true&w=majority")
#     db = myclient["LinkedinDB"]
#     col = db["LinkedinData"]
#     data = json.loads(df.to_json(orient='records'))
#     col.insert_many(data)
#     return redirect('')

class DataViewSet(DocumentViewSet):
    document = DataDocument
    serializer_class = DataDocumentSerializer
 
    filter_backends = [
        FilteringFilterBackend,
        CompoundSearchFilterBackend
    ]
 
    # Define search fields
    search_fields = (
            'full_name',
            'gender',
            'job_title',
            'job_title_role',
            'job_title_sub_role',
            'location_country',
            'location_continent',
            'languages'
    )

    # Filter fields
    filter_fields = {
        'full_name':'full_name',
        'gender': 'gender',
        'job_title': 'job_title',
        'job_title_role': 'job_title_role',
        'job_title_sub_role': 'job_title_sub_role',
        'location_country': 'location_country',
        'location_continent': 'location_continent',
        'languages':'languages'         
    }