import json
import subprocess
from datetime import date
from django.urls import reverse
from django.db.models import Q

import pandas as pd
import requests
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from django.http import HttpResponseRedirect, JsonResponse
from django.http.response import JsonResponse
from django.shortcuts import redirect, render
from django_elasticsearch_dsl_drf.filter_backends import (
    CompoundSearchFilterBackend, FilteringFilterBackend)
from django_elasticsearch_dsl_drf.viewsets import DocumentViewSet
from pymongo import MongoClient
from pymongo.operations import InsertOne
from zipfile import *
import gzip

from .documents import *
from .models import *
from .serializers import *

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
def preparingData(uploaded_file_url):
    print('start preparing data')
    ds=pd.read_json(uploaded_file_url)
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
    
    ages=[]
    year=date.today().year
    for i in range(0,len(ds)):
        birth_year=ds['birth_year'][i]
        if not birth_year:
            ages.append(None)
        else:
            ages.append(year-birth_year)
    
    ds['age']=ages        
        
    df=ds[['full_name','gender','age','job_title','job_title_role','job_title_sub_role','location_country','location_continent','emails','languages']]
    print('finish preparing data | start inserting data')

    # myclient = MongoClient("mongodb+srv://soumaya:soumaya1Atlas@cluster0.y9xab.mongodb.net/test?retryWrites=true&w=majority")
    myclient = MongoClient("mongodb://localhost:27017")
    db = myclient["LinkedinDB"]
    col = db["LinkedinApp_data"]
    col.drop_indexes()
    data = json.loads(df.to_json(orient='records'))
    col.bulk_write([ InsertOne(item) for item in data])
    print('finish inserting data')




def prepareFile(uploaded_file_url):
    print('start preparing file')
    # with open("../LinkedinDjango"+uploaded_file_url,'r', encoding="utf8") as contents:
    #     save = contents.read()

    uploaded_file_url="["+",\n".join(uploaded_file_url.splitlines())+"]"
    # with open("../LinkedinDjango"+uploaded_file_url,'w', encoding="utf8") as contents:
    #     contents.write("[")
    #     contents.write(save)
    # with open("../LinkedinDjango"+uploaded_file_url,'a', encoding="utf8") as contents:
    #     contents.write("]")
    print('finish preparing file')
    return uploaded_file_url


def getJobSubRole(request):
    job_role=request.GET.get('job_role')
    job_title_sub_role = Data.objects.filter(job_title_role__iexact=job_role).values_list('job_title_sub_role', flat=True).distinct()

    # list_job_title_sub_role=set()
    # r =requests.get('http://127.0.0.1:8000/search_emails/?job_title_role='+str(job_role).lower())
    # r_dictionary= r.json()
    # for item in r_dictionary['results']:
    #     list_job_title_sub_role.add(item['job_title_sub_role'])
        

    response_data={
        'list_job_title_sub_role':sorted(list(job_title_sub_role)),
        # 'data':r_dictionary['results']
    } 
    return JsonResponse(response_data)

def getCountry(request):
    continent=request.GET.get('continent')
    countries = Data.objects.filter(location_continent__iexact=continent).values_list('location_country', flat=True).distinct()

    # list_countries=set()
    # r =requests.get('http://127.0.0.1:8000/search_emails/?location_continent='+str(continent))
    # r_dictionary= r.json()
    # for item in r_dictionary['results']:
    #     list_countries.add(item['location_country'])
    response_data={
        'list_countries':sorted(list(countries), key=lambda x: (x is None, x))
    } 
    return JsonResponse(response_data)

def getResults(request):
    gender=request.GET.get('gender') 
    age=int(request.GET.get('age')) if request.GET.get('age') !='' else request.GET.get('age') 
    job_role=request.GET.get('job_role') 
    job_sub_role=request.GET.get('job_sub_role')
    continent=request.GET.get('continent') 
    country=request.GET.get('country') 
    language=request.GET.get('language') 
    all_emails=list()

    genderquery = Q(gender=gender) if gender != '' else Q()
    
    if age == 10 :
        agequery = Q(age__lt=20) 
    elif age == 20 :
        agequery = Q(age__range=(21,30)) 
    elif age == 30 :
        agequery = Q(age__range=(31,40)) 
    elif age == 40 :
        agequery = Q(age__range=(41,50)) 
    elif age == 50 :
        agequery = Q(age__range=(51,60)) 
    elif age == 60 :
        agequery = Q(age__gt=60) 
    else:
        agequery=Q()
    job_rolequery = Q(job_title_role__iexact=job_role) if job_role != '' else Q()
    job_sub_rolequery = Q(job_title_sub_role__iexact=job_sub_role) if job_sub_role != '' else Q()
    continentquery = Q(location_continent__iexact=continent) if continent != '' else Q()
    countryquery = Q(location_country=country) if country != '' else Q()
    languagequery = Q(languages=language) if language != '' else Q()
    results = Data.objects.filter(genderquery&job_rolequery&job_sub_rolequery&continentquery&countryquery&languagequery&agequery).values()
    # r =requests.get('http://127.0.0.1:8000/search_emails/?format=json&gender='+str(gender)+'&job_title_role='+str(job_role).lower()+'&job_title_sub_role='+str(job_sub_role).lower()+'&location_continent='+str(continent)+'&location_country='+str(country)+'&languages='+str(language)+'&age__'+age)
    # r_dictionary= r.json()
    for item in results:
        list_emails=list()
        for email in item['emails']:
            list_emails.append(email['address'])
        all_emails.append(list_emails)
    response_data={
        'all_emails':all_emails,
        'data':list(results),
        'count':results.count()
            } 
    return JsonResponse(response_data)


def index(request):
    if request.method == 'POST' and request.FILES['datafile']:
        print('start uploading file')
        myfiles = request.FILES.getlist('datafile')
        for myfile in myfiles:
            start_lines=0
            end_lines=10000
            with gzip.open(myfile, 'rb') as f:
                readlines=f.readlines()
                for i, l in enumerate(readlines):
                    pass
                nbr_lines=i+1
                print(nbr_lines)
                # myfile=f.read().decode("utf-8")
                # json_string=prepareFile(myfile) 
                # preparingData(json_string)
                # Work with 50k lines per time
                while (True):
                    if end_lines>nbr_lines:
                        part = readlines[end_lines-10000:nbr_lines]
                        part = b''.join(part)
                        myfile = part.decode("utf-8")
                        json_string=prepareFile(myfile) 
                        preparingData(json_string)
                        break
                    else:
                        # part = [next(f) for x in range(lines)] 
                        part = readlines[start_lines:end_lines]
                        part = b''.join(part)
                        myfile = part.decode("utf-8")
                        json_string=prepareFile(myfile) 
                        preparingData(json_string)
                        start_lines=end_lines
                        end_lines+=10000
                    
            # with ZipFile(myfile) as myzip:
            #     names = myzip.namelist()
            #     for name in names :
            #         myfile=myzip.read(name).decode("utf-8")
            # print('myfileee', myfile )
        #     fs = FileSystemStorage()
        #     filename = fs.save(myfile.name, myfile)
        #     print('finish uploading file')
        #     uploaded_file_url = fs.url(filename)
        # subprocess.run("python manage.py search_index --rebuild",shell=True, input='y'.encode())
        return HttpResponseRedirect(reverse("index"))

    
    data = Data.objects.all()
    job_title_role = Data.objects.all().values_list('job_title_role', flat=True).distinct()
    language = Data.objects.all().values_list('languages', flat=True).distinct()
    # print('MGDB',[str(i).replace(' ','_') for i in list(job_title_role)])
    # list_job_title_role=set()
    # list_language=set()
    # job_title_roles=DataDocument.search().source('job_title_role')

    # for hit in job_title_roles.scan(): 
    #     list_job_title_role.add(hit.job_title_role)
    # r =requests.get('http://127.0.0.1:8000/search_emails/?format=json')
    # r_dictionary= r.json()
    # for item in r_dictionary['results']:
        # list_job_title_role.add(item['job_title_role']) 
        # list_language.add(item['languages']) 
        
    # print(list_job_title_role)
    context = {
        'data': data,
        'list_job_title_role': sorted(list(job_title_role), key=lambda x: (x is None, x)), 
        'list_language': sorted(list(language), key=lambda x: (x is None, x)),
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
            'age',
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
        'age': 'age',
        'job_title': 'job_title',
        'job_title_role': 'job_title_role',
        'job_title_sub_role': 'job_title_sub_role',
        'location_country': 'location_country',
        'location_continent': 'location_continent',
        'languages':'languages'         
    }
