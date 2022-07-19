from django.urls import re_path as url
from . import views 

urlpatterns = [ 
    url('search_emails/',views.DataViewSet.as_view({'get':'list'})),
    url('getJobSubRole/', views.getJobSubRole , name='getJobSubRole'),
    url('getCountry/', views.getCountry , name='getCountry'),
    url('', views.index , name='index'),
    
] 