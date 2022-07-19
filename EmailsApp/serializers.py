from django_elasticsearch_dsl_drf.serializers import DocumentSerializer

from .documents import *
from .models import *
 


class DataDocumentSerializer(DocumentSerializer):
    class Meta:
        model=Data
        document = DataDocument
        fields = (
            'full_name',
            'gender',
            'job_title',
            'job_title_role',
            'job_title_sub_role',
            'location_country',
            'location_continent',
            'emails',
            'languages'
        )

        def get_location(self,obj):
            try:
                return obj.location.to_dict()
            except:
                return{}

