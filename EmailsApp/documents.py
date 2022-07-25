from django_elasticsearch_dsl import Document,Index,fields

from .models import *


PUBLISHER_INDEX_data=Index('data')

PUBLISHER_INDEX_data.settings(
    number_of_shards=1,
    number_of_replicas=1,
    max_result_window=500000000
)


@PUBLISHER_INDEX_data.doc_type
class DataDocument(Document):
    emails = fields.ObjectField()

    def prepare_emails(self, instance):
        return instance.emails

    job_title_sub_role = fields.TextField()

    def prepare_job_title_sub_role(self, instance):
        return str(instance.job_title_sub_role).replace(' ','_')


    job_title_role = fields.TextField()

    def prepare_job_title_role(self, instance):
        return str(instance.job_title_role).replace(' ','_')
        
        
    location_country = fields.TextField()

    def prepare_location_country(self, instance):
        return str(instance.location_country).replace(' ','_')


    class Django :
        model= Data

        fields = [
            'full_name', 
            'gender',
            'age',
            'job_title',
            # 'job_title_role',
            # 'job_title_sub_role',
            # 'location_country',
            'location_continent',
            'languages'
        ]

