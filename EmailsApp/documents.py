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
    
    # _id = fields.ObjectField()

    # def prepare__id(self, instance):
    #     return instance._id

    class Django :
        model= Data

        fields = [
            'full_name',
            'gender',
            'job_title',
            'job_title_role',
            'job_title_sub_role',
            'location_country',
            'location_continent',
            'languages'
        ]

