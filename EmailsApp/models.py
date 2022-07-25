from djongo import models

class Email(models.Model):
    id=models.ObjectIdField(primary_key=True,unique=False)
    address=models.EmailField(max_length = 255)
    type=models.CharField(max_length=255)


class Data(models.Model):
    id=models.ObjectIdField(primary_key=True)
    full_name = models.CharField(max_length=255)
    gender = models.CharField(max_length=255)
    age = models.IntegerField()
    job_title = models.CharField(max_length=255)
    job_title_role = models.CharField(max_length=255)
    job_title_sub_role = models.CharField(max_length=255)
    location_country = models.CharField(max_length=255)
    location_continent = models.CharField(max_length=255)
    emails = models.ArrayField(model_container=Email)
    languages =models.CharField(max_length=255)
    