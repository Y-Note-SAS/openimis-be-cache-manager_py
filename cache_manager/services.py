# services.py
from django.db import models
from django.core.exceptions import ValidationError
from django.utils.translation import gettext as _
from django.core.cache import caches
from django.conf import settings
from location.models import Location, HealthFacility, UserDistrict, OfficerVillage, LocationManager
from insuree.models import InsureePolicy
from claim.models import (ClaimDedRem, Claim, ClaimAdmin, Feedback, ClaimItem, ClaimService, ClaimAttachment,
                          ClaimAttachmentType, FeedbackPrompt)
from insuree.models import Insuree, InsureePhoto, Family, InsureePolicy, InsureeStatusReason, PolicyRenewalDetail
from individual.models import Individual, IndividualDataSource, Group, GroupIndividual, IndividualDataSourceUpload
from medical.models import Diagnosis, Item, Service
from policy.models import Policy, PolicyRenewal
from product.models import Product, ProductItem, ProductService
from tools.models import Extract
from contribution.models import Premium
from cs.models import ChequeImport, ChequeImportLine, ChequeUpdatedHistory
from core.models import Role, User, RoleRight, InteractiveUser, UserRole, Officer
from django.db.models import QuerySet
from itertools import islice

class CacheService:
    
    def __init__(self, user):
        self.user = user


    @staticmethod
    def clear_all_model_cache(model):
        cache = caches['default']
        redis_client = cache.client.get_client()
        redis_client.select(0)
        if model and model in CacheService.openimis_models:
            prefix = CacheService.get_prefixed_model(model)
            keys = redis_client.scan_iter(match=f'{prefix}*', count=10000)
            
            for key in keys:
                key_str = key.decode('utf-8')
                redis_client.delete(key_str)
            return None
        
    @staticmethod
    def clear_module_cache(model):
        cache = caches[model]
        redis_client = cache.client.get_client()
        redis_client.select(0)
        if model and model in CacheService.cache_modules:
            cache_config = settings.CACHES[model]
            prefix = cache_config.get('KEY_PREFIX', '')
            keys = redis_client.scan_iter(match=f'{prefix}*', count=10000)
            
            for key in keys:
                key_str = key.decode('utf-8')
                redis_client.delete(key_str)
            return None

    cache_modules = {'location', 'coverage'}
    
    openimis_models = {'location_user', 'coverage', 'claim_admin', 'claim', 'claim_item', 'claim_attachment_type', 'claim_attachment',
                   'claim_service','claim_ded_rem', 'premium', 'role', 'role_right', 'interactive_user', 'user_role', 'user', 'officer', 
                   'insuree_photo', 'family', 'insuree', 'insuree_policy', 'health_facility', 'user_district', 'location', 'officer_village', 
                   'feedback_prompt', 'diagnosis', 'item', 'service', 'policy', 'policy_renewal', 'product', 'product_service', 'extract'}
    #'individual', 'individual_data_source_upload', 'individual_data_source', 'group', 'group_individual', 'history_business_model', 
    # 'cheque_import', 'cheque_import_line', 'cheque_updated_history', 'product_item','feedback', 'policy_renewal_detail', 'insuree_status_reason',  

    @staticmethod
    def get_prefixed_model(model):
        """
        Retourne le modèle avec le préfixe 'oi:1:' basé sur le modèle donné.
        """
        
        model_class, _ = CacheService.get_model_class(model)
        prefix = f"oi:1:{model_class.__name__}:"
        
        return prefix

    def items_count(model):
            match model:              
                    case "cheque_import":
                        valid_items_count = ChequeImport.objects.filter().count()
                    case "cheque_import_line":
                        valid_items_count = ChequeImportLine.objects.filter().count()
                    case "cheque_updated_history":
                        valid_items_count = ChequeUpdatedHistory.objects.filter().count()
                    case "individual":
                        valid_items_count = Individual.objects.filter().count()
                    case "individual_data_source_upload":
                        valid_items_count = IndividualDataSourceUpload.objects.filter().count()
                    case "individual_data_source":
                        valid_items_count = IndividualDataSource.objects.filter().count()
                    case "group":
                        valid_items_count = Group.objects.filter().count()
                    case "group_individual":
                        valid_items_count = GroupIndividual.objects.filter().count()  
                    case "location_user":
                        valid_items_count = Location.objects.filter(validity_to__isnull=True).count() + 3      
                    case _:
                        model_class, _ = CacheService.get_model_class(model)
                        valid_items_count = model_class.objects.filter(validity_to__isnull=True).count()
            return valid_items_count


    @staticmethod
    def get_model_class(model):
        """
        Returns the corresponding Django model class based on the model name.
        """
        model_classes = {
            'location': Location,
            'claim_admin': ClaimAdmin,
            'feedback': Feedback,
            'claim': Claim,
            'feedback_prompt': FeedbackPrompt,
            'claim_item': ClaimItem,
            'claim_attachment_type': ClaimAttachmentType,
            'claim_attachment': ClaimAttachment,
            'claim_service': ClaimService,
            'claim_ded_rem': ClaimDedRem,
            'premium': Premium,
            'role': Role,
            'role_right': RoleRight,
            'interactive_user': InteractiveUser,
            'user_role': UserRole,
            'user': User,
            'officer': Officer,
            'insuree_photo': InsureePhoto,
            'family': Family,
            'insuree_status_reason': InsureeStatusReason,
            'insuree': Insuree,
            'insuree_policy': InsureePolicy,
            'health_facility': HealthFacility,
            'user_district': UserDistrict,
            'officer_village': OfficerVillage,
            'diagnosis': Diagnosis,
            'item': Item,
            'service': Service,
            'policy': Policy,
            'policy_renewal': PolicyRenewal,
            'product': Product,
            'product_service': ProductService,
            'extract': Extract
        }
        
        cache_modules = {
            'location_user': Location,
            'coverage': InsureePolicy,
        }

        if model in model_classes:
            return model_classes[model], True
        elif model in cache_modules:
            return cache_modules[model], False
        else:
            raise ValidationError(_("Model_not_found_for_preloading"))

    @staticmethod
    def preload_model_cache(model, user):
        """
        Preheats the cache by loading all the data of the specified model.
        """
        CACHE_TIMEOUT = None
        BATCH_SIZE = 10000
        try:
            
            if model not in CacheService.openimis_models:
                raise ValidationError(_("Unsupported_model_for_cache_preheating"))
            
            model_class, is_model = CacheService.get_model_class(model)
            all_objects = model_class.objects.filter(validity_to__isnull=True).only("id")
            cache_data = {}
            
            if is_model:
                cache = caches['default']
                # for obj in all_objects:
                #     cache_data[get_cache_key(model_class, obj.id)] = obj
                    
                # cache.set_many(cache_data, timeout=CACHE_TIMEOUT)
                for chunk in chunked_queryset(all_objects, BATCH_SIZE):
                    cache_data = {
                        get_cache_key(model_class, obj.id): {"id": obj.id}
                        for obj in chunk
                    }
                    cache.set_many(cache_data, timeout=CACHE_TIMEOUT)
            else:
                if model == 'location_user':
                    # cache = caches['location']
                    all_objects = UserDistrict.get_user_districts(user)
                else:
                    cache = caches[model]
                    # for obj in all_objects:
                    #     cache_data[get_cache_key_base(model, obj.id)] = obj
                        
                    # cache.set_many(cache_data, timeout=CACHE_TIMEOUT)
                    for chunk in chunked_queryset(all_objects, BATCH_SIZE):
                        cache_data = {
                            get_cache_key_base(model, obj.id): obj
                            for obj in chunk
                        }
                        cache.set_many(cache_data, timeout=CACHE_TIMEOUT)
            
            return True
        except Exception as exc:
            raise ValidationError(_("Error_during_cache_preheating:") + str(exc))

# Generation of the object key
def get_cache_key(model, id):
    return f"{model.__name__}:{id}"

def get_cache_key_base(model, id):
    return f"{model}_{id}"

BATCH_SIZE = 10000

def chunked_queryset(qs: QuerySet, batch_size: int):
    """
    Generator to yield queryset in chunks to avoid memory overload.
    """
    start = 0
    while True:
        chunk = list(qs[start:start+batch_size])
        if not chunk:
            break
        yield chunk
        start += batch_size