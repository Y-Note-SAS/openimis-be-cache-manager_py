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

class CacheService:
    
    def __init__(self, user):
        self.user = user


    @staticmethod
    def clear_all_model_cache(model):
        cache = caches['default']
        redis_client = cache.client.get_client()
        redis_client.select(0)
        if model and model in CacheService.openimis_models:
            prefix = CacheService.MODEL_PREFIXES[model] 
            keys = redis_client.scan_iter(match=f'{prefix}*')
            
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
            keys = redis_client.scan_iter(match=f'{prefix}*')
            
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

    MODEL_PREFIXES = {
        'services_pricelist': 'oi:1:ServicesPricelist:',
        'location': 'oi:1:Location:',
        'claim_admin': 'oi:1:ClaimAdmin:',
        'feedback': 'oi:1:Feedback:',
        'claim': 'oi:1:Claim:',
        'feedback_prompt': 'oi:1:FeedbackPrompt:',
        'claim_item': 'oi:1:ClaimItem:',
        'claim_attachment_type': 'oi:1:ClaimAttachmentType:',
        'claim_attachment': 'oi:1:ClaimAttachment:',
        'claim_service': 'oi:1:ClaimService:',
        'claim_ded_rem': 'oi:1:ClaimDedRem:',
        'premium': 'oi:1:Premium:',
        'history_business_model': 'oi:1:HistoryBusinessModel:',
        'role': 'oi:1:Role:',
        'role_right': 'oi:1:RoleRight:',
        'interactive_user': 'oi:1:InteractiveUser:',
        'user_role': 'oi:1:UserRole:',
        'user': 'oi:1:User:',
        'officer': 'oi:1:Officer:',
        'cheque_import': 'oi:1:ChequeImport:',
        'cheque_import_line': 'oi:1:ChequeImportLine:',
        'cheque_updated_history': 'oi:1:ChequeUpdatedHistory:',
        'individual': 'oi:1:Individual:',
        'individual_data_source_upload': 'oi:1:IndividualDataSourceUpload:',
        'individual_data_source': 'oi:1:IndividualDataSource:',
        'group': 'oi:1:Group:',
        'group_individual': 'oi:1:GroupIndividual:',
        'insuree_photo': 'oi:1:InsureePhoto:',
        'family': 'oi:1:Family:',
        'insuree_status_reason': 'oi:1:InsureeStatusReason:',
        'insuree': 'oi:1:Insuree:',
        'insuree_policy': 'oi:1:InsureePolicy:',
        'policy_renewal_detail': 'oi:1:PolicyRenewalDetail:',
        'health_facility': 'oi:1:HealthFacility:',
        'user_district': 'oi:1:UserDistrict:',
        'officer_village': 'oi:1:OfficerVillage:',
        'diagnosis': 'oi:1:Diagnosis:',
        'item': 'oi:1:Item:',
        'service': 'oi:1:Service:',
        'policy': 'oi:1:Policy:',
        'policy_renewal': 'oi:1:PolicyRenewal:',
        'product': 'oi:1:Product:',
        'product_item': 'oi:1:ProductItem:',
        'product_service': 'oi:1:ProductService:',
        'extract': 'oi:1:Extract:',
    }

    def items_count(model):
            match model:
                    case "location":
                        valid_items_count = Location.objects.filter(validity_to__isnull=True).count()
                    case "coverage":
                        valid_items_count = InsureePolicy.objects.filter(validity_to__isnull=True).count()
                    case "claim_admin":
                        valid_items_count = ClaimAdmin.objects.filter(validity_to__isnull=True).count()
                    case "feedback":
                        valid_items_count = Feedback.objects.filter(validity_to__isnull=True).count()
                    case "claim":
                        valid_items_count = Claim.objects.filter(validity_to__isnull=True).count()
                    case "feedback_prompt":
                        valid_items_count = FeedbackPrompt.objects.filter(validity_to__isnull=True).count()
                    case "claim_item":
                        valid_items_count = ClaimItem.objects.filter(validity_to__isnull=True).count()
                    case "claim_attachment_type":
                        valid_items_count = ClaimAttachmentType.objects.filter(validity_to__isnull=True).count()
                    case "claim_attachment":
                        valid_items_count = ClaimAttachment.objects.filter(validity_to__isnull=True).count()
                    case "claim_service":
                        valid_items_count = ClaimService.objects.filter(validity_to__isnull=True).count()
                    case "claim_ded_rem":
                        valid_items_count = ClaimDedRem.objects.filter(validity_to__isnull=True).count()
                    case "premium":
                        valid_items_count = Premium.objects.filter(validity_to__isnull=True).count()
                    case "role":
                        valid_items_count = Role.objects.filter(validity_to__isnull=True).count()
                    case "role_right":
                        valid_items_count = RoleRight.objects.filter(validity_to__isnull=True).count()
                    case "interactive_user":
                        valid_items_count = InteractiveUser.objects.filter(validity_to__isnull=True).count()
                    case "user_role":
                        valid_items_count = UserRole.objects.filter(validity_to__isnull=True).count()
                    case "user":
                        valid_items_count = User.objects.filter(validity_to__isnull=True).count()
                    case "officer":
                        valid_items_count = Officer.objects.filter(validity_to__isnull=True).count()
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
                    case "insuree_photo":
                        valid_items_count = InsureePhoto.objects.filter(validity_to__isnull=True).count()
                    case "family":
                        valid_items_count = Family.objects.filter(validity_to__isnull=True).count()
                    case "insuree_status_reason":
                        valid_items_count = InsureeStatusReason.objects.filter(validity_to__isnull=True).count()
                    case "insuree":
                        valid_items_count = Insuree.objects.filter(validity_to__isnull=True).count()
                    case "insuree_policy":
                        valid_items_count = InsureePolicy.objects.filter(validity_to__isnull=True).count()
                    case "policy_renewal_detail":
                        valid_items_count = PolicyRenewalDetail.objects.filter(validity_to__isnull=True).count()
                    case "policy_renewal":
                        valid_items_count = PolicyRenewal.objects.filter(validity_to__isnull=True).count()
                    case "health_facility":
                        valid_items_count = HealthFacility.objects.filter(validity_to__isnull=True).count()
                    case "user_district":
                        valid_items_count = UserDistrict.objects.filter(validity_to__isnull=True).count()
                    case "officer_village":
                        valid_items_count = OfficerVillage.objects.filter(validity_to__isnull=True).count()
                    case "diagnosis":
                        valid_items_count = Diagnosis.objects.filter(validity_to__isnull=True).count()
                    case "item":
                        valid_items_count = Item.objects.filter(validity_to__isnull=True).count()
                    case "service":
                        valid_items_count = Service.objects.filter(validity_to__isnull=True).count()
                    case "policy":
                        valid_items_count = Policy.objects.filter(validity_to__isnull=True).count()
                    case "PolicyRenewal":
                        valid_items_count = PolicyRenewal.objects.filter(validity_to__isnull=True).count()
                    case "product":
                        valid_items_count = Product.objects.filter(validity_to__isnull=True).count()
                    case "product_item":
                        valid_items_count = ProductItem.objects.filter(validity_to__isnull=True).count()
                    case "product_service":
                        valid_items_count = ProductService.objects.filter(validity_to__isnull=True).count()
                    case "extract":
                        valid_items_count = Extract.objects.filter(validity_to__isnull=True).count()
                    case "location_user":
                        valid_items_count = Location.objects.filter(validity_to__isnull=True).count() + 3
                    case _:
                        valid_items_count = 0
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
        try:
            
            if model not in CacheService.openimis_models:
                raise ValidationError(_("Unsupported_model_for_cache_preheating"))
            
            model_class, is_model = CacheService.get_model_class(model)
            all_objects = model_class.objects.filter(validity_to__isnull=True)
            cache_data = {}
            
            if is_model:
                cache = caches['default']
                for obj in all_objects:
                    cache_data[get_cache_key(model_class, obj.id)] = obj
                    
                cache.set_many(cache_data, timeout=CACHE_TIMEOUT)
            else:
                if model == 'location_user':
                    # cache = caches['location']
                    all_objects = UserDistrict.get_user_districts(user)
                else:
                    cache = caches[model]
                    for obj in all_objects:
                        cache_data[get_cache_key_base(model, obj.id)] = obj
                        
                    cache.set_many(cache_data, timeout=CACHE_TIMEOUT)
            
            return True
        except Exception as exc:
            raise ValidationError(_("Error_during_cache_preheating:") + str(exc))

# Generation of the object key
def get_cache_key(model, id):
    return f"{model.__name__}:{id}"

def get_cache_key_base(model, id):
    return f"{model}_{id}"
