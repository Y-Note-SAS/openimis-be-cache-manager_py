import logging
from core import models as core_models
from django_redis.cache import RedisCache
import graphene
from django.core.cache import caches
from django.conf import settings
from django.contrib.auth.models import AnonymousUser
from location.models import free_cache_for_user, Location, HealthFacility, UserDistrict, OfficerVillage
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
logger = logging.getLogger(__name__)
from django.core.exceptions import ValidationError, PermissionDenied
from django.utils.translation import gettext as _
from core.schema import OpenIMISMutation
from policy.models import clean_all_enquire_cache_product
from django.db.models import Q
from core import filter_validity
from cache_manager.services import CacheService

class CacheInfoType(graphene.ObjectType):
    cache_name = graphene.String()
    model = graphene.String(required=False)

    class Meta:
        fields = ("cache_name", "model")

class PageInfoType(graphene.ObjectType):
    has_next_page = graphene.Boolean()
    has_previous_page = graphene.Boolean()
    start_cursor = graphene.String()
    end_cursor = graphene.String()

class CacheInfoEdge(graphene.ObjectType):
    node = graphene.Field(CacheInfoType)


class CacheInfoConnection(graphene.ObjectType):
    total_count = graphene.Int()
    max_item_count = graphene.Int()

    page_info = graphene.Field(PageInfoType)
    edges = graphene.List(CacheInfoEdge)

def clear_cache_graph(cache, model=None, user=None):
    match model:
        case "location_user":
            free_cache_for_user(user.id)
        case "coverage":
            clean_all_enquire_cache_product()
        case _:
            cache.clear()

class Query(graphene.ObjectType):
    cache_info = graphene.Field(
        CacheInfoConnection,
        model=graphene.String(required=False),
        # first=graphene.Int(),
        order_by=graphene.List(graphene.String)
    )

    def resolve_cache_info(self, info, model):
        openimis_models = CacheService.openimis_models
        MODEL_PREFIXES = CacheService.MODEL_PREFIXES

        edges = [CacheInfoEdge(node=cache_info) for cache_info in []]

        if model:
            model = model.lower()
            cache = caches['default']
            
            if (model in settings.CACHES and model != 'location') or model == "location_user":
                cache = caches['location'] if model == "location_user" else caches[model]
            elif model not in openimis_models:
                logger.debug(f"Cache model '{model}' not found.")
                raise ValidationError(_("query.cache_manager.model_does_not_exist"))
        else:
            raise ValidationError(_("query.cache_manager.model_required"))
        
        redis_client = cache.client.get_client()
        redis_client.select(0)

        cache_config = settings.CACHES.get(model, settings.CACHES['default'])
        if model == 'location':
            cache_config = settings.CACHES['default']
        if model == "location_user":
            cache_config = settings.CACHES.get('location')
        prefix = cache_config.get('KEY_PREFIX', '') 
        if cache_config == settings.CACHES['default']:
            prefix = MODEL_PREFIXES[model]        
        keys = redis_client.scan_iter(match=f'{prefix}*')
        cache_info_list = []

        for key in keys:
            key_str = key.decode('utf-8')
            parts = key_str.split(':')
            actual_key = parts[-1]
            if model not in settings.CACHES and model != "location_user":
                actual_key = f"{parts[-2]}:{parts[-1]}"
            data = cache.get(actual_key)
            
            cache_info_list.append(CacheInfoType(
                cache_name=data,
                model=model
            ))
            # if len(cache_info_list) >= first:
            #     break

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
                valid_items_count = len(cache_info_list)
            case _:
                valid_items_count = 0  


        total_count = len(cache_info_list)
        max_item_count = valid_items_count


        page_info = PageInfoType(
            has_next_page=False,
            has_previous_page=False,
            start_cursor=cache_info_list[0].cache_name if cache_info_list else None,
            end_cursor=cache_info_list[-1].cache_name if cache_info_list else None
        )       

        edges = [CacheInfoEdge(node=cache_info) for cache_info in cache_info_list]

        return CacheInfoConnection(
            total_count=total_count,
            max_item_count = max_item_count,
            page_info=page_info,
            edges=edges
        )

class ClearCacheMutation(OpenIMISMutation):
    _mutation_module = "cache_manager"
    _mutation_class = "ClearCacheMutation"

    class Input(OpenIMISMutation.Input):
        model = graphene.String()


    @classmethod
    def async_mutate(cls, user, **data):
        openimis_models = CacheService.openimis_models
        try:
            if type(user) is AnonymousUser or not user.id:
                raise ValidationError(_("mutation.authentication_required"))
            model = data.get("model", None)
            if model:
                model = model.lower()
                if model in caches or model == "location_user":
                    cache = caches['location'] if model == "location_user" else caches[model]

                    match model:
                        case "location_user":
                            clear_cache_graph(cache, model, user)
                        case "coverage":
                            clear_cache_graph(cache, model, user)
                        case _:
                            raise ValidationError(_("model_does_not_define"))

                    return None
                elif model in openimis_models:
                    CacheService.clear_all_model_cache(model)
                    return None
                else:
                    raise ValidationError(_(f"The cache for model '{model}' does not exist."))
            else:
                raise ValidationError(_("model_cannot_be_null"))
        except Exception as exc:
            return [
                {
                    "message": _("cache_manager.mutation.failed_to_clear_cache")
                    % {"model": data["model"]},
                    "detail": str(exc),
                }
            ]

class Mutation(graphene.ObjectType):
    clear_cache = ClearCacheMutation.Field()
