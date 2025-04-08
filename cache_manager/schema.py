import logging
from django_redis.cache import RedisCache
import graphene
from django.core.cache import caches
from django.conf import settings
from django.contrib.auth.models import AnonymousUser
from location.models import free_cache_for_user, Location
logger = logging.getLogger(__name__)
from django.core.exceptions import ValidationError, PermissionDenied
from django.utils.translation import gettext as _
from core.schema import OpenIMISMutation
# from policy.models import clean_all_enquire_cache_product
from django.db.models import Q
from core import filter_validity
from cache_manager.services import CacheService
from django.utils.translation import gettext as _

class CacheInfoType(graphene.ObjectType):
    cache_name = graphene.String()
    model = graphene.String(required=False)
    total_count = graphene.Int()
    max_item_count = graphene.Int()

    class Meta:
        fields = ("cache_name", "model", "total_count", "max_item_count")

class PageInfoType(graphene.ObjectType):
    has_next_page = graphene.Boolean()
    has_previous_page = graphene.Boolean()
    start_cursor = graphene.String()
    end_cursor = graphene.String()

class CacheInfoEdge(graphene.ObjectType):
    node = graphene.Field(CacheInfoType)

class CacheInfoConnection(graphene.ObjectType):
    total_count = graphene.Int()
    page_info = graphene.Field(PageInfoType)
    edges = graphene.List(CacheInfoEdge)
    


class Query(graphene.ObjectType):
    cache_info = graphene.Field(
        CacheInfoConnection,
        model=graphene.String(required=False),
        order_by=graphene.List(graphene.String),
        first=graphene.Int(), 
        after=graphene.String(),
        before=graphene.String(),
    )

    def resolve_cache_info(self, info, model=None, order_by=None, first=10, after=None, before=None):
        openimis_models = CacheService.openimis_models
        MODEL_PREFIXES = CacheService.MODEL_PREFIXES
        
        cache_info_list = []
        for model in openimis_models:
            model = model.lower()
            if model == "location_user":
                cache = caches['location']
                cache_config = settings.CACHES.get('location')
            elif model in settings.CACHES and model != 'location':
                cache = caches[model]
                cache_config = settings.CACHES.get(model, settings.CACHES['default'])
            else:
                cache = caches['default']
                cache_config = settings.CACHES['default']

            redis_client = cache.client.get_client()
            redis_client.select(0)
            prefix = cache_config.get('KEY_PREFIX', '')
            if cache_config == settings.CACHES['default']:
                prefix = MODEL_PREFIXES.get(model, '')
            key_count = sum(1 for _ in redis_client.scan_iter(match=f'{prefix}*'))

            max_item_count = CacheService.items_count(model)
            total_count = key_count
            cache_info_list.append(CacheInfoType(
                cache_name=model,
                model=model,
                max_item_count=max_item_count,
                total_count=total_count
            ))

        total_count = len(cache_info_list)
            
        if after:
            start_index = next((i for i, cache_info in enumerate(cache_info_list) if cache_info.cache_name == after), -1)
            if start_index != -1:
                start_index += 1 
            else:
                start_index = 0
        elif before:
            start_index = next((i for i, cache_info in enumerate(cache_info_list) if cache_info.cache_name == before), len(cache_info_list))
            start_index = max(0, start_index - first)
        else:
            start_index = 0
        
        # Get the page slice based on 'first'
        cache_info_list_page = cache_info_list[start_index:start_index + first]

        # Determine if there are more pages
        has_next_page = start_index + first < total_count
        has_previous_page = start_index > 0

        # Get the start and end cursors
        start_cursor = cache_info_list_page[0].cache_name if cache_info_list_page else None
        end_cursor = cache_info_list_page[-1].cache_name if cache_info_list_page else None
        
        page_info = PageInfoType(
            has_next_page=has_next_page,
            has_previous_page=has_previous_page,
            start_cursor=start_cursor,
            end_cursor=end_cursor
        )
        
        edges = [CacheInfoEdge(node=cache_info) for cache_info in cache_info_list_page]
        return CacheInfoConnection(
            total_count=total_count,
            page_info=page_info,
            edges=edges
        )
        
        
class ClearCacheMutation(OpenIMISMutation):
    _mutation_module = "cache_manager"
    _mutation_class = "ClearCacheMutation"

    class Input(OpenIMISMutation.Input):
        models = graphene.List(graphene.String)    

    @classmethod
    def async_mutate(cls, user, **data):
        openimis_models = CacheService.openimis_models
        try:
            if type(user) is AnonymousUser or not user.id:
                raise ValidationError(_("mutation.authentication_required"))
            models = data.get("models", None)

            if not models:
                raise ValidationError(_("models_cannot_be_null"))

            for model in models:
                model = model.lower()
                if model in caches or model == "location_user":
                    # cache = caches['location'] if model == "location_user" else caches[model]
                    match model:
                        case "location_user":
                            # free_cache_for_user(user.id)
                            CacheService.clear_module_cache("location")
                        case "coverage":
                            # clean_all_enquire_cache_product()
                            CacheService.clear_module_cache(model)
                        case _:
                            raise ValidationError(_("model_does_not_define"))
                elif model in openimis_models:
                    CacheService.clear_all_model_cache(model)
                else:
                    raise ValidationError(_(f"The cache for model '{model}' does not exist."))
            return None 
        except Exception as exc:
            return [
                {
                    "message": _("cache_manager.mutation.failed_to_clear_cache")
                    % {"models": str(data["models"])},
                    "detail": str(exc),
                }
            ]

class PreheatCacheMutation(OpenIMISMutation):
    _mutation_module = "cache_manager"
    _mutation_class = "PreheatCacheMutation"

    class Input(OpenIMISMutation.Input):
        model = graphene.String(required=True)

    @classmethod
    def async_mutate(cls, user, **data):
        try:
            if user.is_anonymous or not user.id:
                raise ValidationError(_("authentication_required"))
            
            model = data.get("model")
            if not model:
                raise ValidationError(_("Model_cannot_be_null"))

            result = CacheService.preload_model_cache(model, user)
            
            if result:
                return None 
            else:
                raise ValidationError(_("Failed_to_preheat_the_cache."))
        except Exception as exc:
            return [
                {
                    "message": _("Failed_to_preheat_cache_for_model:") + str(data["model"]),
                    "detail": str(exc),
                }
            ]

class Mutation(graphene.ObjectType):
    clear_cache = ClearCacheMutation.Field()
    preheat_cache = PreheatCacheMutation.Field()
