import logging
from django_redis.cache import RedisCache
import graphene
from django.core.cache import caches
from django.conf import settings
from django.contrib.auth.models import AnonymousUser
from location.models import free_cache_for_user
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
        order_by=graphene.List(graphene.String)
    )

    def resolve_cache_info(self, info):
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
        page_info = PageInfoType(
            has_next_page=False,
            has_previous_page=False,
            start_cursor=cache_info_list[0].cache_name if cache_info_list else None,
            end_cursor=cache_info_list[-1].cache_name if cache_info_list else None
        )
        edges = [CacheInfoEdge(node=cache_info) for cache_info in cache_info_list]
        return CacheInfoConnection(
            total_count=total_count,
            max_item_count=max_item_count,
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
                    cache = caches['location'] if model == "location_user" else caches[model]
                    match model:
                        case "location_user":
                            clear_cache_graph(cache, model, user)
                        case "coverage":
                            clear_cache_graph(cache, model, user)
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

class Mutation(graphene.ObjectType):
    clear_cache = ClearCacheMutation.Field()
