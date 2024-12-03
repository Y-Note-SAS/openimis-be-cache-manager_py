import logging
from core import models as core_models
from django_redis.cache import RedisCache
import graphene
from django.core.cache import caches
from django.conf import settings
from django.contrib.auth.models import AnonymousUser
from location.models import Location, free_cache_for_user
from insuree.models import InsureePolicy
logger = logging.getLogger(__name__)
from django.core.exceptions import ValidationError, PermissionDenied
from django.utils.translation import gettext as _
from core.schema import OpenIMISMutation
from policy.models import clean_all_enquire_cache_product
from django.db.models import Q
from core import filter_validity

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
        case "location":
            # free_cache_for_user(user.id)
            cache_items = Location.objects.filter(
                Q(parent__isnull=False) | Q(type='R'),
                *filter_validity(),
            )
        case "coverage":
            clean_all_enquire_cache_product()
        case _:
            cache.clear()
    
    for cache_item in cache_items:
        cache.delete(f"{model}_{cache_item.id}")

    cache.delete(f"{model}_graph")
    cache.delete(f"{model}_types")


class Query(graphene.ObjectType):
    cache_info = graphene.Field(
        CacheInfoConnection,
        model=graphene.String(required=False),
        first=graphene.Int(),
        order_by=graphene.List(graphene.String)
    )

    def resolve_cache_info(self, info, model, first=1000):
        if model:
            model = model.lower()
            cache = caches[model]
            if model not in settings.CACHES:
                logger.debug(f"Cache model '{model}' not found.")
                return CacheInfoConnection(
                    total_count=0,
                    max_item_count = 0,
                    page_info=page_info,
                    edges=edges
                )
        else:
            cache = caches['default']
        
        redis_client = cache.client.get_client()
        redis_client.select(0)

        cache_config = settings.CACHES.get(model, settings.CACHES['default'])
        prefix = cache_config.get('KEY_PREFIX', '') 
        keys = redis_client.scan_iter(match=f'{prefix}*')
        cache_info_list = []

        for key in keys:
            key_str = key.decode('utf-8')
            parts = key_str.split(':')
            actual_key = parts[-1]
            data = cache.get(actual_key)
            
            cache_info_list.append(CacheInfoType(
                cache_name=data,
                model=model
            ))
            if len(cache_info_list) >= first:
                break

        match model:
            case "location":
                valid_items_count = Location.objects.filter(validity_to__isnull=True).count()
            case "coverage":
                valid_items_count = InsureePolicy.objects.filter(validity_to__isnull=True).count()
            case _:
                valid_items_count = 0  

        valid_items_count = Location.objects.filter(validity_to__isnull=True).count()
        total_count = len(cache_info_list)
        max_item_count = valid_items_count

        
        page_info = PageInfoType(
            has_next_page=len(cache_info_list) > first,
            has_previous_page=False,
            start_cursor=cache_info_list[0].cache_name if cache_info_list else None,
            end_cursor=cache_info_list[-1].cache_name if cache_info_list else None
        )

        edges = [CacheInfoEdge(node=cache_info) for cache_info in cache_info_list]

        return CacheInfoConnection(
            total_count=total_count,
            max_item_count = max_item_count + 3,
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
        try:
            if type(user) is AnonymousUser or not user.id:
                raise ValidationError(_("mutation.authentication_required"))
            model = data.get("model", None)
            if model:
                model = model.lower()
                if model in caches:
                    cache = caches[model]

                    match model:
                        case "location":
                            clear_cache_graph(cache, model, user)
                        case "coverage":
                            clear_cache_graph(cache, model, user)
                        case _:
                            cache.clear()

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
