import logging
from django_redis.cache import RedisCache
import graphene
from django.core.cache import caches
from django.conf import settings
from location.models import Location
from insuree.models import InsureePolicy
logger = logging.getLogger(__name__)

class CacheInfoType(graphene.ObjectType):
    cache_name = graphene.String()
    module = graphene.String()

    class Meta:
        fields = ("cache_name", "module")

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

class Query(graphene.ObjectType):
    cache_info = graphene.Field(
        CacheInfoConnection,
        module=graphene.String(required=False),
        first=graphene.Int(),
        order_by=graphene.List(graphene.String)
    )

    def resolve_cache_info(self, info, module, first=1000):
        if module:
            module = module.lower()
            cache = caches[module]
            if module not in settings.CACHES:
                logger.debug(f"Cache module '{module}' not found.")
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

        cache_config = settings.CACHES.get(module, settings.CACHES['default'])
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
                module=module
            ))
            if len(cache_info_list) >= first:
                break

        match module:
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
            max_item_count = max_item_count + 2,
            page_info=page_info,
            edges=edges
        )


