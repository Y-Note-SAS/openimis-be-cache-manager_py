from django.apps import AppConfig

MODULE_NAME = "cache"

DEFAULT_CFG = {
    "gql_manage_cache_perms": ["101011"],
}

class CacheManagerConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'cache_manager'

    gql_manage_cache_perms = []

    def __load_config(self, cfg):
        for field in cfg:
            if hasattr(CacheManagerConfig, field):
                setattr(CacheManagerConfig, field, cfg[field])

    def ready(self):
        from core.models import ModuleConfiguration
        cfg = ModuleConfiguration.get_or_default(MODULE_NAME, DEFAULT_CFG)
        self.__load_config(cfg)
