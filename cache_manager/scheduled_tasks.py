from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
from .services import CacheService
from core.models import InteractiveUser
_logger = logging.getLogger(__name__)

def cache_prewarming_job():
    """
    Cette fonction charge les objet en cache chaque 2h:00
    """
    _logger.warning("Prewarm cache job started...")
    admin_user = InteractiveUser.objects.filter(id=1).first()
    if not admin_user:
        _logger.warning("Utilisateur admin non trouvé")
        return True
    models = [
        "insuree", "family",
        "policy", "insuree_policy",
        "service", "item",
        "claim", "claim_service", "claim_item"
    ]
    for model in models:
        result = CacheService.preload_model_cache(model, admin_user)
        _logger.warning("result for model %s: %s", model, result)
    _logger.warning("Prewarm cache job finished...")


def schedule_tasks(scheduler: BackgroundScheduler):
    """
    This is the function to attach job to the system
    """
    scheduler.add_job(
        cache_prewarming_job,
        trigger=CronTrigger(day='*', hour=2, minute=0),
        id="cache_prewarming_job",
        max_instances=1,
        replace_existing=True,
        # misfire_grace_time=0 # Don't allow late executions
    )
