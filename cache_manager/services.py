# services.py
from django.core.cache import caches

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

            print(f"voici bien le modele: {model}")
            for key in keys:
                key_str = key.decode('utf-8')
                redis_client.delete(key_str)
            return None

    
    openimis_models = {'location', 'claim_admin', 'feedback', 'claim', 'feedback_prompt', 'claim_item', 'claim_attachment_type', 'claim_attachment', 'claim_service', 
                   'claim_ded_rem', 'premium', 'history_business_model', 'role', 'role_right', 'interactive_user', 'user_role', 'user', 'officer', 
                   'cheque_import', 'cheque_import_line', 'cheque_updated_history', 'individual', 'individual_data_source_upload', 'individual_data_source', 'group', 
                   'group_individual', 'insuree_photo', 'family', 'insuree_status_reason', 'insuree', 'insuree_policy', 'policy_renewal_detail', 'health_facility', 'user_district', 
                   'officer_village', 'diagnosis', 'item', 'service', 'policy', 'policy_renewal', 'product', 'product_item', 'product_service', 'extract'}

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

    

