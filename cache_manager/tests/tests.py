import json
from dataclasses import dataclass
from core.models.openimis_graphql_test_case import openIMISGraphQLTestCase
from django.core.cache import caches
from cache_manager.schema import CacheService
from cache_manager.services import get_cache_key, get_cache_key_base
from django_redis import get_redis_connection

from insuree.test_helpers import create_test_insuree
from location.models import Location
from insuree.models import Insuree
from core.models import User
from core.test_helpers import create_test_interactive_user
from graphql_jwt.shortcuts import get_token
from location.test_helpers import create_test_village

@dataclass
class DummyContext:
    """Context object required for token generation."""
    user: User

class CacheManagerTestCase(openIMISGraphQLTestCase):
    admin_user = None
    test_village = None
    test_insuree = None
    test_photo = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        
        caches.settings["default"] = {
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
            'LOCATION': 'default-test'
        }
        caches.settings["location"] = {
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
            'LOCATION': 'location-test'
        }
        
        cls.test_village = create_test_village()
        cls.location = cls.test_village 
        cls.test_insuree = create_test_insuree(
            with_family=True,
            is_head=True,
            custom_props={'current_village': cls.test_village},
            family_custom_props={'location': cls.test_village}
        )
        cls.admin_user = create_test_interactive_user(username="testLocationAdmin")
        cls.admin_token = get_token(cls.admin_user, DummyContext(user=cls.admin_user))

        # Use default cache and flush Redis DB before each test
        cls.cache = caches['default'] 

    def test_cache_info_query(self):
        query = """
        query {
            cacheInfo(first: 2) {
                totalCount
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                    startCursor
                    endCursor
                }
                edges {
                    node {
                        cacheName
                        model
                        totalCount
                        maxItemCount
                    }
                }
            }
        }
        """
        response = self.query(
            query,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.admin_token}"},
        )
        content = json.loads(response.content)

        self.assertResponseNoErrors(response)
        self.assertIn("cacheInfo", content["data"])
        self.assertGreaterEqual(content["data"]["cacheInfo"]["totalCount"], 0)
        self.assertLessEqual(len(content["data"]["cacheInfo"]["edges"]), 2)
        self.assertFalse(content["data"]["cacheInfo"]["pageInfo"]["hasPreviousPage"])

    def test_clear_cache_mutation(self):
        # Set and validate a key in default cache
        cache = caches['default']
        cache_key = get_cache_key(Insuree, self.test_insuree.id)
        cache.set(cache_key, self.test_insuree, timeout=None)
        self.assertIsNotNone(cache.get(cache_key))

        # Trigger GraphQL mutation to clear insuree cache
        mutation = """
        mutation {
            clearCache(input: { models: ["insuree"] }) {
                clientMutationId
            }
        }
        """
        response = self.query(
            mutation,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.admin_token}"},
        )

        self.assertResponseNoErrors(response)
        self.assertIsNone(cache.get(cache_key))

    def test_preheat_cache_mutation(self):
        # Trigger GraphQL mutation to preheat location model cache
        mutation = """
        mutation {
            preheatCache(input: { model: "location" }) {
                clientMutationId
            }
        }
        """
        response = self.query(
            mutation,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.admin_token}"},
        )

        self.assertResponseNoErrors(response)

        # Verify location cache was populated
        cache_key = get_cache_key(Location, self.location.id)
        cached_data = self.cache.get(cache_key)
        self.assertIsNotNone(cached_data)
        self.assertEqual(cached_data["id"], self.location.id)

    def test_clear_module_cache(self):
        # Manually add key to location module cache
        cache = caches['location']
        cache_key = get_cache_key_base('location', self.location.id)
        cache.set(cache_key, self.location, timeout=None)

        # Ensure key is present before clearing
        self.assertIsNotNone(cache.get(cache_key))

        # Clear location module cache
        CacheService.clear_module_cache('location')

        # Ensure key was removed
        self.assertIsNone(cache.get(cache_key))

    def test_clear_all_model_cache(self):
        # Manually add key to default cache for location model
        cache_key = get_cache_key(Location, self.location.id)
        self.cache.set(cache_key, self.location, timeout=None)

        # Confirm key exists
        self.assertIsNotNone(self.cache.get(cache_key))

        # Clear all location model cache entries
        CacheService.clear_all_model_cache('location')

        # Confirm key is deleted
        self.assertIsNone(self.cache.get(cache_key))

    def test_get_prefixed_model(self):
        # Verify cache prefix format for location model
        prefix = CacheService.get_prefixed_model('location')
        self.assertEqual(prefix, 'oi:1:Location:')
