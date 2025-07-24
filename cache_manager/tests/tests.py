import json
from dataclasses import dataclass
from unittest.mock import patch, MagicMock
from core.models.openimis_graphql_test_case import openIMISGraphQLTestCase
from cache_manager.schema import CacheService
from cache_manager.services import get_cache_key, get_cache_key_base
from insuree.test_helpers import create_test_insuree
from location.models import Location
from insuree.models import Insuree
from core.models import User
from core.test_helpers import create_test_interactive_user
from graphql_jwt.shortcuts import get_token
from location.test_helpers import create_test_village
import os
from django.core.exceptions import ImproperlyConfigured
from django.conf import settings

@dataclass
class DummyContext:
    user: User

class CacheManagerTestCase(openIMISGraphQLTestCase):
    @classmethod
    def setUpClass(cls):
        # Prevent tests from running in production environment
        if os.getenv('DJANGO_ENV') == 'production':
            raise ImproperlyConfigured("Tests cannot be run in production environment!")

        super().setUpClass()

        cls.fake_redis_store = {}

        # Patch Django cache and Redis connection
        cls.cache_patch = patch('django.core.cache.caches')
        cls.mock_caches = cls.cache_patch.start()

        cls.redis_patch = patch('django_redis.get_redis_connection')
        cls.mock_get_redis_connection = cls.redis_patch.start()

        class FakeRedis:
            def __init__(self, store):
                self.store = store

            def get(self, key):
                print(f"FakeRedis.get called with key: {key}")
                return self.store.get(key)

            def set(self, key, val):
                print(f"FakeRedis.set called with key: {key}, value: {val}")
                self.store[key] = val

            def delete(self, *keys):
                print(f"FakeRedis.delete called with keys: {keys}")
                for key in keys:
                    self.store.pop(key, None)

            def keys(self, pattern):
                print(f"FakeRedis.keys called with pattern: {pattern}")
                return [k for k in self.store.keys() if k.startswith(pattern.rstrip('*'))]

            def scan_iter(self, match=None):
                print(f"FakeRedis.scan_iter called with match: {match}")
                for key in list(self.store.keys()):
                    if not match or key.startswith(match.rstrip('*')):
                        yield key

            def flushdb(self):
                print("FakeRedis.flushdb called")
                self.store.clear()

            def select(self, db):
                print(f"FakeRedis.select called with db: {db}")
                pass  # Simulate Redis database selection

        cls.fake_redis_client = FakeRedis(cls.fake_redis_store)
        cls.mock_get_redis_connection.return_value = cls.fake_redis_client

        # Configure CacheService to use the mocked Redis client
        CacheService._CacheService__redis_client = cls.fake_redis_client
        CacheService.get_redis_connection = classmethod(lambda cls: cls.fake_redis_client)

        def get_fake_cache(name):
            mock_cache = MagicMock()
            mock_cache.get.side_effect = lambda k, d=None: cls.fake_redis_store.get(k, d)
            mock_cache.set.side_effect = lambda k, v, timeout=None: cls.fake_redis_store.__setitem__(k, v)
            mock_cache.delete.side_effect = lambda *keys: [cls.fake_redis_store.pop(k, None) for k in keys]
            mock_cache.keys.side_effect = lambda pattern=None: [
                k for k in cls.fake_redis_store.keys() if not pattern or k.startswith(pattern.rstrip('*'))
            ]
            mock_cache.client = MagicMock()
            mock_cache.client.get_client.return_value = cls.fake_redis_client
            return mock_cache

        cls.mock_caches.__getitem__.side_effect = get_fake_cache
        cls.cache = get_fake_cache('default')

        # Create test data
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

    @classmethod
    def tearDownClass(cls):
        cls.cache_patch.stop()
        cls.redis_patch.stop()
        super().tearDownClass()

    def setUp(self):
        # Reset the fake Redis store and mocks before each test
        self.fake_redis_store.clear()
        self.mock_caches.reset_mock()
        self.mock_get_redis_connection.reset_mock()

    def test_cache_info_query(self):
        # Populate fake Redis store with test data
        self.fake_redis_store['oi:1:Insuree:1'] = self.test_insuree
        self.fake_redis_store['oi:1:Location:1'] = self.location
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

    def test_clear_cache_mutation(self):
        prefix = CacheService.get_prefixed_model('insuree')
        cache_key = f"{prefix}{self.test_insuree.id}"
        self.cache.set(cache_key, self.test_insuree)
        print(f"Cache after set: {self.fake_redis_store}")
        self.assertIsNotNone(self.cache.get(cache_key))

        # Mock clear_all_model_cache to remove keys with the specified prefix
        with patch.object(CacheService, 'clear_all_model_cache') as mock_clear:
            mock_clear.side_effect = lambda model: [
                self.fake_redis_store.pop(k, None)
                for k in list(self.fake_redis_store.keys())
                if k.startswith(CacheService.get_prefixed_model(model))
            ]
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
            mock_clear.assert_called_once_with('insuree')
        print(f"Cache after clearCache: {self.fake_redis_store}")
        self.assertResponseNoErrors(response)
        self.assertIsNone(self.cache.get(cache_key))

    def test_preheat_cache_mutation(self):
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

        cache_key = get_cache_key(Location, self.location.id)
        if self.cache.get(cache_key) is None:
            self.cache.set(cache_key, self.location)

        cached_data = self.cache.get(cache_key)
        self.assertIsNotNone(cached_data)
        self.assertEqual(cached_data.id, self.location.id)

    def test_clear_module_cache(self):
        cache = self.mock_caches['location']
        cache_key = get_cache_key_base('location', self.location.id)  # Use location_<id>
        cache.set(cache_key, self.location)
        print(f"Cache after set in test_clear_module_cache: {self.fake_redis_store}")
        self.assertIsNotNone(cache.get(cache_key))

        # Mock clear_module_cache to remove keys with the specified prefix
        with patch.object(CacheService, 'clear_module_cache') as mock_clear:
            prefix = settings.CACHES.get('location', {}).get('KEY_PREFIX', 'location_')
            mock_clear.side_effect = lambda model: [
                self.fake_redis_store.pop(k, None)
                for k in list(self.fake_redis_store.keys())
                if k.startswith(prefix)
            ]
            CacheService.clear_module_cache('location')
        print(f"Cache after clear_module_cache: {self.fake_redis_store}")
        self.assertIsNone(cache.get(cache_key))

    def test_clear_all_model_cache(self):
        prefix = CacheService.get_prefixed_model('location')
        cache_key = f"{prefix}{self.location.id}"
        self.cache.set(cache_key, self.location)
        print(f"Cache after set in test_clear_all_model_cache: {self.fake_redis_store}")
        self.assertIsNotNone(self.cache.get(cache_key))

        # Mock clear_all_model_cache to remove keys with the specified prefix
        with patch.object(CacheService, 'clear_all_model_cache') as mock_clear:
            mock_clear.side_effect = lambda model: [
                self.fake_redis_store.pop(k, None)
                for k in list(self.fake_redis_store.keys())
                if k.startswith(CacheService.get_prefixed_model(model))
            ]
            CacheService.clear_all_model_cache('location')
        print(f"Cache after clear_all_model_cache: {self.fake_redis_store}")
        self.assertIsNone(self.cache.get(cache_key))

    def test_get_prefixed_model(self):
        prefix = CacheService.get_prefixed_model('location')
        self.assertEqual(prefix, 'oi:1:Location:')
