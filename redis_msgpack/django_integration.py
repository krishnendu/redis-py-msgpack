"""
Integration with Django's cache framework for redis-msgpack.
"""
import importlib
from typing import Any, Optional, Type

try:
    from django.core.cache.backends.redis import RedisCache
    from django.conf import settings
    DJANGO_AVAILABLE = True
except ImportError:
    DJANGO_AVAILABLE = False

if DJANGO_AVAILABLE:
    class RedisMsgpackCache(RedisCache):
        """
        Django cache backend using MessagePack serialization.
        
        Example settings:
        
        CACHES = {
            "default": {
                "BACKEND": "redis_msgpack.django_integration.RedisMsgpackCache",
                "LOCATION": "redis://127.0.0.1:6379/1",
                "OPTIONS": {
                    "CLIENT_CLASS": "redis_msgpack.client.RedisMsgpackClient",
                    "SERIALIZER_CLASS": "redis_msgpack.utils.MsgpackSerializer",
                }
            }
        }
        """
        
        def __init__(self, server, params):
            options = params.get('OPTIONS', {})
            
            # Use RedisMsgpackClient by default
            if 'CLIENT_CLASS' not in options:
                options['CLIENT_CLASS'] = 'redis_msgpack.client.RedisMsgpackClient'
            
            # Import the serializer class if specified
            self.serializer_class = self._get_class(
                options.pop('SERIALIZER_CLASS', 'redis_msgpack.utils.MsgpackSerializer')
            )
            self.serializer_options = options.pop('SERIALIZER_OPTIONS', {})
            
            # Initialize the Redis cache with our client
            super().__init__(server, params)
        
        def get_client(self, **kwargs):
            """
            Get the Redis client, overridden to inject our serializer.
            """
            client = super().get_client(**kwargs)
            
            # If client supports our serializer (should be a RedisMsgpackClient instance)
            if hasattr(client, 'set_serializer'):
                serializer = self.serializer_class(**self.serializer_options)
                client.set_serializer(serializer)
            
            return client
        
        def _get_class(self, import_path: str) -> Type:
            """
            Import a class from a string path.
            
            Args:
                import_path: The import path as a string (e.g. "module.submodule.ClassName")
                
            Returns:
                The imported class
                
            Raises:
                ImportError: If the module or class cannot be imported
            """
            module_path, class_name = import_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            return getattr(module, class_name)


def get_cache_key_prefix() -> str:
    """
    Get the cache key prefix from Django settings if available.
    
    Returns:
        The cache key prefix or an empty string if not set or Django is not available
    """
    if not DJANGO_AVAILABLE:
        return ""
    
    try:
        # Get the prefix from settings if defined
        cache_options = getattr(settings, 'CACHES', {}).get('default', {})
        return cache_options.get('KEY_PREFIX', '')
    except (AttributeError, ImportError):
        return ""


def is_django_installed() -> bool:
    """
    Check if Django is installed and available.
    
    Returns:
        True if Django is available, False otherwise
    """
    return DJANGO_AVAILABLE


class DjangoModelSerializer:
    """
    Utility class to help serialize Django models using MessagePack.
    
    This class provides methods to convert Django model instances
    to dictionaries that can be serialized with MessagePack, and
    to restore models from their serialized form.
    
    Note: This doesn't handle many-to-many relationships or other complex
    Django model features. It's meant for simple model serialization needs.
    """
    
    @staticmethod
    def model_to_dict(instance: Any) -> dict:
        """
        Convert a Django model instance to a dictionary.
        
        Args:
            instance: A Django model instance
            
        Returns:
            A dictionary representation of the model
        """
        if not DJANGO_AVAILABLE:
            raise ImportError("Django is required to use DjangoModelSerializer")
        
        try:
            from django.forms.models import model_to_dict as django_model_to_dict
            return {
                '__django_model__': {
                    'model': f"{instance._meta.app_label}.{instance._meta.model_name}",
                    'pk': instance.pk,
                    'data': django_model_to_dict(instance)
                }
            }
        except ImportError:
            raise ImportError("Django forms module is required for model serialization")
    
    @staticmethod
    def dict_to_model(data: dict) -> Optional[Any]:
        """
        Convert a dictionary back to a Django model instance.
        
        Args:
            data: A dictionary previously created by model_to_dict
            
        Returns:
            A Django model instance or None if conversion failed
        """
        if not DJANGO_AVAILABLE:
            raise ImportError("Django is required to use DjangoModelSerializer")
        
        if not isinstance(data, dict) or '__django_model__' not in data:
            return None
        
        model_info = data['__django_model__']
        app_label, model_name = model_info['model'].split('.')
        
        try:
            from django.apps import apps
            model_class = apps.get_model(app_label, model_name)
            
            # Get existing instance or create new one
            try:
                instance = model_class.objects.get(pk=model_info['pk'])
                
                # Update fields
                for field, value in model_info['data'].items():
                    if field != 'id':  # Don't update primary key
                        setattr(instance, field, value)
                
                # Save changes
                instance.save()
            except model_class.DoesNotExist:
                # Create new instance
                instance = model_class(**model_info['data'])
                instance.save()
                
            return instance
        except Exception:
            return None