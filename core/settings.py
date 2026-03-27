"""
Django settings for core project.
"""

import os
import secrets
import sys
from pathlib import Path
from urllib.parse import urlparse

import dj_database_url
from django.core.exceptions import ImproperlyConfigured

BASE_DIR = Path(__file__).resolve().parent.parent


def env_bool(name, default=False):
    return os.environ.get(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def env_list(name, default=""):
    return [item.strip() for item in os.environ.get(name, default).split(",") if item.strip()]


def normalize_host(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "://" in raw:
        parsed = urlparse(raw)
        raw = parsed.netloc or parsed.path
    return raw.split("/", 1)[0].strip()


def build_https_origin(host):
    normalized = normalize_host(host)
    if not normalized:
        return ""
    if normalized.startswith("localhost") or normalized.startswith("127.0.0.1"):
        return f"http://{normalized}"
    return f"https://{normalized}"


def normalize_origin(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "://" in raw:
        parsed = urlparse(raw)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
    return build_https_origin(raw)

ON_HEROKU = bool(os.environ.get("DYNO"))
CURRENT_COMMAND = sys.argv[1] if len(sys.argv) > 1 else ""
LOCAL_DEV_COMMANDS = {"runserver", "test", "shell", "check", "makemigrations", "migrate"}
ALLOW_LOCAL_DEV_DEFAULTS = not ON_HEROKU and Path(sys.argv[0]).name == "manage.py" and CURRENT_COMMAND in LOCAL_DEV_COMMANDS
DEBUG = env_bool("DEBUG", ALLOW_LOCAL_DEV_DEFAULTS and CURRENT_COMMAND == "runserver")
SECRET_KEY = os.environ.get("SECRET_KEY", "").strip()

if not SECRET_KEY:
    if ALLOW_LOCAL_DEV_DEFAULTS:
        SECRET_KEY = secrets.token_urlsafe(50)
    else:
        raise ImproperlyConfigured("SECRET_KEY environment variable must be set when DEBUG is False.")

ALLOWED_HOSTS = [host for host in (normalize_host(item) for item in env_list("ALLOWED_HOSTS", "localhost,127.0.0.1")) if host]
CSRF_TRUSTED_ORIGINS = [origin for origin in (normalize_origin(item) for item in env_list("CSRF_TRUSTED_ORIGINS")) if origin]
EXPLICIT_ALLOWED_HOSTS = bool(os.environ.get("ALLOWED_HOSTS", "").strip())


def add_host_and_origin(host):
    normalized_host = normalize_host(host)
    if normalized_host and normalized_host not in ALLOWED_HOSTS:
        ALLOWED_HOSTS.append(normalized_host)

    origin = build_https_origin(normalized_host)
    if origin and origin not in CSRF_TRUSTED_ORIGINS:
        CSRF_TRUSTED_ORIGINS.append(origin)

APP_NAME = os.environ.get("APP_NAME", os.environ.get("HEROKU_APP_NAME", "")).strip()
if APP_NAME:
    normalized_app = normalize_host(APP_NAME)
    if "." in normalized_app:
        add_host_and_origin(normalized_app)
    else:
        add_host_and_origin(f"{normalized_app}.herokuapp.com")

for platform_host in [
    os.environ.get("RENDER_EXTERNAL_HOSTNAME", ""),
    os.environ.get("RAILWAY_PUBLIC_DOMAIN", ""),
    os.environ.get("VERCEL_URL", ""),
    os.environ.get("WEBSITE_HOSTNAME", ""),
]:
    add_host_and_origin(platform_host)

if ON_HEROKU and ".herokuapp.com" not in ALLOWED_HOSTS:
    ALLOWED_HOSTS.append(".herokuapp.com")

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    # django-plotly-dash
    'django_plotly_dash.apps.DjangoPlotlyDashConfig',
    'dpd_static_support',
    'channels',
    # App local
    'dashboard',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django_plotly_dash.middleware.BaseMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'core.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'core.wsgi.application'
ASGI_APPLICATION = 'core.asgi.application'

CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels.layers.InMemoryChannelLayer',
    },
}

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    DATABASES = {
        'default': dj_database_url.parse(
            DATABASE_URL,
            conn_max_age=600,
            ssl_require=not DEBUG,
        )
    }
else:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': BASE_DIR / 'db.sqlite3',
        }
    }

AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

LANGUAGE_CODE = 'pt-br'
TIME_ZONE = 'America/Sao_Paulo'
USE_I18N = True
USE_TZ = True

STATIC_URL = '/static/'
STATICFILES_DIRS = [BASE_DIR / 'static']
STATIC_ROOT = BASE_DIR / 'staticfiles'
STORAGES = {
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedStaticFilesStorage",
    },
}

STATICFILES_FINDERS = [
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    'django_plotly_dash.finders.DashAssetFinder',
    'core.staticfinders.NormalizedDashComponentFinder',
    'django_plotly_dash.finders.DashAppDirectoryFinder',
]

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# Necessário para os iframes do Dash funcionarem no mesmo domínio
X_FRAME_OPTIONS = 'SAMEORIGIN'
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
USE_X_FORWARDED_HOST = True
SECURE_SSL_REDIRECT = not DEBUG
SECURE_HSTS_SECONDS = 31536000 if not DEBUG else 0
SECURE_HSTS_INCLUDE_SUBDOMAINS = not DEBUG
SECURE_HSTS_PRELOAD = not DEBUG
SESSION_COOKIE_SECURE = not DEBUG
CSRF_COOKIE_SECURE = not DEBUG
