from django.apps import AppConfig


class DashboardConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'dashboard'

    def ready(self):
        # Importar dash_apps para registrar os apps Dash ao iniciar o Django
        import dashboard.dash_apps  # noqa: F401
