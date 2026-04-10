# Deploy na Heroku

## Arquivos de deploy

- `Procfile`: sobe o app com `gunicorn` e roda migracoes no release.
- `.python-version`: fixa o major do Python usado pela Heroku.
- `requirements.txt`: dependencias minimas para build Linux.

## Config vars minimas

Defina estas variaveis no app:

- `SECRET_KEY`
- `DEBUG=False`
- `HEROKU_APP_NAME=<nome-do-app>`

Para gerar uma chave forte localmente:

- `python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"`

Se usar dominio customizado, adicione tambem:

- `ALLOWED_HOSTS=seu-dominio.com,www.seu-dominio.com`
- `CSRF_TRUSTED_ORIGINS=https://seu-dominio.com,https://www.seu-dominio.com`

Este projeto ja inclui `eco.dscorp.top` por padrao em `ALLOWED_HOSTS` e `CSRF_TRUSTED_ORIGINS`.
Se precisar adicionar outros dominios, prefira:

- `PROJECT_DOMAINS=eco.dscorp.top,outro-dominio.com`

`ALLOWED_HOSTS` e `CSRF_TRUSTED_ORIGINS` continuam disponiveis para sobrescritas explicitas.

## Passo a passo

1. Crie o app:
   - `heroku create <nome-do-app>`
2. Provisione o banco Postgres:
   - `heroku addons:create heroku-postgresql`
3. Configure as variaveis:
   - `heroku config:set SECRET_KEY="<uma-chave-forte>" DEBUG=False HEROKU_APP_NAME=<nome-do-app>`
4. Faca o deploy:
   - `git push heroku main`
5. Abra o app:
   - `heroku open`

## Comandos uteis

- Ver logs:
  - `heroku logs --tail`
- Rodar shell:
  - `heroku run python manage.py shell`
- Criar superusuario:
  - `heroku run python manage.py createsuperuser`
