# Deploy na Heroku

## Arquivos de deploy

- `Procfile`: sobe o app com `gunicorn` e roda migrações no release.
- `.python-version`: fixa o major do Python usado pela Heroku.
- `requirements.txt`: dependências mínimas para build Linux.

## Config vars mínimas

Defina estas variáveis no app:

- `SECRET_KEY`
- `DEBUG=False`
- `HEROKU_APP_NAME=<nome-do-app>`

Para gerar uma chave forte localmente:

- `python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"`

Se usar domínio customizado, adicione também:

- `ALLOWED_HOSTS=seu-dominio.com,www.seu-dominio.com`
- `CSRF_TRUSTED_ORIGINS=https://seu-dominio.com,https://www.seu-dominio.com`

## Passo a passo

1. Crie o app:
   - `heroku create <nome-do-app>`
2. Provisione o banco Postgres:
   - `heroku addons:create heroku-postgresql`
3. Configure as variáveis:
   - `heroku config:set SECRET_KEY="<uma-chave-forte>" DEBUG=False HEROKU_APP_NAME=<nome-do-app>`
4. Faça o deploy:
   - `git push heroku main`
5. Abra o app:
   - `heroku open`

## Comandos úteis

- Ver logs:
  - `heroku logs --tail`
- Rodar shell:
  - `heroku run python manage.py shell`
- Criar superusuário:
  - `heroku run python manage.py createsuperuser`
