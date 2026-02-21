FROM python:3.8-slim

# Mettez à jour les paquets et installez les dépendances nécessaires pour pipenv
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && apt-get clean

# Copiez les fichiers Pipfile et Pipfile.lock dans le conteneur
COPY Pipfile Pipfile.lock /app/
# Elle spécifie le répertoire racine dans lequel Python cherchera des modules lors de l'exécution de scripts Python. 
ENV PYTHONPATH="/app"
# Cette instruction définit le répertoire de travail (working directory) dans lequel les commandes ultérieures seront exécutées. 
# Cela signifie que lorsque vous exécutez des commandes telles que COPY, RUN, ou CMD, elles s'appliqueront à partir du répertoire "/app" par défaut
WORKDIR /app

# Installez pipenv & packages
RUN pip install pipenv
RUN pipenv requirements > requirements.txt
RUN pipenv requirements --dev > requirements-dev.txt
RUN pip install -r requirements.txt

# Copiez les scripts dans le conteneur
COPY *.py /app/

# Commande par défaut à exécuter lors du démarrage du conteneur
ENTRYPOINT [ "python", "run.py" ]
