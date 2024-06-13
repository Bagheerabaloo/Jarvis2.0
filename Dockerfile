# Utilizza un'immagine Docker ufficiale di Python 3.11 come immagine di base
FROM python:3.11

# Imposta la directory di lavoro nel container
WORKDIR /app

# Copia i file requirements.txt nella directory di lavoro del container
COPY requirements.txt ./

# Installa i pacchetti richiesti
RUN pip install --no-cache-dir -r requirements.txt

# Copia il resto del codice sorgente del tuo progetto nella directory di lavoro del container
COPY . /app

# Esegui il tuo programma Python quando il container viene avviato
CMD ["python", "main.py"]